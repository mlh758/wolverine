/*
AgentCommandDispatcher's claim bookkeeping. See the README for scope and what is abstracted.

The question: can an agent's claim be held, stolen, or released by a dispatch that does not
own it? Every defect found in GH-4537's review rounds is an instance of that one question,
so the model carries the two candidate designs behind `atomic` and lets the checker say
which of them needs the five separate guards the lock-free one has accumulated.

  atomic = true   one lock around claim publication and the abandonment scan: each operation
                  is a single event, so the Dispatcher machine's run-to-completion semantics
                  ARE the lock.
  atomic = false  the shipped lock-free design: each operation is a sequence of events with
                  yields between them, which is what lets the checker interleave two
                  operations inside one another.

The step boundaries in the lock-free path are the real code's atomic units -- individual
concurrent-map operations -- so an interleaving the checker finds is one the CLR may
produce.
*/

/* ---- the agent is singular; nodes are 1..N, 0 means "none" ---- */

event eGo: (disp: machine);

/* Leader -> dispatcher driver: place the agent on this node. */
event ePlace: (dest: int);

/* Dispatcher <-> operation drivers. Each is one atomic unit of the real code. */
event eFreshCheck: (dest: int, op: machine);
event eFreshCheckR: (drop: bool);
event eLaneFor: (dest: int, op: machine);
event eLaneForR: (lane: int);
event eTakeClaim: (dest: int, op: machine);
event eTakeClaimR: (displaced: int);
event eTryAdd: (dest: int, ticket: int, lane: int, op: machine);
event eTryAddR: (ok: bool);
event ePutBack: (dest: int, displaced: int, op: machine);
event ePutBackR;
event eTryWrite: (lane: int, ticket: int, op: machine);
event eTryWriteR: (ok: bool);
event eReleaseOne: (ticket: int, op: machine);
event eReleaseOneR;

/* ...and the coarse equivalents, which the one-lock design uses instead. */
event eEnqueueAtomic: (dest: int, op: machine);
event eEnqueueAtomicR;
event eAbandonAtomic: (node: int, op: machine);
event eAbandonAtomicR;

event eLaneRemove: (node: int, op: machine);
event eLaneRemoveR: (lane: int);
event eMarkAbandoned: (lane: int, op: machine);
event eMarkAbandonedR;
event eCloseWriter: (lane: int, op: machine);
event eCloseWriterR;
event eScan: (lane: int, node: int, op: machine);
event eScanR;
event eCancel: (lane: int, op: machine);
event eCancelR;

/* Lane worker <-> dispatcher. */
event eRead: (lane: int, op: machine);
event eReadR: (has: bool, ticket: int, dest: int);
event eAdmit: (lane: int, ticket: int, op: machine);
event eAdmitR: (ok: bool);

/* A node leaves the roster; the health check will abandon its lane. */
event eDepart: (node: int);

/* A lane's worker may only retire when the dispatcher confirms its queue is empty; any
   write after that spawns a fresh one. Parking forever instead would be a deadlock, and a
   plain idle budget would let a worker exit just before a late write and strand it. */
/* The leader's isOutstanding(agent, node): a dispatch already pending for THAT node
   suppresses a re-emission, and one pending for any other node does not. */
event eProbe: (op: machine);
event eProbeR: (dest: int);
event eTick;

event eTryExit: (lane: int, op: machine);
event eTryExitR: (retire: bool);
event eStarted: (node: int);
event eDeparted: (node: int);
event eNewTicket: (op: machine);
event eNewTicketR: (ticket: int);
/* Dispatcher -> leader: this agent's claim on that node is let go. */
event eReleased: (dest: int);

/* ---- monitor events ---- */
event eMConfig: (nodes: int);
/* The agent is now running on this node (a start that reached a live node). */
event eMStarted: (node: int);
/* A claim exists (dest > 0) or does not (dest == 0). */
event eMClaim: (dest: int);
/* Work outstanding: +1 opens, -1 closes. Quiescence is zero. */
event eMBusy: (delta: int);
/* A node's liveness. */
event eMNode: (node: int, alive: bool);

/* ------------------------------------------------------------------ */

machine Dispatcher {
  var atomic: bool;
  var nodes: int;

  /* _inFlight, for the one agent: the node a start is claimed for, 0 for none. */
  var claim: int;

  /* _queued, keyed by command value. With one agent a command IS its destination, so the
     key is the destination and the value is the owning dispatch's ticket. */
  var queued: map[int, int];

  /* _lanes: node -> lane generation, 0 for no lane. Generations are never reused, which is
     what the shipped code gets from comparing Lane references. */
  var lanes: map[int, int];
  var nextLane: int;

  /* Per dispatch: where it is aimed, which lane owns it, and whether it is still to run. */
  var dDest: map[int, int];
  var dLane: map[int, int];
  /* Where a command is aimed. Unlike dDest this outlives its release, because a released
     command is still sitting in its lane and will still be read. */
  var tDest: map[int, int];
  var nextTicket: int;

  /* Lane state: abandoned, and the buffer of tickets written to it. */
  var abandoned: map[int, bool];
  var closed: map[int, bool];
  var buffer: map[int, seq[int]];

  var leader: machine;
  var laneWorker: map[int, machine];

  start state Init {
    entry (p: (atomic: bool, nodes: int, leader: machine)) {
      atomic = p.atomic;
      nodes = p.nodes;
      leader = p.leader;
      nextLane = 1;
      nextTicket = 1;
      goto Serving;
    }
  }

  state Serving {
    /* ---------------- the one-lock design ---------------- */

    on eEnqueueAtomic do (p: (dest: int, op: machine)) {
      enqueueWhole(p.dest);
      send p.op, eEnqueueAtomicR;
    }

    on eAbandonAtomic do (p: (node: int, op: machine)) {
      abandonWhole(p.node);
      send p.op, eAbandonAtomicR;
    }

    /* ---------------- the lock-free design, one step per event ---------------- */

    on eFreshCheck do (p: (dest: int, op: machine)) {
      send p.op, eFreshCheckR, (drop = claim == p.dest,);
    }

    on eLaneFor do (p: (dest: int, op: machine)) {
      send p.op, eLaneForR, (lane = laneFor(p.dest),);
    }

    on eTakeClaim do (p: (dest: int, op: machine)) {
      var displaced: int;
      displaced = claim;
      claim = p.dest;
      announce eMClaim, (dest = claim,);
      send p.op, eTakeClaimR, (displaced = displaced,);
    }

    on eTryAdd do (p: (dest: int, ticket: int, lane: int, op: machine)) {
      if (p.dest in queued) {
        send p.op, eTryAddR, (ok = false,);
      } else {
        queued[p.dest] = p.ticket;
        dDest[p.ticket] = p.dest;
        dLane[p.ticket] = p.lane;
        tDest[p.ticket] = p.dest;
        announce eMBusy, (delta = 1,);
        send p.op, eTryAddR, (ok = true,);
      }
    }

    on ePutBack do (p: (dest: int, displaced: int, op: machine)) {
      /* Restore only where this enqueue's own value is still the one in the map. */
      if (claim == p.dest) {
        claim = p.displaced;
        announce eMClaim, (dest = claim,);
      }
      send p.op, ePutBackR;
    }

    on eTryWrite do (p: (lane: int, ticket: int, op: machine)) {
      if (p.lane in closed && closed[p.lane]) {
        send p.op, eTryWriteR, (ok = false,);
      } else {
        pushBuffer(p.lane, p.ticket);
        send p.op, eTryWriteR, (ok = true,);
      }
    }

    on eLaneRemove do (p: (node: int, op: machine)) {
      var lane: int;
      lane = 0;
      if (p.node in lanes) {
        lane = lanes[p.node];
        lanes -= (p.node);
      }
      send p.op, eLaneRemoveR, (lane = lane,);
    }

    on eMarkAbandoned do (p: (lane: int, op: machine)) {
      abandoned[p.lane] = true;
      send p.op, eMarkAbandonedR;
    }

    on eCloseWriter do (p: (lane: int, op: machine)) {
      closed[p.lane] = true;
      send p.op, eCloseWriterR;
    }

    on eScan do (p: (lane: int, node: int, op: machine)) {
      scanLane(p.lane, p.node);
      send p.op, eScanR;
    }

    on eCancel do (p: (lane: int, op: machine)) {
      abandoned[p.lane] = true;
      send p.op, eCancelR;
    }

    /* ---------------- lane workers ---------------- */

    on eRead do (p: (lane: int, op: machine)) {
      var t: int;
      if (p.lane in buffer && sizeof(buffer[p.lane]) > 0) {
        t = buffer[p.lane][0];
        buffer[p.lane] -= (0);
        send p.op, eReadR, (has = true, ticket = t, dest = tDest[t]);
      } else {
        send p.op, eReadR, (has = false, ticket = 0, dest = 0);
      }
    }

    on eAdmit do (p: (lane: int, ticket: int, op: machine)) {
      send p.op, eAdmitR, (ok = !(p.lane in abandoned && abandoned[p.lane]),);
    }

    on eReleaseOne do (p: (ticket: int, op: machine)) {
      releaseTicket(p.ticket, false);
      send p.op, eReleaseOneR;
    }

    on eTryExit do (p: (lane: int, op: machine)) {
      if (p.lane in buffer && sizeof(buffer[p.lane]) > 0) {
        send p.op, eTryExitR, (retire = false,);
      } else {
        laneWorker -= (p.lane);
        send p.op, eTryExitR, (retire = true,);
      }
    }

    on eProbe do (p: (op: machine)) {
      send p.op, eProbeR, (dest = claim,);
    }

    on eNewTicket do (p: (op: machine)) {
      send p.op, eNewTicketR, (ticket = freshTicket(),);
    }
  }

  /* ---------------- shared helpers ---------------- */

  fun laneFor(dest: int): int {
    var lane: int;
    if (dest in lanes) {
      return lanes[dest];
    }
    lane = nextLane;
    nextLane = nextLane + 1;
    lanes[dest] = lane;
    buffer[lane] = default(seq[int]);
    abandoned[lane] = false;
    closed[lane] = false;
    return lane;
  }

  fun pushBuffer(lane: int, ticket: int) {
    var s: seq[int];
    s = buffer[lane];
    s += (sizeof(s), ticket);
    buffer[lane] = s;
    if (!(lane in laneWorker)) {
      laneWorker[lane] = new LaneWorker((disp = this, lane = lane, leader = leader));
    }
  }

  /* release(): single-shot on the queue entry, then compare-and-remove on the claim. */
  fun releaseTicket(ticket: int, report: bool) {
    var dest: int;
    if (!(ticket in dDest)) {
      return;
    }
    dest = dDest[ticket];
    if (!(dest in queued) || queued[dest] != ticket) {
      return;
    }
    queued -= (dest);
    dDest -= (ticket);
    dLane -= (ticket);
    announce eMBusy, (delta = -1,);
    if (claim == dest) {
      claim = 0;
      announce eMClaim, (dest = claim,);
      if (report) {
        /* AbandonDispatchesExcept hands this back to the leader, which drops the matching
           ledger entry. Only the abandonment path reports; an ordinary completion is
           silent, and the leader finds out from the grid. */
        send leader, eReleased, (dest = dest,);
      }
    }
  }

  fun scanLane(lane: int, node: int) {
    var d: int;
    var doomed: seq[int];
    var i: int;
    foreach (d in keys(queued)) {
      if (dLane[queued[d]] == lane) {
        doomed += (sizeof(doomed), queued[d]);
      }
    }
    i = 0;
    while (i < sizeof(doomed)) {
      releaseTicket(doomed[i], true);
      i = i + 1;
    }
  }

  fun enqueueWhole(dest: int) {
    var lane: int;
    var ticket: int;
    if (claim == dest) {
      return;
    }
    lane = laneFor(dest);
    if (dest in queued) {
      return;
    }
    claim = dest;
    announce eMClaim, (dest = claim,);
    ticket = nextTicket;
    nextTicket = nextTicket + 1;
    queued[dest] = ticket;
    dDest[ticket] = dest;
    dLane[ticket] = lane;
    tDest[ticket] = dest;
    announce eMBusy, (delta = 1,);
    if (lane in closed && closed[lane]) {
      releaseTicket(ticket, false);
    } else {
      pushBuffer(lane, ticket);
    }
  }

  fun abandonWhole(node: int) {
    var lane: int;
    if (!(node in lanes)) {
      return;
    }
    lane = lanes[node];
    lanes -= (node);
    abandoned[lane] = true;
    closed[lane] = true;
    scanLane(lane, node);
  }

  fun freshTicket(): int {
    var t: int;
    t = nextTicket;
    nextTicket = nextTicket + 1;
    return t;
  }
}

/* ------------------------------------------------------------------ */
/* One Enqueue call. In atomic mode it is a single event; otherwise it is the real code's
   sequence, and every `receive` between two steps is a point the checker may interleave
   another operation into. */
machine EnqueueOp {
  var disp: machine;
  var atomic: bool;
  var dest: int;

  start state Run {
    entry (p: (disp: machine, atomic: bool, dest: int)) {
      disp = p.disp;
      atomic = p.atomic;
      dest = p.dest;
      if (atomic) { whole(); } else { stepwise(); }
      announce eMBusy, (delta = -1,);
      raise halt;
    }
  }

  fun whole() {
    send disp, eEnqueueAtomic, (dest = dest, op = this);
    receive { case eEnqueueAtomicR: { } }
  }

  fun stepwise() {
    var lane: int;
    var ticket: int;
    var displaced: int;
    var drop: bool;
    var ok: bool;

    send disp, eFreshCheck, (dest = dest, op = this);
    receive { case eFreshCheckR: (r: (drop: bool)) { drop = r.drop; } }
    if (drop) { return; }

    send disp, eLaneFor, (dest = dest, op = this);
    receive { case eLaneForR: (r: (lane: int)) { lane = r.lane; } }

    send disp, eTakeClaim, (dest = dest, op = this);
    receive { case eTakeClaimR: (r: (displaced: int)) { displaced = r.displaced; } }

    send disp, eNewTicket, (op = this,);
    receive { case eNewTicketR: (r: (ticket: int)) { ticket = r.ticket; } }

    send disp, eTryAdd, (dest = dest, ticket = ticket, lane = lane, op = this);
    receive { case eTryAddR: (r: (ok: bool)) { ok = r.ok; } }
    if (!ok) {
      send disp, ePutBack, (dest = dest, displaced = displaced, op = this);
      receive { case ePutBackR: { } }
      return;
    }

    send disp, eTryWrite, (lane = lane, ticket = ticket, op = this);
    receive { case eTryWriteR: (r: (ok: bool)) { ok = r.ok; } }
    if (!ok) {
      send disp, eReleaseOne, (ticket = ticket, op = this);
      receive { case eReleaseOneR: { } }
    }
  }
}

/* One AbandonLane call, from the health check. */
machine AbandonOp {
  var disp: machine;
  var atomic: bool;
  var node: int;

  start state Run {
    entry (p: (disp: machine, atomic: bool, node: int)) {
      disp = p.disp;
      atomic = p.atomic;
      node = p.node;
      if (atomic) { whole(); } else { stepwise(); }
      announce eMBusy, (delta = -1,);
      raise halt;
    }
  }

  fun whole() {
    send disp, eAbandonAtomic, (node = node, op = this);
    receive { case eAbandonAtomicR: { } }
  }

  fun stepwise() {
    var lane: int;

    send disp, eLaneRemove, (node = node, op = this);
    receive { case eLaneRemoveR: (r: (lane: int)) { lane = r.lane; } }
    if (lane == 0) { return; }

    send disp, eMarkAbandoned, (lane = lane, op = this);
    receive { case eMarkAbandonedR: { } }

    send disp, eCloseWriter, (lane = lane, op = this);
    receive { case eCloseWriterR: { } }

    send disp, eScan, (lane = lane, node = node, op = this);
    receive { case eScanR: { } }

    send disp, eCancel, (lane = lane, op = this);
    receive { case eCancelR: { } }
  }
}

/* A lane's worker: read, admit, execute, release -- forever. The execute step is outside
   any lock in BOTH designs, which is the one window a lock cannot close. */
machine LaneWorker {
  var disp: machine;
  var lane: int;
  var ticks: int;

  var leader: machine;

  start state Run {
    entry (p: (disp: machine, lane: int, leader: machine)) {
      disp = p.disp;
      lane = p.lane;
      leader = p.leader;
      pump();
    }
  }

  fun pump() {
    var has: bool;
    var ticket: int;
    var dest: int;
    var ok: bool;

    while (ticks < 12) {
      send disp, eRead, (lane = lane, op = this);
      receive { case eReadR: (r: (has: bool, ticket: int, dest: int)) {
        has = r.has; ticket = r.ticket; dest = r.dest; } }

      if (has) {
        ticks = ticks + 1;
        announce eMBusy, (delta = 1,);

        /* Admission. Deliberately a separate step from the execute below in BOTH designs:
           the command cannot be run while a lock is held, so this is the one window an
           atomic design does not close. */
        send disp, eAdmit, (lane = lane, ticket = ticket, op = this);
        receive { case eAdmitR: (r: (ok: bool)) { ok = r.ok; } }

        if (ok) {
          announce eMStarted, (node = dest,);
          send leader, eStarted, (node = dest,);
        }

        send disp, eReleaseOne, (ticket = ticket, op = this);
        receive { case eReleaseOneR: { } }

        announce eMBusy, (delta = -1,);
      } else {
        /* Nothing queued. Retiring is the dispatcher's call, so it cannot race a write. */
        send disp, eTryExit, (lane = lane, op = this);
        receive { case eTryExitR: (r: (retire: bool)) { ok = r.retire; } }
        if (ok) { return; }
      }
    }
  }
}

/* The leader's placement loop, cut down to what touches the dispatcher: it places the agent
   once, and places it again whenever the dispatcher reports the claim let go. */
machine Leader {
  var i: int;
  var disp: machine;
  var atomic: bool;
  var nodes: int;
  var places: int;
  var budget: int;
  /* What the grid shows: the node the agent is observed running on, 0 for nowhere. */
  var runsOn: int;
  var roster: map[int, bool];
  /* NodeAgentController's _pendingAssignments for this agent: the node a placement was sent
     to, 0 for none. While it is set the leader holds the agent there and emits nothing --
     which is exactly why a dispatch wedged against a departed node has to be released and
     REPORTED, so AbandonDispatchesExcept can drop the entry. */
  var pendDest: int;

  start state Init {
    entry (p: (atomic: bool, nodes: int, budget: int)) {
      atomic = p.atomic;
      nodes = p.nodes;
      budget = p.budget;
      i = 1;
      while (i <= nodes) {
        roster[i] = true;
        i = i + 1;
      }
    }
    on eGo do (p: (disp: machine)) {
      disp = p.disp;
      goto Placing;
    }
  }

  state Placing {
    entry {
      /* The first evaluation, ledger and all -- not a bare place(), or the next tick sees
         no hold and places the agent a second time. */
      pendDest = 1;
      place(1);
    }

    on eStarted do (p: (node: int)) {
      runsOn = p.node;
      pendDest = 0;
    }

    on eDeparted do (p: (node: int)) {
      roster -= (p.node);
      if (runsOn == p.node) {
        runsOn = 0;
      }
    }

    on eReleased do (p: (dest: int)) {
      /* AbandonDispatchesExcept: drop the hold this claim was backing, and only that one. */
      if (pendDest == p.dest) {
        pendDest = 0;
      }
      evaluate();
    }

    on eTick do {
      evaluate();
    }
  }

  /* Any node still in the roster; the leader never aims at one it has seen leave. */
  fun pickLive(): int {
    var live: seq[int];
    var n: int;
    foreach (n in keys(roster)) {
      live += (sizeof(live), n);
    }
    return live[choose(sizeof(live))];
  }

  /* One pass of the assignment evaluation, cut down to the agent this model follows. */
  fun evaluate() {
    var dest: int;
    var pending: int;

    if (places >= budget || runsOn != 0 || sizeof(roster) == 0) {
      return;
    }

    /* The ledger holds the agent on its destination until that placement is confirmed or
       the dispatcher reports the claim let go. */
    if (pendDest != 0) {
      return;
    }

    dest = pickLive();

    /* isOutstanding(agent, dest): only a dispatch aimed at THIS node suppresses. */
    send disp, eProbe, (op = this,);
    receive { case eProbeR: (r: (dest: int)) { pending = r.dest; } }
    if (pending == dest) {
      return;
    }

    pendDest = dest;
    place(dest);
  }

  fun place(dest: int) {
    places = places + 1;
    announce eMBusy, (delta = 1,);
    new EnqueueOp((disp = disp, atomic = atomic, dest = dest));
  }
}

/* The health check: a node leaves the roster, and its lane is abandoned. */
/* Keeps the leader's evaluation running, the way the health-check loop does. */
machine Ticker {
  start state Run {
    entry (p: (leader: machine, ticks: int)) {
      var i: int;
      i = 0;
      while (i < p.ticks) {
        send p.leader, eTick;
        i = i + 1;
      }
    }
  }
}

machine HealthCheck {
  var disp: machine;
  var atomic: bool;

  start state Run {
    entry (p: (disp: machine, atomic: bool, node: int, leader: machine, kill: bool)) {
      disp = p.disp;
      atomic = p.atomic;
      if (p.kill) {
        announce eMNode, (node = p.node, alive = false);
      }
      announce eMBusy, (delta = 1,);
      send p.leader, eDeparted, (node = p.node,);
      new AbandonOp((disp = disp, atomic = atomic, node = p.node));
      raise halt;
    }
  }
}
