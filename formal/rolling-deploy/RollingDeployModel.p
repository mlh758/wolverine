/*
GH-3987 + GH-3959: agent assignment under a Kubernetes rolling deploy, on nodes with a
finite capacity. See formal/rolling-deploy/README.md for the model, its scope, and the
mutant ledger.

Store    = node_assignments rows (per node, per agent) + live set + advertised per-node
           capacities + leadership + the "membership has been stable for the debounce
           window" oracle, serialized.
Node     = one process: its health-check tick, the local facts of running its agents,
           a capacity ceiling it dies beyond (the overload trip), and (when leader) the
           placement/shed/rebalance evaluation.
Deployer = the rollout: replace nodes one at a time (new pod ready, then old pod
           terminated), scale down (terminate without replacement), or scale out;
           brackets the whole thing with deploy-begin/deploy-end so the store's
           stability oracle is honest.
Wedge    = the GH-3987 trap: an agent silently stops running while its durable
           assignment row survives, so from the outside the table looks correct.
Trip     = the GH-3959 cascade seed: a node running past its capacity dies unless the
           leader's shed relieves it first; its share then redistributes — bounded by
           the survivors' ceilings, or in the mutant without them, fatally.
Squeeze  = dynamic capacity: memory pressure shrinks a hosting node's advertised
           ceiling at runtime (per-agent cost is not constant — an agent working off a
           backlog costs substantially more than an idle one), and the plane must shed
           down to the new number and recover the displaced agents when capacity returns.

Leadership correctness is assumed (proved by the leader-election spec); here it can
move when the leader node is rolled. Partitions and crashes are the agent-assignment
spec's territory; membership changes here are graceful (an overload death folds its
stale-eject into the departure).

The three design changes this spec exists to prove sound (the GH-3987/GH-3959 proposal):
  1. Rebalance moves of RUNNING agents are gated on the stability oracle; urgent
     placement of unowned agents is never gated.
  2. Every node's tick reconciles its own durable rows against what it is actually
     running, and re-drives a start for any row with no runner (skipping agents whose
     own stop/row-removal is still in flight).
  3. Placement is capacity-bounded (GH-3959): no target without headroom, an agent with
     nowhere to go WAITS unassigned, and a node found above its ceiling is shed — so one
     node's death is bounded degradation, not a cascade. Capacity is advertised on every
     heartbeat, not registered once: the plane follows the number wherever it moves, so
     the node-side provider may be a static config or a live pressure signal.
*/

event eJoin: (node: machine, id: int, cap: int);  // id and capacity ceiling, self-reported
event eJoined;
event eGo;

/* A health-check tick's round trip. The node RE-ADVERTISES its capacity on every sync:
   capacity is a live signal (a node under memory pressure advertises fewer slots), not a
   join-time constant — the assignment plane just follows the number wherever it moves. */
event eSync: (node: machine, id: int, cap: int);
event eSyncReply: (rows: map[int, map[int, bool]], live: map[int, bool], caps: map[int, int], amLeader: bool, stable: bool);

/* A node writes its OWN row after it starts an agent and removes it after a stop. */
event eAddRow: (agent: int, id: int);     // AddAssignmentAsync
event eRemoveRow: (agent: int, id: int);  // RemoveAssignmentAsync
event eRowRemoved: (agent: int);         // RemoveAssignmentAsync returned to the writer
/* Leader -> store placement decisions. */
event eAssignReq: (agent: int, id: int);  // AssignAgent
event eStopReq: (agent: int, id: int);    // StopRemoteAgent (healer or rebalance move)
event eRun: (agent: int, run: bool);      // store -> node, over a courier so it interleaves

event eRefill;                            // stand-in for "the health-check loop runs forever"

/* The rolling deploy. */
event eShutdown: (deployer: machine);    // deployer -> node: graceful pod termination
event eLeave: (id: int, deployer: machine); // node -> store: deregister, cascade row delete
event eReady;                             // replacement node -> deployer: pod is up
event eDeparted;                          // store -> deployer: the leave is absorbed
event eDeployBegin: (deployer: machine); // membership entering flux: stability oracle off
event eDeployAck;                         // store -> deployer: flux is recorded, safe to roll
event eDeployEnd;                         // membership stable again: oracle back on

/* The GH-3987 fault: an arsonist ARMS the store; the store fires the wedge on the next
   agent start it records, so the fault lands on an agent that really has a row. */
event eArmWedge;
event eWedge: (agent: int);              // the agent dies silently; the row survives

/* The dynamic-capacity fault: memory pressure rises on a node that is actually hosting
   agents (a backlog inflates per-agent cost, a noisy neighbor eats the headroom), so its
   advertised capacity SHRINKS at runtime. Armed like the wedge; fires on a row-adder. */
event eArmSqueeze;
event eSqueeze;

/* The GH-3959 fault: a node that finds itself RUNNING more agents than its capacity arms
   an overload trip on itself (over a courier, so relief can race it). If it is still over
   capacity when the trip lands, the node dies — the OOM kill whose redistribution is what
   cascaded in production. If the leader's shed relieved it first, it survives the spike. */
event eOverloadTrip;
event eTripped: (id: int);               // node -> store: died over capacity, eject me

/* rows[agent][node] = node holds a durable assignment row for agent.
   live[node] = node is a registered, non-departed member. */
machine Store {
  var agentCount: int;
  var rows: map[int, map[int, bool]];
  var live: map[int, bool];
  var caps: map[int, int];
  var members: map[int, machine];
  var leader: int;
  var stable: bool;
  var armedWedges: int;
  var armedSqueezes: int;

  start state Serving {
    entry (cfg: (agents: int)) {
      var a: int;
      var empty: map[int, bool];
      agentCount = cfg.agents;
      stable = true;
      a = 1;
      while (a <= agentCount) {
        rows[a] = empty;
        a = a + 1;
      }
    }

    on eArmWedge do { armedWedges = armedWedges + 1; }
    on eArmSqueeze do { armedSqueezes = armedSqueezes + 1; }

    /* The stability oracle: the debounce window is open for the whole rollout. Acked so
       the deployer cannot race its first pod event past this record. */
    on eDeployBegin do (m: (deployer: machine)) {
      stable = false;
      send m.deployer, eDeployAck;
      announceStore(0, 0);
    }

    on eDeployEnd do {
      var refills: int;
      stable = true;
      /* Re-opening rebalance is new work for the leader: refill so it gets ticks. */
      refills = refillAll();
      announceStore(0, refills);
    }

    on eJoin do (m: (node: machine, id: int, cap: int)) {
      var refills: int;
      members[m.id] = m.node;
      live[m.id] = true;
      caps[m.id] = m.cap;   // the ceiling is advertised alongside the node's identity
      send m.node, eJoined;
      /* Membership is a durable change the leader must get ticks to react to. */
      refills = refillAll();
      announceStore(0, refills);
    }

    /* Graceful departure: deregister and cascade-delete the node's assignment rows,
       exactly as deleting the node row does via FK cascade. */
    on eLeave do (m: (id: int, deployer: machine)) {
      var refills: int;
      refills = departNode(m.id);
      announceStore(0, refills);
      send m.deployer, eDeparted;
      /* Close the departure fault the node opened; the refills above keep the monitor
         hot while the leader re-places the orphaned agents. */
      announce eMFault, (delta = -1,);
    }

    /* An overload death: same ejection as a graceful leave (the stale-eject that follows
       an OOM kill is folded into it, as the sibling specs fold ejection hysteresis), but
       nobody is waiting on an ack. */
    on eTripped do (m: (id: int)) {
      var refills: int;
      refills = departNode(m.id);
      announceStore(0, refills);
      announce eMFault, (delta = -1,);
    }

    on eSync do (m: (node: machine, id: int, cap: int)) {
      var refills: int;
      /* The advertised capacity rides the heartbeat. A change is a durable change the
         leader must get ticks to react to — shed above the new ceiling, or backfill
         waiting agents into new headroom. */
      if ((m.id in caps) && caps[m.id] != m.cap) {
        caps[m.id] = m.cap;
        refills = refillAll();
        announceStore(0, refills);
      }
      /* Leadership handover (assumed correct): if no live leader, the syncer takes it. */
      if (leader == 0 || !(leader in live) || !live[leader]) {
        leader = m.id;
        announceStore(0, 0);
      }
      send m.node, eSyncReply, (rows = rows, live = live, caps = caps, amLeader = leader == m.id, stable = stable);
    }

    on eAddRow do (m: (agent: int, id: int)) {
      var refills: int;
      if ((m.id in live) && live[m.id] && !((m.id in rows[m.agent]) && rows[m.agent][m.id])) {
        rows[m.agent][m.id] = true;
        refills = refillAll();
        /* Fire one armed fault on the node whose agent just came up with a row. */
        if (armedWedges > 0) {
          armedWedges = armedWedges - 1;
          new StrikeCourier((target = members[m.id], agent = m.agent));
        } else if (armedSqueezes > 0) {
          armedSqueezes = armedSqueezes - 1;
          new SqueezeCourier(members[m.id]);
        }
      }
      announceRowDone(refills);
    }

    on eRemoveRow do (m: (agent: int, id: int)) {
      var refills: int;
      if ((m.id in rows[m.agent]) && rows[m.agent][m.id]) {
        rows[m.agent][m.id] = false;
        refills = refillAll();
      }
      /* RemoveAssignmentAsync returned: the writer's StopAgentAsync is complete. */
      send members[m.id], eRowRemoved, (agent = m.agent,);
      announceRowDone(refills);
    }

    on eAssignReq do (m: (agent: int, id: int)) {
      new RunCourier((target = members[m.id], agent = m.agent, run = true));
      announceStore(1, 0);
    }

    on eStopReq do (m: (agent: int, id: int)) {
      new RunCourier((target = members[m.id], agent = m.agent, run = false));
      announceStore(1, 0);
    }
  }

  fun refillAll(): int {
    var i: int;
    var n: int;
    foreach (i in keys(members)) {
      if (live[i]) {
        send members[i], eRefill;
        n = n + 1;
      }
    }
    return n;
  }

  /* Deregister a node and cascade-delete its assignment rows; returns the refill count. */
  fun departNode(id: int): int {
    var a: int;
    live[id] = false;
    a = 1;
    while (a <= agentCount) {
      if (id in rows[a]) {
        rows[a][id] = false;
      }
      a = a + 1;
    }
    return refillAll();
  }

  fun announceStore(newRuns: int, newRefills: int) {
    announce eMStore, (leader = leader, rows = rows, live = live, caps = caps, stable = stable, newRuns = newRuns, newRefills = newRefills);
  }

  fun announceRowDone(newRefills: int) {
    announceStore(0, newRefills);
    announce eMRowDone;
  }
}

/* Each command/fault rides its own courier so it interleaves with the ticks. */
machine RunCourier {
  start state Deliver {
    entry (m: (target: machine, agent: int, run: bool)) {
      send m.target, eRun, (agent = m.agent, run = m.run);
    }
  }
}

machine StrikeCourier {
  start state Deliver {
    entry (m: (target: machine, agent: int)) {
      send m.target, eWedge, (agent = m.agent,);
    }
  }
}

machine TripCourier {
  start state Deliver {
    entry (target: machine) {
      send target, eOverloadTrip;
    }
  }
}

machine SqueezeCourier {
  start state Deliver {
    entry (target: machine) {
      send target, eSqueeze;
    }
  }
}

machine Arsonist {
  start state Init {
    entry (cfg: (store: machine, kind: int)) {
      if (cfg.kind == 0) {
        send cfg.store, eArmWedge;
      } else {
        send cfg.store, eArmSqueeze;
      }
    }
  }
}

/* running[a] is the local fact of running agent a. pending is the leader's in-memory
   pending-assignment ledger (GH-3698): starts dispatched but not yet visible as rows;
   it dies with the node, and a new leader starting empty is safe because the duplicate
   healer mops up any double placement that slips through the handover. */
machine Node {
  var store: machine;
  var deployer: machine;
  var reportReady: bool;
  var id: int;
  var k: int;
  var cap: int;   // GH-3959: the most agents this node can host without dying
  var agentCount: int;
  var budget: int;
  var running: map[int, bool];
  /* An overload trip is armed and in flight (see eOverloadTrip); owes the monitor a
     fault-close, paid when the trip event is consumed whatever the outcome. */
  var tripPending: bool;
  /* My own RemoveAssignmentAsync in flight: StopAgentAsync isn't complete until the row
     removal returned, and the reconcile sweep must not resurrect an agent mid-stop. */
  var stopping: map[int, bool];
  var pending: map[int, int];   // leader-only: agent -> node dispatched, unconfirmed (0 = none)
  /* Ticks a pending entry has been held. The hold must EXPIRE (the real ledger's TTL +
     dispatcher probe): a stale stop crossing a newer start can consume the dispatched
     start without leaving a row, and an unconditional hold would then park the agent
     forever — PEx found exactly that trace when this counter was missing. */
  var pendingAge: map[int, int];

  start state Booting {
    entry (cfg: (store: machine, id: int, k: int, cap: int, agents: int, deployer: machine, reportReady: bool)) {
      var a: int;
      store = cfg.store;
      id = cfg.id;
      k = cfg.k;
      cap = cfg.cap;
      agentCount = cfg.agents;
      deployer = cfg.deployer;
      reportReady = cfg.reportReady;
      budget = k;
      a = 1;
      while (a <= agentCount) {
        running[a] = false;
        stopping[a] = false;
        pending[a] = 0;
        pendingAge[a] = 0;
        a = a + 1;
      }
      send store, eJoin, (node = this, id = id, cap = cap);
    }
    defer eGo;
    defer eShutdown;
    defer eWedge;
    defer eRun;
    defer eOverloadTrip;
    defer eSqueeze;
    on eJoined goto Idle;
  }

  state Idle {
    /* Busy until eGo: full budget, about to start — not quiescent before first placement. */
    entry {
      announceMe(true);
      if (reportReady) {
        send deployer, eReady;
      }
    }
    on eGo goto Ticking;
    on eRefill do { consumeRefill(); }
    on eRun do (m: (agent: int, run: bool)) { applyRun(m.agent, m.run); }
    on eRowRemoved do (m: (agent: int)) { stopping[m.agent] = false; }
    on eWedge do (m: (agent: int)) { applyWedge(m.agent); }
    on eSqueeze do { applySqueeze(); }
    on eOverloadTrip do { handleTrip(); }
    on eShutdown do (m: (deployer: machine)) { beginLeaving(m.deployer); }
    ignore eSyncReply;
  }

  state Ticking {
    entry {
      if (budget > 0) {
        announceMe(true);
        budget = budget - 1;
        send store, eSync, (node = this, id = id, cap = cap);
        goto AwaitSync;
      }
      announceMe(false);
    }
    on eRefill do { consumeRefill(); goto Ticking; }
    on eRun do (m: (agent: int, run: bool)) { applyRun(m.agent, m.run); goto Ticking; }
    on eRowRemoved do (m: (agent: int)) { stopping[m.agent] = false; }
    on eWedge do (m: (agent: int)) { applyWedge(m.agent); goto Ticking; }
    on eSqueeze do { applySqueeze(); goto Ticking; }
    on eOverloadTrip do { handleTrip(); }
    on eShutdown do (m: (deployer: machine)) { beginLeaving(m.deployer); }
    ignore eSyncReply;
  }

  state AwaitSync {
    on eSyncReply do (r: (rows: map[int, map[int, bool]], live: map[int, bool], caps: map[int, int], amLeader: bool, stable: bool)) {
      var work: bool;
      /* GH-3987 fix 2: reconcile this node's durable rows against what is actually
         running here, and re-drive anything assigned-but-not-running. */
      work = reconcile(r.rows);
      if (r.amLeader) {
        if (evaluate(r.rows, r.live, r.caps, r.stable)) {
          work = true;
        }
      }
      /* Keep ticking while there is placement work outstanding. */
      if (work && budget < k) {
        budget = k;
      }
      goto Ticking;
    }
    on eRefill do { consumeRefill(); }
    on eRun do (m: (agent: int, run: bool)) { applyRun(m.agent, m.run); }
    on eRowRemoved do (m: (agent: int)) { stopping[m.agent] = false; }
    on eWedge do (m: (agent: int)) { applyWedge(m.agent); }
    on eSqueeze do { applySqueeze(); }
    on eOverloadTrip do { handleTrip(); }
    on eShutdown do (m: (deployer: machine)) { beginLeaving(m.deployer); }
  }

  /* Gracefully gone: rows cascaded store-side; commands lost to this node still retire
     their couriers' accounting. */
  state Departed {
    on eRun do (m: (agent: int, run: bool)) { announce eMRunDone; }
    on eRefill do { announce eMRefillDone; }
    /* A trip armed before the departure still owes the monitor its fault-close. */
    on eOverloadTrip do {
      if (tripPending) {
        tripPending = false;
        announce eMFault, (delta = -1,);
      }
    }
    ignore eSyncReply;
    ignore eGo;
    ignore eWedge;
    ignore eSqueeze;
    ignore eRowRemoved;
    ignore eShutdown;
    ignore eJoined;
  }

  fun beginLeaving(dep: machine) {
    var a: int;
    /* Open the departure fault BEFORE dropping the runners: the monitor must not read
       the drop as quiescence. Closed by the store once the leave is absorbed. */
    announce eMFault, (delta = 1,);
    a = 1;
    while (a <= agentCount) {
      running[a] = false;
      a = a + 1;
    }
    announce eMNode, (id = id, running = running, cap = cap, alive = false, busy = false);
    send store, eLeave, (id = id, deployer = dep);
    goto Departed;
  }

  /* GH-3987 fix 2: an agent this node's durable row claims, that is not actually
     running here, is re-driven — unless this node's own stop of it is still in flight,
     in which case the row is about to disappear and resurrecting the agent would fight
     the stop (the GH-4240 residue class). */
  fun reconcile(rows: map[int, map[int, bool]]): bool {
    var a: int;
    var found: bool;
    a = 1;
    while (a <= agentCount) {
      if ((id in rows[a]) && rows[a][id] && !running[a] && !stopping[a]) {
        running[a] = true;
        announceMe(true);
        found = true;
      }
      a = a + 1;
    }
    if (found) {
      armTripIfOver();
    }
    return found;
  }

  /* The leader's evaluation: the capacity shed, urgent placement, and the duplicate
     healer always run; rebalance moves of running agents only once membership is stable
     (GH-3987 fix 1). Placement never targets a node without headroom (GH-3959): when
     nothing has headroom the agent WAITS unassigned rather than overload a node. */
  fun evaluate(rows: map[int, map[int, bool]], live: map[int, bool], caps: map[int, int], stable: bool): bool {
    var a: int;
    var i: int;
    var n: int;
    var work: bool;
    var cl: seq[int];
    var keep: int;
    var target: int;
    var maxN: int;
    var minN: int;
    var victim: int;
    var loads: map[int, int];
    var shedNode: int;

    /* Live row-holders per node = the load the distribution can see. */
    foreach (n in keys(live)) {
      if (live[n]) {
        loads[n] = 0;
      }
    }
    a = 1;
    while (a <= agentCount) {
      foreach (n in keys(rows[a])) {
        if (rows[a][n] && (n in loads)) {
          loads[n] = loads[n] + 1;
        }
      }
      a = a + 1;
    }

    /* GH-3959 shed: a node above its ceiling (an in-flight start landed on a node that
       filled up meanwhile) is relieved — one agent per tick — before the overload can
       trip it. The stopped agent becomes unowned and the urgent path re-places it
       somewhere with headroom, or it waits. */
    foreach (n in keys(loads)) {
      if (shedNode == 0 && (n in caps) && loads[n] > caps[n]) {
        shedNode = n;
      }
    }
    if (shedNode != 0) {
      victim = lowestRowedAgentOn(rows, shedNode);
      if (victim != 0) {
        announce eMShed;
        send store, eStopReq, (agent = victim, id = shedNode);
        work = true;
      }
    }

    a = 1;
    while (a <= agentCount) {
      cl = claimantsOf(rows[a], live);
      if (sizeof(cl) == 0) {
        if (pending[a] != 0 && (pending[a] in live) && live[pending[a]] && pendingAge[a] < 2) {
          /* A start is already on its way there (GH-3698 ledger): hold, and count the
             in-flight copy toward that node's share so the distribution balances the
             rest around it. Holding IS work — the leader keeps polling until the start
             confirms or the hold expires, exactly the TTL + dispatcher-probe backstop. */
          pendingAge[a] = pendingAge[a] + 1;
          if (pending[a] in loads) {
            loads[pending[a]] = loads[pending[a]] + 1;
          }
          work = true;
        } else if (pending[a] != 0 && (pending[a] in live) && live[pending[a]]) {
          /* The hold expired with the destination still live: re-drive the start to the
             SAME node. Deterministic re-dispatch is what keeps an unconfirmed start from
             spawning a rival copy elsewhere (the agent-assignment spec's rule) — and
             under GH-3959 it is also what keeps a retry capacity-neutral: the slot was
             already this agent's. A duplicate start there is idempotent; the retry is
             what heals the stale-stop-consumed-my-start race PEx found. */
          pendingAge[a] = 0;
          if (pending[a] in loads) {
            loads[pending[a]] = loads[pending[a]] + 1;
          }
          send store, eAssignReq, (agent = a, id = pending[a]);
          work = true;
        } else {
          /* Urgent placement: an agent with no live owner is NEVER debounced — but it is
             capacity-bounded (GH-3959): only a node with headroom may take it, and if
             nothing has headroom the agent waits unassigned for capacity to appear
             (a node joins, a backlog drains, a shed frees a slot) rather than overload a
             survivor. */
          target = minLoadedWithHeadroom(loads, caps);
          if (target != 0) {
            pending[a] = target;
            pendingAge[a] = 0;
            loads[target] = loads[target] + 1;
            send store, eAssignReq, (agent = a, id = target);
            work = true;
          }
        }
      } else {
        /* Durable evidence supersedes the ledger. */
        pending[a] = 0;
        pendingAge[a] = 0;
        if (sizeof(cl) >= 2) {
          /* GH-2602 healer: keep one copy (highest id, arbitrary), stop the rest. */
          keep = cl[0];
          i = 1;
          while (i < sizeof(cl)) {
            if (cl[i] > keep) {
              keep = cl[i];
            }
            i = i + 1;
          }
          i = 0;
          while (i < sizeof(cl)) {
            if (cl[i] != keep) {
              send store, eStopReq, (agent = a, id = cl[i]);
            }
            i = i + 1;
          }
          work = true;
        }
      }
      a = a + 1;
    }

    if (stable) { /* GH-3987: rebalance only once membership has settled */
      maxN = maxLoaded(loads);
      minN = minLoaded(loads);
      if (maxN != 0 && minN != 0 && loads[maxN] - loads[minN] >= 2 && (minN in caps) && loads[minN] < caps[minN]) {
        victim = lowestRowedAgentOn(rows, maxN);
        if (victim != 0) {
          /* A move is detach-then-replace: the stop makes the agent unowned and the
             urgent path re-places it on the underloaded node next tick. One move per
             tick, so churn is observable and bounded. */
          announce eMMove;
          send store, eStopReq, (agent = victim, id = maxN);
          work = true;
        }
      }
    }

    return work;
  }

  fun claimantsOf(claims: map[int, bool], live: map[int, bool]): seq[int] {
    var n: int;
    var res: seq[int];
    foreach (n in keys(claims)) {
      if (claims[n] && (n in live) && live[n]) {
        res += (sizeof(res), n);
      }
    }
    return res;
  }

  fun runningCount(): int {
    var a: int;
    var c: int;
    a = 1;
    while (a <= agentCount) {
      if (running[a]) {
        c = c + 1;
      }
      a = a + 1;
    }
    return c;
  }

  /* GH-3959: running past the KILL LINE arms an overload trip on this node, delivered
     over a courier so the leader's relief (a shed stop) genuinely RACES the death, the
     way an OOM kill races a rebalance in production. The kill line sits one agent ABOVE
     the advertised ceiling: a ceiling is a planning limit provisioned with margin below
     the real limit (the incident's own numbers: ~6.4 GB planned against a 9.2 GB kill),
     because no ceiling can recall a start already on the wire — one in-flight race must
     fit in the gap. Opens a fault so a settled-looking moment with a trip in flight is
     never read as quiescence; the trip's consumption closes it. */
  fun armTripIfOver() {
    if (!tripPending && runningCount() > cap + 1) {
      tripPending = true;
      announce eMFault, (delta = 1,);
      new TripCourier(this);
    }
  }

  fun handleTrip() {
    var a: int;
    tripPending = false;
    if (runningCount() > cap + 1) {
      /* Still over capacity when the trip lands: the node dies. Open the departure fault
         BEFORE closing the trip fault and dropping the runners, so the monitor never
         sees a settled-looking instant mid-death. The eMTrip announce is what the
         cascade-floor property counts. */
      announce eMFault, (delta = 1,);
      announce eMTrip;
      announce eMFault, (delta = -1,);
      a = 1;
      while (a <= agentCount) {
        running[a] = false;
        a = a + 1;
      }
      announce eMNode, (id = id, running = running, cap = cap, alive = false, busy = false);
      send store, eTripped, (id = id,);
      goto Departed;
    }
    /* Relieved before the trip landed — the shed (or a stop) won the race. */
    announce eMFault, (delta = -1,);
  }

  fun minLoadedWithHeadroom(loads: map[int, int], caps: map[int, int]): int {
    var n: int;
    var best: int;
    foreach (n in keys(loads)) {
      if ((n in caps) && loads[n] < caps[n]) {
        if (best == 0 || loads[n] < loads[best] || (loads[n] == loads[best] && n < best)) {
          best = n;
        }
      }
    }
    return best;
  }

  fun minLoaded(loads: map[int, int]): int {
    var n: int;
    var best: int;
    foreach (n in keys(loads)) {
      if (best == 0 || loads[n] < loads[best] || (loads[n] == loads[best] && n < best)) {
        best = n;
      }
    }
    return best;
  }

  fun maxLoaded(loads: map[int, int]): int {
    var n: int;
    var best: int;
    foreach (n in keys(loads)) {
      if (best == 0 || loads[n] > loads[best] || (loads[n] == loads[best] && n > best)) {
        best = n;
      }
    }
    return best;
  }

  fun lowestRowedAgentOn(rows: map[int, map[int, bool]], n: int): int {
    var a: int;
    a = 1;
    while (a <= agentCount) {
      if ((n in rows[a]) && rows[a][n]) {
        return a;
      }
      a = a + 1;
    }
    return 0;
  }

  fun applyRun(a: int, run: bool) {
    if (run) {
      if (!running[a]) {
        /* Announce the pending row write BEFORE the state that changes runner counts. */
        announce eMRowPending;
        running[a] = true;
        announceMe(budget > 0);
        send store, eAddRow, (agent = a, id = id);
        armTripIfOver();
      }
      /* else: duplicate start; idempotent, like StartAgentAsync. */
    } else {
      /* StopAgentAsync: stop whatever is here, then remove the row unconditionally.
         The row-pending announce comes first so the runner-count drop is never read
         as quiescence. */
      stopping[a] = true;
      announce eMRowPending;
      running[a] = false;
      announceMe(budget > 0);
      send store, eRemoveRow, (agent = a, id = id);
    }
    announce eMRunDone;
  }

  /* The GH-3987 fault. Deliberately NOT tracked as an open monitor fault: the node's
     own busy/budget machinery keeps the monitor hot exactly as long as the health-check
     sweep still has a chance to reconcile, and then the cluster SETTLES into the bad
     state — which is what lets the assigned-but-not-running assertion catch a design
     with no reconcile sweep as a safety violation rather than a liveness hang. */
  fun applyWedge(a: int) {
    if (running[a] && !stopping[a]) {
      running[a] = false;
      if (budget < k) {
        budget = k;
      }
      /* One announce carries the runner drop and the busy flag atomically. */
      announceMe(true);
    }
  }

  fun consumeRefill() {
    if (budget < k) {
      budget = k;
    }
    /* Announce busy before signalling the refill consumed (see leader-election). */
    announceMe(true);
    announce eMRefillDone;
  }

  fun announceMe(busy: bool) {
    announce eMNode, (id = id, running = running, cap = cap, alive = true, busy = busy);
  }

  /* Memory pressure: the node's own pressure-to-slots translation shrinks the advertised
     capacity (floor 1). The next sync advertises it and the leader sheds down to it. The
     kill line moves with the ceiling — pressure means the margin is thinner — so a
     squeeze can also arm a trip. */
  fun applySqueeze() {
    if (cap > 1) {
      cap = cap - 1;
      if (budget < k) {
        budget = k;
      }
      announceMe(true);
      armTripIfOver();
    }
  }
}

/* The rollout: for each victim, bring a replacement up, wait for it to be ready, then
   gracefully terminate the old node and wait for the store to absorb the departure —
   one pod at a time, like a maxUnavailable=0 rolling update. Then any scale-down
   terminations (no replacement), then any pure scale-out joins. Brackets everything in
   deploy-begin/end so the stability oracle is honest. */
machine Deployer {
  var store: machine;
  var victims: seq[machine];
  var downs: seq[machine];
  var extraJoins: int;
  var k: int;
  var cap: int;
  var agentCount: int;
  var nextId: int;
  var step: int;
  var removed: int;
  var joins: int;

  start state Init {
    entry (cfg: (store: machine, victims: seq[machine], downs: seq[machine], extraJoins: int, k: int, cap: int, agents: int, firstId: int)) {
      store = cfg.store;
      victims = cfg.victims;
      downs = cfg.downs;
      extraJoins = cfg.extraJoins;
      k = cfg.k;
      cap = cfg.cap;
      agentCount = cfg.agents;
      nextId = cfg.firstId;
      send store, eDeployBegin, (deployer = this,);
    }
    on eDeployAck do { nextPhase(); }
    defer eReady;
    defer eDeparted;
  }

  state SpawnReplacement {
    entry {
      spawnNode();
    }
    on eReady goto TerminateOld;
    defer eDeparted;
  }

  state TerminateOld {
    entry {
      send victims[step], eShutdown, (deployer = this,);
    }
    on eDeparted do {
      step = step + 1;
      nextPhase();
    }
    defer eReady;
  }

  /* Scale-down: terminate without a replacement — the share of the departed node has to
     fit (or wait) within the survivors' ceilings. */
  state ScaleDown {
    entry {
      send downs[removed], eShutdown, (deployer = this,);
    }
    on eDeparted do {
      removed = removed + 1;
      nextPhase();
    }
    defer eReady;
  }

  state ScaleOut {
    entry {
      spawnNode();
    }
    on eReady do {
      joins = joins + 1;
      nextPhase();
    }
    defer eDeparted;
  }

  state Done {
    ignore eReady;
    ignore eDeparted;
  }

  fun nextPhase() {
    if (step < sizeof(victims)) {
      goto SpawnReplacement;
    }
    if (removed < sizeof(downs)) {
      goto ScaleDown;
    }
    if (joins < extraJoins) {
      goto ScaleOut;
    }
    send store, eDeployEnd;
    goto Done;
  }

  fun spawnNode() {
    var replacement: machine;
    announce eMRoster, (delta = 1,);
    replacement = new Node((store = store, id = nextId, k = k, cap = cap, agents = agentCount, deployer = this, reportReady = true));
    nextId = nextId + 1;
    send replacement, eGo;
  }
}
