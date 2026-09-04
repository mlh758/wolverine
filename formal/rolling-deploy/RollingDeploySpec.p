/*
What agent assignment owes a cluster being rolled. See the README; asserted at
quiescence, because transient duplicates and unowned agents are unavoidable mid-flight.

  Safety at every quiet point —
    (1) every agent runs on exactly one live node, and the durable rows agree: in
        particular, never "assigned in node_assignments but not running" (GH-3987);
    (2) no durable row survives on a departed node;
    (3) at most ONE rebalance move is ever dispatched while membership is in flux
        (slack of one for the stability-snapshot race) — a rollout causes one
        rebalance, not one per pod event;
    (4) no node is at rest above its capacity ceiling, and an agent waits unassigned
        ONLY when no live node has headroom (GH-3959's bounded degradation);
    (5) at most ONE node dies over capacity per run (slack of one for a start already
        in flight) — redistribution must never overload the next node: the cascade
        has a floor;
    (6) once membership is stable, the load is even up to capacity (a fuller node next
        to a node with no headroom is not an imbalance).
  Liveness — quiescence is reached (the hot state catches a wedge never reconciled,
        an orphan never re-placed, or a rebalance that never settles).
*/

event eMAgents: (count: int);
event eMRoster: (delta: int);
event eMNode: (id: int, running: map[int, bool], cap: int, alive: bool, busy: bool);
event eMStore: (leader: int, rows: map[int, map[int, bool]], live: map[int, bool], caps: map[int, int], stable: bool, newRuns: int, newRefills: int);
event eMRunDone;
event eMRefillDone;
/* A node's durable row write (add/remove) in flight to the store, and its completion. */
event eMRowPending;
event eMRowDone;
/* A graceful departure in progress but not yet absorbed by the store (+1 opens,
   -1 closes). A wedge is deliberately NOT a fault here: the wedged cluster must be
   allowed to settle so the assigned-but-not-running assertion can see it. */
event eMFault: (delta: int);
/* The leader dispatched a rebalance move (a stop of a running agent for evenness). */
event eMMove;
/* The leader dispatched a shed (a stop relieving a node found above its ceiling). */
event eMShed;
/* A node died over capacity (the GH-3959 overload). One per run can be an in-flight
   race; a second means the redistribution of the first overloaded someone — the cascade. */
event eMTrip;

spec DeployConverges observes eMAgents, eMRoster, eMNode, eMStore, eMRunDone, eMRefillDone, eMRowPending, eMRowDone, eMFault, eMMove, eMShed, eMTrip {
  var agents: int;
  var roster: int;
  var running: map[int, map[int, bool]];   // node -> agent -> running locally
  var alive: map[int, bool];
  var busy: map[int, bool];
  var rows: map[int, map[int, bool]];      // agent -> node -> durable row
  var live: map[int, bool];
  var caps: map[int, int];                 // node -> capacity ceiling (node-announced truth)
  var stable: bool;
  /* Run-state commands emitted by the store but not yet applied by their target. */
  var pendingRuns: int;
  /* Refill waves sent but not yet consumed. */
  var pendingRefills: int;
  /* Durable row writes emitted by a node but not yet applied by the store. */
  var pendingRows: int;
  /* Departures/wedges injected but not yet fully absorbed. */
  var pendingFaults: int;
  /* Rebalance moves dispatched while membership was in flux. */
  var unstableMoves: int;
  /* Nodes that died over capacity. */
  var trips: int;

  start cold state Quiet {
    on eMAgents do (p: (count: int)) { agents = p.count; check(); }
    on eMRoster do (p: (delta: int)) { roster = roster + p.delta; check(); }
    on eMNode do (p: (id: int, running: map[int, bool], cap: int, alive: bool, busy: bool)) { node(p); }
    on eMStore do (p: (leader: int, rows: map[int, map[int, bool]], live: map[int, bool], caps: map[int, int], stable: bool, newRuns: int, newRefills: int)) { st(p); }
    on eMRunDone do { pendingRuns = pendingRuns - 1; check(); }
    on eMRefillDone do { pendingRefills = pendingRefills - 1; check(); }
    on eMRowPending do { pendingRows = pendingRows + 1; check(); }
    on eMRowDone do { pendingRows = pendingRows - 1; check(); }
    on eMFault do (p: (delta: int)) { pendingFaults = pendingFaults + p.delta; check(); }
    on eMMove do { move(); }
    on eMShed do { check(); }
    on eMTrip do { trip(); }
  }

  hot state Working {
    on eMAgents do (p: (count: int)) { agents = p.count; check(); }
    on eMRoster do (p: (delta: int)) { roster = roster + p.delta; check(); }
    on eMNode do (p: (id: int, running: map[int, bool], cap: int, alive: bool, busy: bool)) { node(p); }
    on eMStore do (p: (leader: int, rows: map[int, map[int, bool]], live: map[int, bool], caps: map[int, int], stable: bool, newRuns: int, newRefills: int)) { st(p); }
    on eMRunDone do { pendingRuns = pendingRuns - 1; check(); }
    on eMRefillDone do { pendingRefills = pendingRefills - 1; check(); }
    on eMRowPending do { pendingRows = pendingRows + 1; check(); }
    on eMRowDone do { pendingRows = pendingRows - 1; check(); }
    on eMFault do (p: (delta: int)) { pendingFaults = pendingFaults + p.delta; check(); }
    on eMMove do { move(); }
    on eMShed do { check(); }
    on eMTrip do { trip(); }
  }

  fun node(p: (id: int, running: map[int, bool], cap: int, alive: bool, busy: bool)) {
    running[p.id] = p.running;
    /* Capacity is tracked from the NODE's own announcements — the source of truth — so a
       leader or store acting on a stale advertised value cannot fool the assertions. */
    caps[p.id] = p.cap;
    alive[p.id] = p.alive;
    busy[p.id] = p.busy;
    check();
  }

  fun st(p: (leader: int, rows: map[int, map[int, bool]], live: map[int, bool], caps: map[int, int], stable: bool, newRuns: int, newRefills: int)) {
    rows = p.rows;
    live = p.live;
    stable = p.stable;
    pendingRuns = pendingRuns + p.newRuns;
    pendingRefills = pendingRefills + p.newRefills;
    check();
  }

  fun trip() {
    /* The cascade floor. One overload death per run can be an already-in-flight start
       landing on a node that filled up meanwhile — a ceiling cannot recall a command on
       the wire, and neither can a memory limit. A SECOND death means the redistribution
       of the first pushed another node over: the GH-3959 cascade, exactly what the
       ceiling exists to prevent. */
    trips = trips + 1;
    assert trips <= 1,
      format("cascading overload: {0} nodes died over capacity in one run — redistributing a dead node's share overloaded another node (allowed slack: 1 for a start already in flight)", trips);
    check();
  }

  fun move() {
    /* One rollout must cause one rebalance, not one per pod event. The slack of one is
       the stability-snapshot race: a leader may act once on a reply that predates the
       deploy-begin record — the same race a wall-clock debounce window has with a pod
       event. Anything more means the gate is not doing its job. */
    if (!stable) {
      unstableMoves = unstableMoves + 1;
      assert unstableMoves <= 1,
        format("the deploy window was ignored: {0} rebalance moves were dispatched while membership was in flux (allowed slack: 1 for the stability-snapshot race)", unstableMoves);
    }
    check();
  }

  fun check() {
    if (settled()) {
      converged();
      goto Quiet;
    } else {
      goto Working;
    }
  }

  fun settled(): bool {
    var i: int;
    if (agents == 0 || roster == 0 || sizeof(alive) < roster) {
      return false;
    }
    if (pendingRuns > 0 || pendingRefills > 0 || pendingRows > 0 || pendingFaults > 0) {
      return false;
    }
    foreach (i in keys(busy)) {
      if (busy[i]) {
        return false;
      }
    }
    return true;
  }

  fun converged() {
    var a: int;
    var n: int;
    var runners: int;
    var runnerNode: int;
    var claims: int;
    var claimNode: int;
    var loads: map[int, int];
    var headroomNode: int;
    var maxL: int;
    var maxLNode: int;
    var minHL: int;

    /* Settled load per live node, from the durable rows (at rest, rows == runners). */
    foreach (n in keys(live)) {
      if (live[n]) {
        loads[n] = 0;
      }
    }
    a = 1;
    while (a <= agents) {
      if (a in rows) {
        foreach (n in keys(rows[a])) {
          if (rows[a][n] && (n in loads)) {
            loads[n] = loads[n] + 1;
          }
        }
      }
      a = a + 1;
    }

    /* GH-3959: no node is ever at rest above its advertised capacity. */
    headroomNode = 0;
    foreach (n in keys(loads)) {
      assert !(n in caps) || loads[n] <= caps[n],
        format("node {0} settled with {1} agents but its capacity is {2} — the GH-3959 overload, at rest", n, loads[n], caps[n]);
      if (headroomNode == 0 && (n in caps) && loads[n] < caps[n]) {
        headroomNode = n;
      }
    }

    a = 1;
    while (a <= agents) {
      runners = 0;
      claims = 0;
      foreach (n in keys(running)) {
        if (alive[n] && (a in running[n]) && running[n][a]) {
          runners = runners + 1;
          runnerNode = n;
        }
      }
      if (a in rows) {
        foreach (n in keys(rows[a])) {
          if (rows[a][n]) {
            claims = claims + 1;
            claimNode = n;
            assert (n in live) && live[n],
              format("agent {0} still has a durable assignment row on departed node {1}", a, n);
          }
        }
      }

      /* The GH-3987 trap, stated directly: the table says the agent is owned, but no
         process is actually running it — and the cluster is at rest, so nothing will
         ever fix it. */
      assert !(claims >= 1 && runners == 0),
        format("agent {0} is assigned in the durable table (node {1}) but is not running anywhere — assigned-but-not-running, the GH-3987 trap", a, claimNode);

      if (runners == 0 && claims == 0) {
        /* GH-3959's deliberate degradation: an agent may wait unassigned — but ONLY
           when no live node could take it. Waiting while capacity exists is the shed
           trade gone wrong (the "parked forever" failure the ceiling must not create). */
        assert headroomNode == 0,
          format("agent {0} is not running anywhere while node {1} still has headroom — it should have been placed", a, headroomNode);
      } else {
        assert runners == 1,
          format("settled with {0} nodes running agent {1} (expected exactly one)", runners, a);
        assert claims == 1 && claimNode == runnerNode,
          format("agent {0} runs on node {1} but the durable rows disagree ({2} row(s), e.g. node {3})", a, runnerNode, claims, claimNode);
      }
      a = a + 1;
    }

    /* Even distribution is only owed once membership has been stable — and only up to
       capacity: a fuller node next to a node with NO headroom is not an imbalance the
       leader is allowed to fix. */
    if (stable) {
      maxL = -1;
      minHL = -1;
      foreach (n in keys(loads)) {
        if (maxL == -1 || loads[n] > maxL) {
          maxL = loads[n];
          maxLNode = n;
        }
        if ((n in caps) && loads[n] < caps[n]) {
          if (minHL == -1 || loads[n] < minHL) {
            minHL = loads[n];
          }
        }
      }
      if (maxL != -1 && minHL != -1) {
        assert maxL - minHL <= 1,
          format("membership is stable but the settled load is uneven: node {0} holds {1} while another node with headroom holds {2}", maxLNode, maxL, minHL);
      }
    }
  }
}
