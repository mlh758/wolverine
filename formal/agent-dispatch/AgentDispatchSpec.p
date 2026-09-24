/*
What the dispatcher's claim bookkeeping owes the leader.

  Safety   -- at quiescence the agent runs on at most one live node, and no claim is left
              behind with nothing outstanding to act on it.
  Liveness -- quiescence is reached (the hot state catches an agent that never settles).

Asserted at quiescence rather than continuously, because both a second start and a
momentarily absent claim are legitimate mid-flight: a re-target starts the agent on a new
node while the doomed dispatch is still queued against the old one, and a release is
followed by a re-placement. It is where the system comes to rest that has to be right.
*/

spec ClaimsStaySound observes eMConfig, eMStarted, eMClaim, eMBusy, eMNode {
  var nodes: int;
  /* Node -> the agent is running there. A start that reaches a departed node does nothing. */
  var running: map[int, bool];
  var alive: map[int, bool];
  /* _inFlight for the one agent: the node a start is claimed for, 0 for none. */
  var claim: int;
  /* Operations and commands in flight. Quiescence is zero. */
  var busy: int;

  start cold state Quiet {
    on eMConfig do (p: (nodes: int)) { config(p.nodes); }
    on eMStarted do (p: (node: int)) { started(p.node); }
    on eMClaim do (p: (dest: int)) { claim = p.dest; check(); }
    on eMBusy do (p: (delta: int)) { busy = busy + p.delta; check(); }
    on eMNode do (p: (node: int, alive: bool)) { node(p.node, p.alive); }
  }

  hot state Working {
    on eMConfig do (p: (nodes: int)) { config(p.nodes); }
    on eMStarted do (p: (node: int)) { started(p.node); }
    on eMClaim do (p: (dest: int)) { claim = p.dest; check(); }
    on eMBusy do (p: (delta: int)) { busy = busy + p.delta; check(); }
    on eMNode do (p: (node: int, alive: bool)) { node(p.node, p.alive); }
  }

  fun config(n: int) {
    var i: int;
    nodes = n;
    i = 1;
    while (i <= n) {
      alive[i] = true;
      running[i] = false;
      i = i + 1;
    }
    check();
  }

  fun started(n: int) {
    /* A command that reaches a node which has left the cluster starts nothing. */
    if (n in alive && alive[n]) {
      running[n] = true;
    }
    check();
  }

  fun node(n: int, up: bool) {
    alive[n] = up;
    if (!up) {
      running[n] = false;
    }
    check();
  }

  fun check() {
    if (nodes > 0 && busy == 0) {
      settled();
      goto Quiet;
    } else {
      goto Working;
    }
  }

  fun settled() {
    var i: int;
    var runners: int;
    var where: int;

    foreach (i in keys(running)) {
      if (running[i]) {
        runners = runners + 1;
        where = i;
      }
    }

    assert runners <= 1,
      format("settled with {0} live nodes running the one agent (duplicate placement)", runners);

    /* Nothing is outstanding, so a surviving claim is one no dispatch will ever release:
       the agent is suppressed from that node forever and reads as permanently pending. */
    assert claim == 0,
      format("settled with nothing in flight, yet a claim on node {0} is still held", claim);
  }
}
