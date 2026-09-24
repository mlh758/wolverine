/*
Two designs, the same schedules. `Atomic` is one lock around claim publication and the
abandonment scan; `LockFree` is the shipped design's individual concurrent-map operations.
See the README for what each run found.
*/

machine AtomicSteadyDriver {
  start state Init { entry { begin(true, 2, 0, false, 3); } }
}

machine LockFreeSteadyDriver {
  start state Init { entry { begin(false, 2, 0, false, 3); } }
}

/* The node dies as it leaves: a command that slips through reaches nothing. */
machine AtomicDepartureDriver {
  start state Init { entry { begin(true, 2, 1, true, 3); } }
}

machine LockFreeDepartureDriver {
  start state Init { entry { begin(false, 2, 1, true, 3); } }
}

/* The node leaves the ROSTER but is still running -- the lagging-snapshot case the ejection
   hysteresis exists for, and the only one in which a late command does damage. */
machine AtomicStaleDriver {
  start state Init { entry { begin(true, 2, 1, false, 3); } }
}

machine LockFreeStaleDriver {
  start state Init { entry { begin(false, 2, 1, false, 3); } }
}

fun begin(atomic: bool, nodes: int, departures: int, kill: bool, budget: int) {
  var disp: machine;
  var leader: machine;
  var i: int;

  announce eMConfig, (nodes = nodes,);

  leader = new Leader((atomic = atomic, nodes = nodes, budget = budget));
  disp = new Dispatcher((atomic = atomic, nodes = nodes, leader = leader));
  send leader, eGo, (disp = disp,);
  new Ticker((leader = leader, ticks = 3));

  i = 0;
  while (i < departures) {
    new HealthCheck((disp = disp, atomic = atomic, node = choose(nodes) + 1,
                     leader = leader, kill = kill));
    i = i + 1;
  }
}

test tcAtomicSteady [main=AtomicSteadyDriver]:
  assert ClaimsStaySound in
    { AtomicSteadyDriver, Ticker, Dispatcher, Leader, EnqueueOp, AbandonOp, LaneWorker, HealthCheck };

test tcLockFreeSteady [main=LockFreeSteadyDriver]:
  assert ClaimsStaySound in
    { LockFreeSteadyDriver, Ticker, Dispatcher, Leader, EnqueueOp, AbandonOp, LaneWorker, HealthCheck };

test tcAtomicDeparture [main=AtomicDepartureDriver]:
  assert ClaimsStaySound in
    { AtomicDepartureDriver, Ticker, Dispatcher, Leader, EnqueueOp, AbandonOp, LaneWorker, HealthCheck };

test tcLockFreeDeparture [main=LockFreeDepartureDriver]:
  assert ClaimsStaySound in
    { LockFreeDepartureDriver, Ticker, Dispatcher, Leader, EnqueueOp, AbandonOp, LaneWorker, HealthCheck };

test tcAtomicStale [main=AtomicStaleDriver]:
  assert ClaimsStaySound in
    { AtomicStaleDriver, Ticker, Dispatcher, Leader, EnqueueOp, AbandonOp, LaneWorker, HealthCheck };

test tcLockFreeStale [main=LockFreeStaleDriver]:
  assert ClaimsStaySound in
    { LockFreeStaleDriver, Ticker, Dispatcher, Leader, EnqueueOp, AbandonOp, LaneWorker, HealthCheck };
