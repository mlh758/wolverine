/*
The cases (see the README). *Stock* means the shipped code: no node-side reconcile sweep.
The two Stock fault cases are COMMITTED COUNTEREXAMPLES — they are expected to violate the
spec, because the shipped system genuinely settles into a duplicate (see the README's bug
ledger). The Sweep variants run the same faults with the GH-4297 node-side reconcile
enabled and must converge.

Faults strike via an Arsonist: kinds 0/1 hit the current owner, kind 2 crashes the leader
at the instant it dispatches a start — the rolling deploy terminating the leader pod with a
placement in flight.
*/

machine SteadyDriver {
  start state Init { entry { begin(3, 0, 0, 0, false); } }
}

machine CrashDriver {
  start state Init { entry { begin(3, 0, 1, 0, false); } }
}

machine PartitionStockDriver {
  start state Init { entry { begin(3, 1, 0, 0, false); } }
}

machine PartitionSweepDriver {
  start state Init { entry { begin(3, 1, 0, 0, true); } }
}

machine HandoverStockDriver {
  start state Init { entry { begin(3, 0, 0, 1, false); } }
}

machine HandoverSweepDriver {
  start state Init { entry { begin(3, 0, 0, 1, true); } }
}

machine ChaosSweepDriver {
  start state Init { entry { begin(3, 1, 1, 1, true); } }
}

fun begin(n: int, partitions: int, crashes: int, leaderKills: int, sweep: bool) {
  var store: machine;
  var nodes: seq[machine];
  var i: int;

  announce eMRoster, (count = n,);

  store = new Store();
  i = 0;
  while (i < n) {
    nodes += (i, new Node((store = store, id = i + 1, k = 3, sweep = sweep)));
    i = i + 1;
  }
  i = 0;
  while (i < n) {
    send nodes[i], eGo;
    i = i + 1;
  }

  i = 0;
  while (i < partitions) {
    new Arsonist((store = store, kind = 0));
    i = i + 1;
  }
  i = 0;
  while (i < crashes) {
    new Arsonist((store = store, kind = 1));
    i = i + 1;
  }
  i = 0;
  while (i < leaderKills) {
    new Arsonist((store = store, kind = 2));
    i = i + 1;
  }
}

test tcSteadyState [main=SteadyDriver]:
  assert OwnershipConverges in
    { SteadyDriver, Store, Node, RunCourier, HealCourier, Arsonist, StrikeCourier };

test tcCrash [main=CrashDriver]:
  assert OwnershipConverges in
    { CrashDriver, Store, Node, RunCourier, HealCourier, Arsonist, StrikeCourier };

/* EXPECTED VIOLATION — bug ledger (b): the healed owner's D2 re-register tramples the
   peer's row; both run; the one-row table names one owner and nothing ever heals it. */
test tcPartitionHealStock [main=PartitionStockDriver]:
  assert OwnershipConverges in
    { PartitionStockDriver, Store, Node, RunCourier, HealCourier, Arsonist, StrikeCourier };

test tcPartitionHealSweep [main=PartitionSweepDriver]:
  assert OwnershipConverges in
    { PartitionSweepDriver, Store, Node, RunCourier, HealCourier, Arsonist, StrikeCourier };

/* EXPECTED VIOLATION — bug ledger (a): the leader dies with a start in flight; the new
   leader's empty pending ledger re-places the agent on a different node; both copies run;
   the table reads immaculate. This is the deploy-sim duplicate-agent shape. */
test tcLeaderHandoverStock [main=HandoverStockDriver]:
  assert OwnershipConverges in
    { HandoverStockDriver, Store, Node, RunCourier, HealCourier, Arsonist, StrikeCourier };

test tcLeaderHandoverSweep [main=HandoverSweepDriver]:
  assert OwnershipConverges in
    { HandoverSweepDriver, Store, Node, RunCourier, HealCourier, Arsonist, StrikeCourier };

test tcChaosSweep [main=ChaosSweepDriver]:
  assert OwnershipConverges in
    { ChaosSweepDriver, Store, Node, RunCourier, HealCourier, Arsonist, StrikeCourier };
