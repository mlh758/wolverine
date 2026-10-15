/*
The cases (see the README). *Stock* is the code BEFORE GH-3987 / GH-4407 (V6.36.0) and
GH-4897 (V6.48.2) shipped: no node-side reconcile sweep, no takeover hold. The two Stock fault
cases are COMMITTED COUNTEREXAMPLES — they are expected to violate the spec, because that
system genuinely settles into a duplicate (see the README's bug ledger). The Sweep variants
run the same faults with the reconcile acting on the first observation (threshold 1, hold
off) and must converge. The Shipped variants are today's defaults: the reconcile with its
three-tick observation threshold, plus a one-evaluation takeover hold. Hold alone runs the
takeover with the hold but no sweep, to ask whether the hold by itself closes the handover
duplicate.

Faults strike via an Arsonist: kinds 0/1 hit the current owner, kind 2 crashes the leader
at the instant it dispatches a start — the rolling deploy terminating the leader pod with a
placement in flight.
*/

machine SteadyDriver {
  start state Init { entry { begin(3, 0, 0, 0, false, 1, 0, false); } }
}

machine CrashDriver {
  start state Init { entry { begin(3, 0, 1, 0, false, 1, 0, false); } }
}

machine PartitionStockDriver {
  start state Init { entry { begin(3, 1, 0, 0, false, 1, 0, false); } }
}

machine PartitionSweepDriver {
  start state Init { entry { begin(3, 1, 0, 0, true, 1, 0, false); } }
}

machine HandoverStockDriver {
  start state Init { entry { begin(3, 0, 0, 1, false, 1, 0, false); } }
}

machine HandoverSweepDriver {
  start state Init { entry { begin(3, 0, 0, 1, true, 1, 0, false); } }
}

machine ChaosSweepDriver {
  start state Init { entry { begin(3, 1, 1, 1, true, 1, 0, false); } }
}

/* The takeover hold on its own: no sweep, one held evaluation. */
machine HandoverHoldDriver {
  start state Init { entry { begin(3, 0, 0, 1, false, 1, 1, false); } }
}

/* Today's defaults: LocalAgentReconciliationThreshold = 3, LeaderTakeoverHoldEvaluations = 1. */
machine SteadyShippedDriver {
  start state Init { entry { begin(3, 0, 0, 0, true, 3, 1, false); } }
}

machine CrashShippedDriver {
  start state Init { entry { begin(3, 0, 1, 0, true, 3, 1, false); } }
}

machine PartitionShippedDriver {
  start state Init { entry { begin(3, 1, 0, 0, true, 3, 1, false); } }
}

machine HandoverShippedDriver {
  start state Init { entry { begin(3, 0, 0, 1, true, 3, 1, false); } }
}

machine ChaosShippedDriver {
  start state Init { entry { begin(3, 1, 1, 1, true, 3, 1, false); } }
}

fun begin(n: int, partitions: int, crashes: int, leaderKills: int, sweep: bool, threshold: int, hold: int, epochMode: bool) {
  var store: machine;
  var nodes: seq[machine];
  var i: int;

  announce eMRoster, (count = n,);

  store = new Store((epochMode = epochMode,));
  i = 0;
  while (i < n) {
    nodes += (i, new Node((store = store, id = i + 1, k = 3, sweep = sweep, threshold = threshold, hold = hold, epochMode = epochMode)));
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
    { SteadyDriver, Store, ConfirmCourier, Node, RunCourier, HealCourier, Arsonist, StrikeCourier };

test tcCrash [main=CrashDriver]:
  assert OwnershipConverges in
    { CrashDriver, Store, ConfirmCourier, Node, RunCourier, HealCourier, Arsonist, StrikeCourier };

/* EXPECTED VIOLATION — bug ledger (b): the healed owner's D2 re-register tramples the
   peer's row; both run; the one-row table names one owner and nothing ever heals it. */
test tcPartitionHealStock [main=PartitionStockDriver]:
  assert OwnershipConverges in
    { PartitionStockDriver, Store, ConfirmCourier, Node, RunCourier, HealCourier, Arsonist, StrikeCourier };

test tcPartitionHealSweep [main=PartitionSweepDriver]:
  assert OwnershipConverges in
    { PartitionSweepDriver, Store, ConfirmCourier, Node, RunCourier, HealCourier, Arsonist, StrikeCourier };

/* EXPECTED VIOLATION — bug ledger (a): the leader dies with a start in flight; the new
   leader's empty pending ledger re-places the agent on a different node; both copies run;
   the table reads immaculate. This is the deploy-sim duplicate-agent shape. */
test tcLeaderHandoverStock [main=HandoverStockDriver]:
  assert OwnershipConverges in
    { HandoverStockDriver, Store, ConfirmCourier, Node, RunCourier, HealCourier, Arsonist, StrikeCourier };

test tcLeaderHandoverSweep [main=HandoverSweepDriver]:
  assert OwnershipConverges in
    { HandoverSweepDriver, Store, ConfirmCourier, Node, RunCourier, HealCourier, Arsonist, StrikeCourier };

test tcChaosSweep [main=ChaosSweepDriver]:
  assert OwnershipConverges in
    { ChaosSweepDriver, Store, ConfirmCourier, Node, RunCourier, HealCourier, Arsonist, StrikeCourier };

/* The GH-4897 hold without the sweep. The hold is a timing heuristic -- one snapshot cycle
   for the predecessor's start to surface -- so a start that takes longer than that still
   produces the handover duplicate. Run it to see whether the checker agrees. */
test tcLeaderHandoverHold [main=HandoverHoldDriver]:
  assert OwnershipConverges in
    { HandoverHoldDriver, Store, ConfirmCourier, Node, RunCourier, HealCourier, Arsonist, StrikeCourier };

/* ---- the shipped defaults: reconcile sweep with a three-tick threshold, takeover hold of one ---- */

test tcSteadyStateShipped [main=SteadyShippedDriver]:
  assert OwnershipConverges in
    { SteadyShippedDriver, Store, ConfirmCourier, Node, RunCourier, HealCourier, Arsonist, StrikeCourier };

test tcCrashShipped [main=CrashShippedDriver]:
  assert OwnershipConverges in
    { CrashShippedDriver, Store, ConfirmCourier, Node, RunCourier, HealCourier, Arsonist, StrikeCourier };

test tcPartitionHealShipped [main=PartitionShippedDriver]:
  assert OwnershipConverges in
    { PartitionShippedDriver, Store, ConfirmCourier, Node, RunCourier, HealCourier, Arsonist, StrikeCourier };

test tcLeaderHandoverShipped [main=HandoverShippedDriver]:
  assert OwnershipConverges in
    { HandoverShippedDriver, Store, ConfirmCourier, Node, RunCourier, HealCourier, Arsonist, StrikeCourier };

test tcChaosShipped [main=ChaosShippedDriver]:
  assert OwnershipConverges in
    { ChaosShippedDriver, Store, ConfirmCourier, Node, RunCourier, HealCourier, Arsonist, StrikeCourier };

/* ---- the proposed design: leader-written rows stamped with a leadership epoch, and a
   node that must win a confirmation on the row before it starts anything. Run against the
   two schedules that are committed counterexamples on stock. ---- */

machine PartitionEpochDriver {
  start state Init { entry { begin(3, 1, 0, 0, true, 1, 0, true); } }
}

machine HandoverEpochDriver {
  start state Init { entry { begin(3, 0, 0, 1, true, 1, 0, true); } }
}

machine ChaosEpochDriver {
  start state Init { entry { begin(3, 1, 1, 1, true, 1, 0, true); } }
}

test tcPartitionHealEpoch [main=PartitionEpochDriver]:
  assert OwnershipConverges in
    { PartitionEpochDriver, Store, Node, RunCourier, ConfirmCourier, HealCourier, Arsonist, StrikeCourier };

test tcLeaderHandoverEpoch [main=HandoverEpochDriver]:
  assert OwnershipConverges in
    { HandoverEpochDriver, Store, Node, RunCourier, ConfirmCourier, HealCourier, Arsonist, StrikeCourier };

test tcChaosEpoch [main=ChaosEpochDriver]:
  assert OwnershipConverges in
    { ChaosEpochDriver, Store, Node, RunCourier, ConfirmCourier, HealCourier, Arsonist, StrikeCourier };
