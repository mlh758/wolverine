/*
The cases (see the README): quiet (no fault), crash (dirty death), stop (graceful
shutdown), drop (lock session dies under a live node, GH-2602), blip (heartbeat goes stale,
GH-3604), chaos (all of the above), ineligible (chaos with one node that may not lead,
GH-4569) and revoked (a node's eligibility is taken away under a running cluster — on the
leader, in some schedules). Faults are scheduler-targeted; each node gets `k` ticks and
every fault refunds them, so quiescence is always reachable after the last fault.
*/

machine QuietDriver {
  start state Init {
    entry {
      begin(3, false, false, 0, 0, 0, 0, false, false);
    }
  }
}

machine CrashDriver {
  start state Init {
    entry {
      begin(3, true, false, 0, 0, 0, 0, false, false);
    }
  }
}

machine StopDriver {
  start state Init {
    entry {
      begin(3, false, true, 0, 0, 0, 0, false, false);
    }
  }
}

machine DropDriver {
  start state Init {
    entry {
      begin(3, false, false, 1, 0, 0, 0, false, false);
    }
  }
}

machine BlipDriver {
  start state Init {
    entry {
      begin(3, false, false, 0, 2, 0, 0, false, false);
    }
  }
}

machine ChaosDriver {
  start state Init {
    entry {
      begin(3, true, true, 1, 1, 0, 0, false, false);
    }
  }
}

/* GH-4569: one of the three nodes may not lead (LeadershipEligible = false, or a warm-up
   node), under the full chaos load. The crash and the stop can take both eligible nodes,
   which is the "no eligible node left" branch of the spec. */
machine IneligibleDriver {
  start state Init {
    entry {
      begin(3, true, true, 1, 1, 1, 0, false, false);
    }
  }
}

/* GH-4569: eligibility is revoked on a scheduler-chosen node while the cluster runs; when
   it is the leader, it must step down, release the lock, and let a peer take over. One
   crash alongside, so the revocation can race a dirty departure. */
machine RevokeDriver {
  start state Init {
    entry {
      begin(3, true, false, 0, 0, 0, 1, false, false);
    }
  }
}

/* Bug ledger (c) with the proposed fix: the leader re-asserts its row every tick. */
machine DropReassertDriver {
  start state Init {
    entry {
      begin(3, false, false, 1, 0, 0, 0, true, false);
    }
  }
}

machine ChaosReassertDriver {
  start state Init {
    entry {
      begin(3, true, true, 1, 1, 0, 0, true, false);
    }
  }
}

/* Phase 2 of the stage map: the leader claim as a compare-and-set on the leader row's
   epoch, with no re-assert. Bug ledger (c) should close on the claim alone. */
machine DropCasDriver {
  start state Init {
    entry {
      begin(3, false, false, 1, 0, 0, 0, false, true);
    }
  }
}

machine ChaosCasDriver {
  start state Init {
    entry {
      begin(3, true, true, 1, 1, 0, 0, false, true);
    }
  }
}

/* Stand up the store and the nodes, start them ticking, then loose the faults. Faults
   ride couriers so their arrival interleaves freely with the ticks — a fault can land
   before the first election, in the middle of one, or long after it settled. */
fun begin(n: int, crashOne: bool, stopOne: bool, drops: int, blips: int, ineligible: int, revokes: int, reassert: bool, casClaim: bool) {
  var store: machine;
  var nodes: seq[machine];
  var i: int;
  var barred: int;

  announce eMRoster, (count = n,);

  /* Which node (by creation index) may not lead; -1 for none. */
  barred = -1;
  if (ineligible > 0) {
    barred = choose(n);
  }

  store = new Store(casClaim);
  i = 0;
  while (i < n) {
    nodes += (i, new Node((store = store, k = 3, threshold = 2, canLead = i != barred, reassert = reassert)));
    i = i + 1;
  }
  i = 0;
  while (i < n) {
    send nodes[i], eStart;
    i = i + 1;
  }

  if (crashOne) {
    new CrashCourier(nodes[choose(n)]);
  }
  if (stopOne) {
    new ShutdownCourier(nodes[choose(n)]);
  }
  i = 0;
  while (i < drops) {
    new SessionReaper((store = store, id = choose(n) + 1, counted = false));
    i = i + 1;
  }
  i = 0;
  while (i < blips) {
    new StaleReaper((store = store, id = choose(n) + 1, counted = false));
    i = i + 1;
  }
  i = 0;
  while (i < revokes) {
    new RevokeCourier(nodes[choose(n)]);
    i = i + 1;
  }
}

test tcQuietCluster [main=QuietDriver]:
  assert ElectionConverges in
    { QuietDriver, Store, Node, SessionReaper, StaleReaper, CrashCourier, ShutdownCourier, RevokeCourier };

test tcCrash [main=CrashDriver]:
  assert ElectionConverges in
    { CrashDriver, Store, Node, SessionReaper, StaleReaper, CrashCourier, ShutdownCourier, RevokeCourier };

test tcGracefulStop [main=StopDriver]:
  assert ElectionConverges in
    { StopDriver, Store, Node, SessionReaper, StaleReaper, CrashCourier, ShutdownCourier, RevokeCourier };

test tcLockSessionDrop [main=DropDriver]:
  assert ElectionConverges in
    { DropDriver, Store, Node, SessionReaper, StaleReaper, CrashCourier, ShutdownCourier, RevokeCourier };

test tcHeartbeatBlip [main=BlipDriver]:
  assert ElectionConverges in
    { BlipDriver, Store, Node, SessionReaper, StaleReaper, CrashCourier, ShutdownCourier, RevokeCourier };

test tcChaos [main=ChaosDriver]:
  assert ElectionConverges in
    { ChaosDriver, Store, Node, SessionReaper, StaleReaper, CrashCourier, ShutdownCourier, RevokeCourier };

test tcIneligibleNode [main=IneligibleDriver]:
  assert ElectionConverges in
    { IneligibleDriver, Store, Node, SessionReaper, StaleReaper, CrashCourier, ShutdownCourier, RevokeCourier };

test tcEligibilityRevoked [main=RevokeDriver]:
  assert ElectionConverges in
    { RevokeDriver, Store, Node, SessionReaper, StaleReaper, CrashCourier, ShutdownCourier, RevokeCourier };

/* ---- the proposed fix for bug ledger (c): a sitting leader re-upserts its row each tick ---- */

test tcLockSessionDropReassert [main=DropReassertDriver]:
  assert ElectionConverges in
    { DropReassertDriver, Store, Node, SessionReaper, StaleReaper, CrashCourier, ShutdownCourier, RevokeCourier };

test tcChaosReassert [main=ChaosReassertDriver]:
  assert ElectionConverges in
    { ChaosReassertDriver, Store, Node, SessionReaper, StaleReaper, CrashCourier, ShutdownCourier, RevokeCourier };

/* ---- phase 2: the compare-and-set claim, no re-assert ---- */

test tcLockSessionDropCas [main=DropCasDriver]:
  assert ElectionConverges in
    { DropCasDriver, Store, Node, SessionReaper, StaleReaper, CrashCourier, ShutdownCourier, RevokeCourier };

test tcChaosCas [main=ChaosCasDriver]:
  assert ElectionConverges in
    { ChaosCasDriver, Store, Node, SessionReaper, StaleReaper, CrashCourier, ShutdownCourier, RevokeCourier };
