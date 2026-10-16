/*
Wolverine's leader election, as NodeAgentController runs it on main.
See formal/leader-election/README.md for what is abstracted away and why.

Store = the wolverine_nodes table, the LeaderUri assignment row, and the session-scoped
        advisory lock, as one machine — so a P execution serializes storage operations
        exactly the way the database does.
Node  = one Wolverine process: DoHealthChecksInternalAsync as a sequence of storage
        round trips, plus the local IsLeader / lock beliefs those round trips feed.
*/

/* A persisted node row: heartbeat freshness plus whether the row carries the LeaderUri
   assignment (what WolverineNode.IsLeader() reads). */
type tRow = (stale: bool, leaderMark: bool);

/* Bootstrapping: PersistAsync hands out sequential node numbers. */
event eRegisterNode: machine;
event eNodeNumber: (id: int);
event eStart;

/* One health-check tick's round trips, in DoHealthChecksInternalAsync order. */
event eHeartbeat: (node: machine, id: int, isLeader: bool);
/* leaderEpoch = the epoch stamped on the `wolverine://leader` row (phase 2 of the stage
   map, .claude/docs/cluster_control_pipeline.md §11): bumped by every successful claim. */
event eSnapshot: (rows: map[int, tRow], youHoldLock: bool, leaderEpoch: int);
event eEject: (id: int);
event eStepDown: (node: machine, id: int);
event eTryAttain: (node: machine, id: int);
event eLockResult: (granted: bool);
/* epoch = the leader epoch the claimant read in its snapshot; renew = a sitting leader
   re-asserting its row (never bumps). Under the compare-and-set claim the store grants the
   claim only if the row's epoch is still the one the claimant read. */
event eClaim: (node: machine, id: int, epoch: int, renew: bool);
event eClaimAck: (ok: bool);

/* Graceful shutdown: NodeAgentController.StopAsync. */
event eShutdown;
event eReleaseAndDelete: (node: machine, id: int, hadLock: bool);
event eStopAck;

/* Faults. eSessionDied = the database noticing a dead advisory-lock session; eMarkStale =
   a row's heartbeat aging past StaleNodeTimeout. `counted` = the fault was pre-announced for
   the monitor's quiescence accounting (see the README). */
event eCrash;
event eSessionDied: (id: int, counted: bool);
event eMarkStale: (id: int, counted: bool);
/* Stand-in for "the health-check loop runs forever": every fault refills the surviving
   nodes' tick budgets, so quiescence is reached only after the last fault is absorbed. */
event eRefill;
/* GH-4569: the operator takes a node's leadership eligibility away under a live lease
   (Durability.LeadershipEligible = false, or the node is restarted as a warm-up node). */
event eRevokeEligibility;

/*
The database. One machine: arrival order is commit order. The advisory lock is
session-level — eEject (DeleteAsync) deliberately does not touch it, because deleting a
dead leader's row does not free the lock its dead session still holds; only eSessionDied
does that.
*/
machine Store {
  var rows: map[int, tRow];
  var members: map[int, machine];
  /* 0 = free. Node ids start at 1. */
  var lockHolder: int;
  var nextId: int;
  /* Phase 2: the claim is a compare-and-set on the leader row's epoch. Off = today's
     unconditional upsert. */
  var casClaim: bool;
  var leaderEpoch: int;

  start state Serving {
    entry (cas: bool) {
      nextId = 1;
      casClaim = cas;
    }

    on eRegisterNode do (node: machine) {
      members[nextId] = node;
      rows[nextId] = (stale = false, leaderMark = false);
      send node, eNodeNumber, (id = nextId,);
      nextId = nextId + 1;
      announceStore(0, 0);
    }

    on eHeartbeat do (m: (node: machine, id: int, isLeader: bool)) {
      /* MarkHealthCheckAsync refreshes an existing row; a miss means a peer ejected a
         still-live node, so ReregisterNodeAsync resurrects it with its real identity —
         leader mark included (GH-3604/D2). The reply folds in the node-state read and the
         HasLeadershipLock ping (GH-2602); see the README on that compression. */
      if (m.id in rows) {
        rows[m.id] = (stale = false, leaderMark = rows[m.id].leaderMark);
      } else {
        rows[m.id] = (stale = false, leaderMark = m.isLeader);
      }
      send m.node, eSnapshot, (rows = rows, youHoldLock = lockHolder == m.id, leaderEpoch = leaderEpoch);
      announceStore(0, 0);
    }

    on eEject do (m: (id: int)) {
      /* DeleteAsync: the row and its assignment rows go, the advisory lock does not. */
      if (m.id in rows) {
        rows -= (m.id);
        /* The victim's own loops keep running no matter what peers did to its row; the
           refill is the model's stand-in for those future ticks, and is what lets a
           still-live victim resurrect itself. */
        send members[m.id], eRefill;
        announceStore(0, 1);
      }
    }

    on eStepDown do (m: (node: machine, id: int)) {
      var refills: int;
      /* stepDownAsync: best-effort release the advisory lock. It also tries to stop a local
         `wolverine://leader` agent, but nothing ever registers one (the leader agent exists
         only as the assignment row), so the LeaderUri row is NOT touched here: it keeps
         naming the ex-leader until the next claim's upsert moves it (see eClaim). When the
         release actually frees the lock -- a step-down for ineligibility, not one forced by
         a lock already gone server-side -- every peer's next tick is an election attempt,
         so they get ticks: the health-check loops run forever. */
      if (lockHolder == m.id) {
        lockHolder = 0;
        refills = refillAll();
      }
      announceStore(0, refills);
    }

    on eTryAttain do (m: (node: machine, id: int)) {
      /* A session-level advisory lock: granted when free, and idempotently re-granted to
         the current holder — the client-side short-circuit in
         AdvisoryLock.TryAttainLockAsync that keeps the lock from stacking. */
      if (lockHolder == 0 || lockHolder == m.id) {
        lockHolder = m.id;
        send m.node, eLockResult, (granted = true,);
      } else {
        send m.node, eLockResult, (granted = false,);
      }
      announceStore(0, 0);
    }

    on eClaim do (m: (node: machine, id: int, epoch: int, renew: bool)) {
      var v: int;
      /* tryStartLeadershipAsync: ensureLocalNodeRegisteredAsync + AddAssignmentAsync of
         LeaderUri. The assignment table has ONE row per agent (PK = agent uri, upsert), so
         the claim moves the `wolverine://leader` row to the claimant and thereby takes the
         mark off whoever held it -- that upsert, not the step-down, is what clears an
         ex-leader's mark. Runs on a pooled connection, not the lock session — so a claim
         CAN land after the lock has already moved on. Today (casClaim off) it lands anyway
         and overwrites the real leader's row: bug ledger (c). With the compare-and-set a
         claim carrying an epoch the row has moved past is refused, and the stale claimant
         learns it was late from the refusal rather than from its next ping. */
      if (m.renew) {
        /* A sitting leader re-asserting: idempotent, never bumps the epoch. */
        if ((m.id in rows) && rows[m.id].leaderMark) {
          send m.node, eClaimAck, (ok = true,);
        } else {
          send m.node, eClaimAck, (ok = false,);
        }
        announceStore(0, 0);
        return;
      }
      if (casClaim && m.epoch != leaderEpoch) {
        send m.node, eClaimAck, (ok = false,);
        announceStore(0, 0);
        return;
      }
      leaderEpoch = leaderEpoch + 1;
      foreach (v in keys(rows)) {
        rows[v] = (stale = rows[v].stale, leaderMark = v == m.id);
      }
      if (!(m.id in rows)) {
        rows[m.id] = (stale = false, leaderMark = true);
      }
      send m.node, eClaimAck, (ok = true,);
      announceStore(0, 0);
    }

    on eReleaseAndDelete do (m: (node: machine, id: int, hadLock: bool)) {
      /* StopAsync: release the lock if the node believes it holds it, then delete the
         node's own row. */
      if (m.hadLock && lockHolder == m.id) {
        lockHolder = 0;
      }
      if (m.id in rows) {
        rows -= (m.id);
      }
      send m.node, eStopAck;
      announceStore(0, 0);
    }

    on eSessionDied do (m: (id: int, counted: bool)) {
      var faultsDone: int;
      if (lockHolder == m.id) {
        lockHolder = 0;
      }
      if (m.counted) {
        faultsDone = 1;
      }
      announceStore(faultsDone, refillAll());
    }

    on eMarkStale do (m: (id: int, counted: bool)) {
      var faultsDone: int;
      if (m.id in rows) {
        rows[m.id] = (stale = true, leaderMark = rows[m.id].leaderMark);
      }
      if (m.counted) {
        faultsDone = 1;
      }
      announceStore(faultsDone, refillAll());
    }
  }

  fun refillAll(): int {
    var i: int;
    foreach (i in keys(members)) {
      send members[i], eRefill;
    }
    return sizeof(members);
  }

  fun announceStore(faultsDone: int, refills: int) {
    announce eMStoreState,
      (lockHolder = lockHolder, rows = rows, newFaultsDone = faultsDone, newRefills = refills);
  }
}

/* Couriers: a fault's effect lands at a scheduler-chosen moment, so the lock-freed and
   row-stale halves of one crash can arrive in any order and at any time. */
machine SessionReaper {
  start state Deliver {
    entry (m: (store: machine, id: int, counted: bool)) {
      send m.store, eSessionDied, (id = m.id, counted = m.counted);
    }
  }
}

machine StaleReaper {
  start state Deliver {
    entry (m: (store: machine, id: int, counted: bool)) {
      send m.store, eMarkStale, (id = m.id, counted = m.counted);
    }
  }
}

machine CrashCourier {
  start state Deliver {
    entry (target: machine) {
      send target, eCrash;
    }
  }
}

machine ShutdownCourier {
  start state Deliver {
    entry (target: machine) {
      send target, eShutdown;
    }
  }
}

machine RevokeCourier {
  start state Deliver {
    entry (target: machine) {
      send target, eRevokeEligibility;
    }
  }
}

/*
One Wolverine process. A tick is DoHealthChecksInternalAsync: write own heartbeat and
read the cluster (one round trip here), eject sustained-stale peers, step down if the
lock is gone, then always try to attain-or-renew the lock. The node blocks on each
round trip the way a synchronous database client does, so a node has at most one
outstanding request — but ticks from different nodes interleave freely at the store.
*/
machine Node {
  var store: machine;
  var id: int;
  /* Ticks granted at start and by every refill wave. */
  var k: int;
  var budget: int;
  var isLeader: bool;
  /* The client-side _locks list: belief, refreshed by the snapshot's folded-in ping. */
  var hasLock: bool;
  /* _staleObservations: consecutive stale sightings per peer (GH-3604 hysteresis). */
  var staleCounts: map[int, int];
  var threshold: int;
  /* DurabilitySettings.CanLead: LeadershipEligible, and never on a warm-up node (GH-4569).
     An ineligible node heartbeats, ejects and reconciles like any other, and never
     contends for the lock. */
  var canLead: bool;
  /* PROPOSED FIX, off by default (the code does not do this): a sitting leader re-upserts
     its `wolverine://leader` row on every tick its lock is renewed, so a stale claim that
     overwrote the row (bug ledger (c) in the README) is repaired on the next tick. */
  var reassert: bool;
  /* The leader epoch read in this tick's snapshot, carried on the claim. */
  var seenEpoch: int;

  start state Booting {
    entry (cfg: (store: machine, k: int, threshold: int, canLead: bool, reassert: bool)) {
      store = cfg.store;
      k = cfg.k;
      threshold = cfg.threshold;
      canLead = cfg.canLead;
      reassert = cfg.reassert;
      budget = cfg.k;
      send store, eRegisterNode, this;
    }
    /* A fault cannot take effect before the row it acts on exists. */
    defer eStart;
    defer eCrash;
    defer eShutdown;
    defer eRevokeEligibility;
    on eNodeNumber do (m: (id: int)) {
      id = m.id;
      goto Waiting;
    }
  }

  state Waiting {
    entry {
      announceMe(false);
    }
    on eStart goto Ticking;
    on eRefill do { consumeRefill(); }
    on eRevokeEligibility do { revoke(); }
    on eCrash goto Dead;
    on eShutdown goto Stopping;
  }

  state Ticking {
    entry {
      announceMe(false);
      if (budget > 0) {
        budget = budget - 1;
        send store, eHeartbeat, (node = this, id = id, isLeader = isLeader);
        goto AwaitSnapshot;
      }
    }
    on eRefill do {
      consumeRefill();
      goto Ticking;
    }
    on eRevokeEligibility do {
      revoke();
      goto Ticking;
    }
    on eCrash goto Dead;
    on eShutdown goto Stopping;
  }

  state AwaitSnapshot {
    on eSnapshot do (m: (rows: map[int, tRow], youHoldLock: bool, leaderEpoch: int)) {
      var v: int;
      var newCounts: map[int, int];
      var doEject: bool;
      var pendingEject: bool;

      hasLock = m.youHoldLock;
      seenEpoch = m.leaderEpoch;

      /* ejectStaleNodes: never self; hysteresis (eject only after `threshold` consecutive
         stale sightings, streak resets on a fresh read, GH-3604/D1); leader protection (a
         stale leader row may be destroyed only by the current lock holder). */
      foreach (v in keys(m.rows)) {
        if (v != id && m.rows[v].stale) {
          if (v in staleCounts) {
            newCounts[v] = staleCounts[v] + 1;
          } else {
            newCounts[v] = 1;
          }
          if (newCounts[v] >= threshold) {
            doEject = true;
            if (m.rows[v].leaderMark && !hasLock) {
              doEject = false;
            }
            if (doEject) {
              send store, eEject, (id = v,);
              newCounts -= (v);
            }
          }
          /* A stale leader row is work only for a node that could ever hold the lock. An
             ineligible node will watch it forever (GH-4569 — see the README), so counting
             it would keep that node ticking forever: a false liveness bug on real behaviour. */
          if ((v in newCounts) && !(m.rows[v].leaderMark && !canLead)) {
            pendingEject = true;
          }
        }
      }
      staleCounts = newCounts;

      /* A stale peer seen but not yet ejected is unfinished work — keep ticking until the
         cluster is clean, as the real loop would. See the README (PEx found this gap). */
      if (pendingEject && budget < k) {
        budget = k;
      }

      /* GH-4569: a node that may not lead does everything above -- heartbeat, ejection,
         the local sweeps -- and nothing below. It never pings or contends for the lock,
         and if its eligibility was taken away under a lease it holds, it lets go. */
      if (!canLead) {
        if (isLeader) {
          stepDownLocally();
        }
        goto Ticking;
      }

      /* GH-2602: we thought we were the leader, but the advisory lock is gone
         server-side. Step down cleanly, then fall through to the normal election
         attempt on this same tick. */
      if (isLeader && !hasLock) {
        stepDownLocally();
      }

      /* Always: an election attempt for a follower, a renewal for the leader. */
      send store, eTryAttain, (node = this, id = id);
      goto AwaitLock;
    }
    on eRefill do { consumeRefill(); }
    on eRevokeEligibility do { revoke(); }
    on eCrash goto Dead;
    on eShutdown goto Stopping;
  }

  state AwaitLock {
    on eLockResult do (m: (granted: bool)) {
      if (m.granted) {
        hasLock = true;
        if (!isLeader) {
          /* tryStartLeadershipAsync: persist the LeaderUri assignment, and only then
             act as the leader. */
          send store, eClaim, (node = this, id = id, epoch = seenEpoch, renew = false);
          goto AwaitClaim;
        }
        /* Already leader: the grant was a renewal; EvaluateAssignmentsAsync is out of
           this model's scope. With the proposed fix, re-assert the leader row too. */
        if (reassert) {
          send store, eClaim, (node = this, id = id, epoch = seenEpoch, renew = true);
          goto AwaitClaim;
        }
        goto Ticking;
      }
      hasLock = false;
      if (isLeader) {
        /* The lock could not be renewed — someone else holds it. */
        stepDownLocally();
      }
      goto Ticking;
    }
    on eRefill do { consumeRefill(); }
    on eRevokeEligibility do { revoke(); }
    on eCrash goto Dead;
    on eShutdown goto Stopping;
  }

  state AwaitClaim {
    on eClaimAck do (m: (ok: bool)) {
      if (m.ok) {
        isLeader = true;
      } else if (!isLeader) {
        /* The claim was refused: another node claimed since this one read the epoch, so
           this grant was already stale. Let go of whatever the lock session still holds
           (tryStartLeadershipAsync releases on a failed claim) and stay a follower. */
        hasLock = false;
        send store, eStepDown, (node = this, id = id);
      }
      announceMe(true);
      goto Ticking;
    }
    on eRefill do { consumeRefill(); }
    on eRevokeEligibility do { revoke(); }
    on eCrash goto Dead;
    on eShutdown goto Stopping;
  }

  /* NodeAgentController.StopAsync: release the lock if believed held, delete own row. The
     process exit also closes the lock session (the reaper), freeing it even if the belief
     was stale. */
  state Stopping {
    entry {
      announce eMNodeState,
        (id = id, alive = true, isLeader = isLeader, busy = true, newFaults = 1, eligible = canLead);
      new SessionReaper((store = store, id = id, counted = true));
      send store, eReleaseAndDelete, (node = this, id = id, hadLock = hasLock);
    }
    on eStopAck goto Stopped;
    on eRefill do { announce eMRefillDone; }
    on eCrash do { }
    ignore eStart;
    ignore eRevokeEligibility;
    ignore eSnapshot;
    ignore eLockResult;
    ignore eClaimAck;
  }

  state Stopped {
    entry {
      announceDead(0);
    }
    on eRefill do { announce eMRefillDone; }
    ignore eStart;
    ignore eRevokeEligibility;
    ignore eCrash;
    ignore eShutdown;
    ignore eSnapshot;
    ignore eLockResult;
    ignore eClaimAck;
    ignore eStopAck;
  }

  /* A dirty crash — SIGKILL, OOM, a pulled node. No cleanup runs. The database frees the
     session's advisory lock and the row's heartbeat goes stale, each at a moment of the
     scheduler's choosing, and in either order. */
  state Dead {
    entry {
      announceDead(2);
      new SessionReaper((store = store, id = id, counted = true));
      new StaleReaper((store = store, id = id, counted = true));
    }
    on eRefill do { announce eMRefillDone; }
    ignore eStart;
    ignore eRevokeEligibility;
    ignore eCrash;
    ignore eShutdown;
    ignore eSnapshot;
    ignore eLockResult;
    ignore eClaimAck;
    ignore eStopAck;
  }

  /* The operator flipped Durability.LeadershipEligible under a running node. The next tick
     is where the controller notices (the CanLead gate), so the node owes one more tick. */
  fun revoke() {
    canLead = false;
    if (budget < k) {
      budget = k;
    }
    announceMe(true);
  }

  fun stepDownLocally() {
    isLeader = false;
    hasLock = false;
    send store, eStepDown, (node = this, id = id);
    announceMe(true);
  }

  fun consumeRefill() {
    if (budget < k) {
      budget = k;
    }
    /* Announce busy BEFORE signalling the refill consumed: eMRefillDone can drop the
       monitor's pendingRefills to zero, and busy=true must land first or the monitor reads
       a false quiescence in between. See the README (PEx found this in tcChaos). */
    announceMe(true);
    announce eMRefillDone;
  }

  fun announceMe(midTick: bool) {
    announce eMNodeState,
      (id = id, alive = true, isLeader = isLeader, busy = budget > 0 || midTick, newFaults = 0,
       eligible = canLead);
  }

  fun announceDead(faults: int) {
    announce eMNodeState,
      (id = id, alive = false, isLeader = false, busy = false, newFaults = faults,
       eligible = canLead);
  }
}
