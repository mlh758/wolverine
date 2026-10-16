/*
Single-agent ownership on the REAL store schema. See formal/agent-assignment/README.md for
the model, its scope, the bug ledger (committed counterexamples), and the mutant ledger.

Store = the ONE node_assignments row for the agent (PK = agent uri, last-writer-wins
upsert), the live set, and the leader identity, serialized. Because the row names a single
owner, the store can never show two owners at once, and the GH-2602 "two rows -> stop one"
healer is structurally unreachable here — exactly as it is against the RDBMS stores. What
keeps ownership single is the leader's in-memory pending ledger (GH-3698), which dies with
the leader, plus the node-side reconcile sweep that shipped as GH-3987 / GH-4407
(`ReconcileLocalAgentsAsync`, on by default with a three-tick observation threshold), and
the GH-4897 takeover hold a newly elected leader applies to bare first-time placements.
Both are per-test toggles here so the pre-fix behaviour stays checkable as a bug ledger.

Node = one process: its health-check tick and the local fact of running the agent.
Leadership correctness is assumed (proved by the leader-election spec); here it can move.
*/

event eJoin: (node: machine, id: int);   // id assigned at construction, self-reported
event eJoined;
event eGo;

/* A health-check tick's round trip. owner = the row's node_id, 0 = no row. */
event eSync: (node: machine, id: int);
/* orphan = the row was taken away by an ejection (the owner died or was cut off), as
   opposed to never having existed: NodeAgentController.OrphanedByStaleNodes, which the
   takeover hold exempts. Cleared the moment any row lands. */
event eSyncReply: (owner: int, live: map[int, bool], amLeader: bool, confirmed: bool, orphan: bool);

/* A node writes its OWN row after it starts the agent and removes it after it stops.
   eAddRow is AddAssignmentAsync: an unconditional upsert, last writer wins.
   eRemoveRow is RemoveAssignmentAsync: DELETE ... WHERE node_id = :me — conditional, so a
   stale stop can never delete the new owner's row. */
event eAddRow: (id: int);
event eRemoveRow: (id: int);
/* eClaimRow is TryClaimAssignmentAsync (GH-4407): INSERT ... ON CONFLICT DO NOTHING — takes
   the row only when nobody owns it, never over a peer's write. */
event eClaimRow: (id: int);
/* Leader -> store placement decision (AssignAgent), forwarded over a courier. */
event eAssignReq: (id: int);
event eRun: (run: bool, epoch: int, version: int);  // store -> node, over a courier so it interleaves

/* The proposed design. The leader writes the assignment row itself, stamped with the
   leadership epoch, and the node may not start until it WINS a confirmation on that row:
   "mark me confirmed where this agent's row still names me at the epoch I was told". A
   command that outlived its assignment -- an unreachable node waking late, a former leader's
   dispatch after a handover -- loses the compare and never starts. */
event eConfirmRow: (id: int, epoch: int, version: int, node: machine);
event eConfirmReply: (ok: bool, epoch: int, version: int);
/* The leader re-sending the start for a row that is placed but not yet confirmed, at the
   row's CURRENT stamp (idempotent at the node). An unconfirmed row is a debt the leader
   keeps paying until the row confirms or its node leaves; without it a command lost to an
   unreachable node strands the agent (found by tcChaosEpoch, 2026-10-16). */
event eRedrive: (id: int);

/* Faults. */
event eCutoff;                            // partition, node side: unreachable, keeps running
event eEjectMember: (id: int, counted: bool);  // eject row + liveness; counted closes a crash fault
event eReconnect;                         // partition heals
event eCrash;                             // permanent, dirty departure
event eRefill;                            // stand-in for "the health-check loop runs forever"

/* An arsonist ARMS the store; the store fires the strike when the condition occurs, so the
   fault lands where it is interesting (see the README on why a fixed up-front target
   cannot reach the interesting case).
     kind 0: partition the next node to become owner, then heal it
     kind 1: crash the next node to become owner
     kind 2: crash the LEADER at the moment it dispatches a start — the rolling deploy
             terminating the leader pod with a placement in flight (the GH-3987 deploy-sim
             duplicate-agent shape) */
event eArm: (kind: int);
event ePartitionNode: (id: int);
event eCrashNode: (id: int);

/* owner = node_id in the agent's single assignment row (0 = row absent);
   live[id] = store considers id reachable. */
machine Store {
  var owner: int;
  var epochMode: bool;
  /* The leadership epoch: a virtual clock bumped on every handover. */
  var epoch: int;
  /* The epoch the current assignment row was written under, and whether its node has
     checked in. Only meaningful when epochMode is on. */
  var rowEpoch: int;
  /* Bumped on every row write. The epoch identifies a leadership term and cannot separate
     two assignments made inside one -- which is what the checker caught: a node re-asserting
     and then being re-assigned by the same leader produced two check-ins carrying the same
     epoch, and the loser's late answer stopped the winner's agent. Fencing the round trip
     needs identity per assignment, not per term. */
  var rowVersion: int;
  var confirmed: bool;
  var orphan: bool;
  var live: map[int, bool];
  var members: map[int, machine];
  var leader: int;
  var armedPartitions: int;
  var armedCrashes: int;
  var armedLeaderKills: int;

  start state Serving {
    entry (cfg: (epochMode: bool)) {
      epochMode = cfg.epochMode;
      epoch = 1;
    }

    on eArm do (m: (kind: int)) {
      if (m.kind == 0) { armedPartitions = armedPartitions + 1; }
      else if (m.kind == 1) { armedCrashes = armedCrashes + 1; }
      else { armedLeaderKills = armedLeaderKills + 1; }
    }

    on eJoin do (m: (node: machine, id: int)) {
      members[m.id] = m.node;
      live[m.id] = true;
      send m.node, eJoined;
      announceStore(0, 0);
    }

    on eSync do (m: (node: machine, id: int)) {
      var refills: int;
      /* A syncing node is reachable; a heal (was not live) refills the fleet. */
      if (m.id in live && live[m.id]) {
        refills = 0;
      } else {
        live[m.id] = true;
        refills = refillAll();
      }
      /* Leadership handover (assumed correct): if no live leader, the syncer takes it. */
      if (leader == 0 || !(leader in live) || !live[leader]) {
        leader = m.id;
        /* A new leader is a new epoch. Everything the old one dispatched is now stale, and
           a former leader that rejoins writes under an epoch the row has moved past. */
        epoch = epoch + 1;
      }
      send m.node, eSyncReply, (owner = owner, live = live, amLeader = leader == m.id, confirmed = confirmed, orphan = orphan);
      announceStore(0, refills);
    }

    /* AddAssignmentAsync: upsert keyed by the agent, so this OVERWRITES whatever node the
       row named before. Only a live node's write is honored (a partitioned node cannot
       reach the database at all). A durable change refills the fleet so the leader (and
       any sweeping node) re-evaluates. */
    on eAddRow do (m: (id: int)) {
      var refills: int;
      /* Under epochs there is no unconditional upsert: the row belongs to the leader that
         wrote it, and a node cannot take it by writing over the top. This is what stops a
         healed node's re-register trampling the peer that owns the agent now. */
      if (epochMode) {
        announceRowDone(0);
        return;
      }
      if ((m.id in live) && live[m.id] && owner != m.id) {
        owner = m.id;
        orphan = false;
        refills = refillAll();
        fireOwnerFault(m.id);
      }
      announceRowDone(refills);
    }

    /* TryClaimAssignmentAsync (GH-4407): the reconcile sweep restoring a runner's claim.
       Insert-if-absent: a row a peer wrote in the meantime is left to its owner, and the
       sweep's next tick then stops this node's copy instead. */
    on eClaimRow do (m: (id: int)) {
      var refills: int;
      if (epochMode) {
        announceRowDone(0);
        return;
      }
      if ((m.id in live) && live[m.id] && owner == 0) {
        owner = m.id;
        orphan = false;
        refills = refillAll();
        fireOwnerFault(m.id);
      }
      announceRowDone(refills);
    }

    /* RemoveAssignmentAsync: conditional on the caller still being the named owner. */
    on eRemoveRow do (m: (id: int)) {
      var refills: int;
      if (owner == m.id) {
        owner = 0;
        refills = refillAll();
      }
      announceRowDone(refills);
    }

    on eAssignReq do (m: (id: int)) {
      var refills: int;
      if (epochMode) {
        /* The leader writes the assignment itself, before anything is dispatched, so the
           durable row is the arbiter rather than a record written after the fact. A durable
           change refills the fleet, as eAddRow does in stock: a node whose copy this row
           orphans must get the ticks to notice (found by tcPartitionHealEpoch, 2026-10-16). */
        owner = m.id;
        orphan = false;
        rowEpoch = epoch;
        rowVersion = rowVersion + 1;
        confirmed = false;
        refills = refillAll();
      }
      new RunCourier((target = members[m.id], run = true, epoch = epoch, version = rowVersion));
      /* kind 2: the deploy terminates the leader pod with this start still in flight.
         The dead leader takes its pending ledger with it. */
      if (armedLeaderKills > 0 && leader != 0 && (leader in live) && live[leader]) {
        armedLeaderKills = armedLeaderKills - 1;
        new StrikeCourier((store = this, id = leader, kind = 1));
      }
      announceStore(1, refills);
    }

    on eRedrive do (m: (id: int)) {
      if (epochMode && owner == m.id && !confirmed && (m.id in live) && live[m.id]) {
        new RunCourier((target = members[m.id], run = true, epoch = rowEpoch, version = rowVersion));
        announceStore(1, 0);
      } else {
        announceStore(0, 0);
      }
    }

    /* The node checking in against the row it was assigned under. Wins only while the row
       still names it at that same epoch -- so a command that outlived its assignment, or one
       from a leader the cluster has moved past, starts nothing. */
    on eConfirmRow do (m: (id: int, epoch: int, version: int, node: machine)) {
      var ok: bool;
      var refills: int;
      ok = (m.id in live) && live[m.id] && owner == m.id && rowEpoch == m.epoch
           && rowVersion == m.version;
      if (ok) {
        confirmed = true;
        refills = refillAll();
        fireOwnerFault(m.id);
      }
      new ConfirmCourier((target = m.node, ok = ok, epoch = m.epoch, version = m.version));
      announceStore(0, refills);
    }

    /* Partition: cut the owner off (it keeps running) and eject it store-side. Ejection
       deletes the node row, and the assignment row goes with it (FK ON DELETE CASCADE).
       The fault opened here is closed when the node heals or dies. */
    on ePartitionNode do (m: (id: int)) {
      var refills: int;
      /* The strike rides a courier, so its target can be gone (crashed, or already cut
         off) by the time it lands. Partitioning a node that has already left is a no-op —
         and must not open a fault, because a Dead node ignores eCutoff and the fault
         could never close. */
      if (!(m.id in live) || !live[m.id]) {
        return;
      }
      announce eMFault, (delta = 1,);
      send members[m.id], eCutoff;
      live[m.id] = false;
      if (owner == m.id) { owner = 0; orphan = true; }
      refills = refillAll();
      announceStore(0, refills);
    }

    on eCrashNode do (m: (id: int)) {
      send members[m.id], eCrash;
    }

    /* Eject a node's row and liveness. counted closes the crash fault Node.Dead opened. */
    on eEjectMember do (m: (id: int, counted: bool)) {
      var refills: int;
      live[m.id] = false;
      if (owner == m.id) { owner = 0; orphan = true; }
      refills = refillAll();
      announceStore(0, refills);
      if (m.counted) {
        announce eMFault, (delta = -1,);
      }
    }
  }

  /* One armed owner-fault on the node that has just taken the agent (partition before
     crash), fired where ownership actually becomes real in each design. */
  fun fireOwnerFault(id: int) {
    if (armedPartitions > 0) {
      armedPartitions = armedPartitions - 1;
      new StrikeCourier((store = this, id = id, kind = 0));
    } else if (armedCrashes > 0) {
      armedCrashes = armedCrashes - 1;
      new StrikeCourier((store = this, id = id, kind = 1));
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

  fun announceStore(newRuns: int, newRefills: int) {
    announce eMStore, (leader = leader, owner = owner, live = live, newRuns = newRuns, newRefills = newRefills);
  }

  fun announceRowDone(newRefills: int) {
    announce eMStore, (leader = leader, owner = owner, live = live, newRuns = 0, newRefills = newRefills);
    announce eMRowDone;
  }
}

/* Each response/fault rides its own courier so it interleaves with the ticks. */
machine ConfirmCourier {
  start state Deliver {
    entry (m: (target: machine, ok: bool, epoch: int, version: int)) {
      send m.target, eConfirmReply, (ok = m.ok, epoch = m.epoch, version = m.version);
      raise halt;
    }
  }
}

machine RunCourier {
  start state Deliver {
    entry (m: (target: machine, run: bool, epoch: int, version: int)) {
      send m.target, eRun, (run = m.run, epoch = m.epoch, version = m.version);
    }
  }
}

machine HealCourier {
  start state Deliver {
    entry (node: machine) {
      send node, eReconnect;
    }
  }
}

machine Arsonist {
  start state Init {
    entry (cfg: (store: machine, kind: int)) {
      send cfg.store, eArm, (kind = cfg.kind,);
    }
  }
}

machine StrikeCourier {
  start state Deliver {
    entry (m: (store: machine, id: int, kind: int)) {
      if (m.kind == 0) {
        send m.store, ePartitionNode, (id = m.id,);
      } else {
        send m.store, eCrashNode, (id = m.id,);
      }
    }
  }
}

/* localRun is the local fact of running the agent; it survives a partition, because a
   cut-off node cannot be told to stop. The leader's placement logic runs in the tick.

   pendingTarget is the GH-3698 pending-assignment ledger for the one agent: leader-local
   and in-memory, exactly like the real one — so it retires when the row lands, is dropped
   on demotion, and above all DIES WITH THE LEADER. A brand-new leader holding no ledger
   and re-placing an in-flight agent is the committed handover counterexample.

   sweep is the GH-3987 / GH-4407 node-side reconcile (ReconcileLocalAgentsAsync): a
   divergence between my row and what I run, seen on `threshold` consecutive ticks, is
   healed — stop my copy if a live peer owns the row (leaving the row alone), restore my
   claim if nobody does (TryClaimAssignmentAsync, insert-if-absent), start my copy if the
   row names me and nothing runs here.

   hold is the GH-4897 takeover hold (LeaderTakeoverHoldEvaluations): for its first `hold`
   evaluations a new leader does not place an agent that is running nowhere and has no
   start in flight — unless the agent is an orphan of a node that left — so a start its
   predecessor dispatched gets one snapshot cycle to surface as a row. */
machine Node {
  var store: machine;
  var id: int;
  var k: int;
  var sweep: bool;
  var threshold: int;
  var hold: int;
  var epochMode: bool;
  /* _reconcileObservations: consecutive ticks the divergence has been seen. */
  var streak: int;
  /* EvaluationsSinceElection, capped at `hold` so the state stays finite. */
  var evalsSinceElection: int;
  var wasLeader: bool;
  /* The (epoch, version) pair of the attempt this node is on. Together they identify ONE
     assignment: the version alone does not, because a healed node's re-assert can carry
     the current version under the epoch of an older win, lose on the epoch, and then pass
     a version-only fence -- which stopped a healthy agent (found by tcChaosEpoch, 2026-10-16).
     Every check-in carries both, and a reply is matched on both. */
  var myEpoch: int;
  var myVersion: int;
  var budget: int;
  var localRun: bool;
  var pendingTarget: int;
  /* GH-3604/D2: a healed node re-registers and restores its assignment rows, once. */
  var owesReassert: bool;
  /* Owes the monitor a fault-close (paired with the +1 the partition opened); closed on
     heal or death so a partitioned-then-crashed node cannot leak an open fault. */
  var owesFaultClose: bool;

  start state Booting {
    entry (cfg: (store: machine, id: int, k: int, sweep: bool, threshold: int, hold: int, epochMode: bool)) {
      store = cfg.store;
      id = cfg.id;
      k = cfg.k;
      sweep = cfg.sweep;
      threshold = cfg.threshold;
      hold = cfg.hold;
      epochMode = cfg.epochMode;
      budget = cfg.k;
      send store, eJoin, (node = this, id = id);
    }
    defer eGo;
    defer eCutoff;
    defer eCrash;
    on eJoined goto Idle;
  }

  state Idle {
    /* Busy until eGo: full budget, about to start — not quiescent before first placement. */
    entry { announceMe(true); }
    on eGo goto Ticking;
    on eRefill do { consumeRefill(); }
    on eRun do (m: (run: bool, epoch: int, version: int)) { applyRun(m.run, m.epoch, m.version); }
    on eConfirmReply do (m: (ok: bool, epoch: int, version: int)) { applyConfirm(m.ok, m.epoch, m.version); }
    on eCutoff goto Partitioned;
    on eCrash goto Dead;
    /* A sync reply can land after a cutoff+reconnect. */
    ignore eSyncReply;
  }

  state Ticking {
    entry {
      if (budget > 0) {
        announceMe(true);
        budget = budget - 1;
        send store, eSync, (node = this, id = id);
        goto AwaitSync;
      }
      announceMe(false);
    }
    on eRefill do { consumeRefill(); goto Ticking; }
    on eRun do (m: (run: bool, epoch: int, version: int)) { applyRun(m.run, m.epoch, m.version); }
    on eConfirmReply do (m: (ok: bool, epoch: int, version: int)) { applyConfirm(m.ok, m.epoch, m.version); }
    on eCutoff goto Partitioned;
    on eCrash goto Dead;
    ignore eSyncReply;
  }

  state AwaitSync {
    on eSyncReply do (r: (owner: int, live: map[int, bool], amLeader: bool, confirmed: bool, orphan: bool)) {
      var target: int;
      var work: bool;
      var unplaced: bool;
      var mismatch: bool;
      var holding: bool;

      /* GH-3604/D2 re-register after a heal: restore my assignment row for the agent I am
         still running. Faithful detail: this is an unconditional UPSERT, so it tramples
         whatever a peer wrote while I was away — bug ledger entry (b), the partition-heal
         duplicate that the one-row schema can never surface to the leader. */
      if (owesReassert) {
        owesReassert = false;
        if (localRun) {
          announce eMRowPending;
          if (epochMode) {
            /* A healed node re-asserts by asking, not by writing. If the agent has moved on
               it loses, and the sweep below stops the copy it is still running. */
            send store, eConfirmRow, (id = id, epoch = myEpoch, version = myVersion, node = this);
          } else {
            send store, eAddRow, (id = id,);
          }
        }
      }

      /* GH-3987 / GH-4407 node-side reconcile sweep (ReconcileLocalAgentsAsync), as
         shipped. A divergence has to be seen on `threshold` consecutive ticks before it is
         acted on, so anything legitimately in flight -- a start racing its row, a stop
         racing its delete -- clears itself within a tick (LocalAgentReconciliationThreshold,
         default 3). While a divergence is under observation the node keeps ticking.
           running here, row names a live peer  -> stop my copy; the row is the owner's claim
                                                  and is left alone (removeAssignment: false)
           running here, no row anywhere        -> restore my claim, insert-if-absent only
           row names me, nothing runs here      -> start my copy (StartAgentAsync re-upserts) */
      if (sweep) {
        mismatch = false;
        if (localRun && r.owner != id) {
          if (r.owner == 0 || ((r.owner in r.live) && r.live[r.owner])) {
            mismatch = true;
          }
        } else if (!localRun && r.owner == id && !epochMode) {
          /* Under epochs a node never starts on the strength of a row alone; the leader's
             re-drive brings the stamped command, and the check-in decides. */
          mismatch = true;
        }

        if (mismatch) {
          streak = streak + 1;
          if (streak >= threshold) {
            streak = 0;
            if (localRun && (r.owner != 0 || epochMode)) {
              /* Under epochs a node never writes its own row, so "running with no row" is
                 an orphan, not a write in flight: stop, and let the leader re-place. */
              localRun = false;
              announceMe(true);
            } else if (localRun) {
              announce eMRowPending;
              send store, eClaimRow, (id = id,);
              announceMe(true);
            } else {
              localRun = true;
              announce eMRowPending;
              send store, eAddRow, (id = id,);
              announceMe(true);
            }
          } else if (budget < k) {
            /* Under observation but not yet acted on: unfinished work, keep ticking. */
            budget = k;
          }
        } else {
          streak = 0;
        }
      }

      if (r.amLeader) {
        if (!wasLeader) {
          /* tryStartLeadershipAsync: EvaluationsSinceElection = 0 */
          evalsSinceElection = 0;
        }
        wasLeader = true;

        /* Stock: a row of any kind means placed, because the row is only ever written by a
           node that has already started.

           Under epochs the row is written by the LEADER, up front and optimistically, so a
           row is a claim rather than a fact and the leader has to be able to take it back --
           otherwise a node chosen a moment before it became unreachable holds the agent for
           good and nothing runs it. Taking it back is safe precisely because of the stamp:
           re-writing the row is what makes the old node's check-in lose. */
        if (epochMode) {
          unplaced = r.owner == 0 || !(r.owner in r.live) || !r.live[r.owner];
        } else {
          unplaced = r.owner == 0;
        }

        /* GH-4897: a bare first-time placement -- no row, nothing of mine in flight, not the
           orphan of a departed node -- is withheld for the first `hold` evaluations after
           election. The real hold applies only when some other node already holds
           assignments (the predecessor's cluster); with one agent that is assumed here,
           which is the conservative direction for the duplicate property. */
        holding = evalsSinceElection < hold && pendingTarget == 0 && !r.orphan;
        if (evalsSinceElection < hold) {
          evalsSinceElection = evalsSinceElection + 1;
        }

        if (unplaced && holding) {
          /* Held back: nothing dispatched, and the next evaluation must still come. */
          if (budget < k) {
            budget = k;
          }
        } else if (unplaced) {
          /* The pending ledger holds an in-flight start on its chosen node: re-drive the
             SAME node rather than spawning a rival copy (GH-3698). Without a live entry
             the distribution is free to pick anyone — modelled as a nondeterministic
             choice, which is what the real even-spread looks like from one agent's seat. */
          if (pendingTarget != 0 && (pendingTarget in r.live) && r.live[pendingTarget]) {
            target = pendingTarget;
          } else {
            target = chooseLive(r.live);
          }
          if (target != 0) {
            send store, eAssignReq, (id = target,);
            pendingTarget = target;
            work = true;
          }
        } else if (epochMode && !r.confirmed) {
          /* Placed but unconfirmed: re-drive at the same stamp until the node checks in
             or leaves. This is the ledger's job today (dispatch hold + presence polls). */
          send store, eRedrive, (id = r.owner,);
          pendingTarget = r.owner;
          work = true;
        } else {
          /* The row landed (stock) or confirmed (epochs): the ledger entry is retired. */
          pendingTarget = 0;
        }

        /* Keep ticking while there is placement work outstanding. */
        if (work && budget < k) {
          budget = k;
        }
      } else {
        /* The ledger is leader-local. A follower holds none — and a NEW leader therefore
           starts empty, which is the handover hole this model exists to exhibit. */
        pendingTarget = 0;
        wasLeader = false;
      }

      goto Ticking;
    }
    on eRefill do { consumeRefill(); }
    on eRun do (m: (run: bool, epoch: int, version: int)) { applyRun(m.run, m.epoch, m.version); }
    on eConfirmReply do (m: (ok: bool, epoch: int, version: int)) { applyConfirm(m.ok, m.epoch, m.version); }
    on eCutoff goto Partitioned;
    on eCrash goto Dead;
  }

  /* Cut off but still running; row/liveness ejected store-side. The partition is temporary. */
  state Partitioned {
    entry {
      owesFaultClose = true;
      /* Whatever this node was leading or waiting on is gone with its reachability. */
      pendingTarget = 0;
      wasLeader = false;
      streak = 0;
      announcePartitioned();
      new HealCourier(this);
    }
    /* Heal: close the fault, owe the D2 re-register, and take a fresh budget to sync. */
    on eReconnect do {
      closeFaultIfOwed();
      owesReassert = true;
      budget = k;
      goto Ticking;
    }
    /* A command lost to an unreachable node still retires its courier's accounting. */
    on eRun do (m: (run: bool, epoch: int, version: int)) { announce eMRunDone; }
    on eConfirmReply do (m: (ok: bool, epoch: int, version: int)) { announce eMRowDone; }
    on eRefill do { announce eMRefillDone; }
    on eCrash goto Dead;
    ignore eGo;
    ignore eSyncReply;
    ignore eCutoff;
  }

  state Dead {
    entry {
      localRun = false;
      /* Open the crash fault BEFORE announcing the death: announceDead drops the runner
         count to zero, and the monitor must not read that as quiescence before recovery.
         Closed by the store's eject below. */
      announce eMFault, (delta = 1,);
      closeFaultIfOwed();
      announceDead();
      send store, eEjectMember, (id = id, counted = true);
    }
    on eRun do (m: (run: bool, epoch: int, version: int)) { announce eMRunDone; }
    on eConfirmReply do (m: (ok: bool, epoch: int, version: int)) { announce eMRowDone; }
    on eRefill do { announce eMRefillDone; }
    /* A cutoff can race a crash: the store fired the strike while this node's eject was
       still in flight, so its +1 was already announced. Retire it — a dead node cannot
       be partitioned. */
    on eCutoff do { announce eMFault, (delta = -1,); }
    ignore eGo;
    ignore eSyncReply;
    ignore eReconnect;
    ignore eCrash;
  }

  fun applyRun(run: bool, epoch: int, version: int) {
    if (epochMode && run) {
      /* A fence may only ever move forward. Commands ride the same asynchronous path as
         everything else, so an older assignment can arrive after a newer one; adopting its
         version would walk this node's notion of "the attempt I am on" backwards, and the
         stale answer it invites then stops an agent this node legitimately holds. The
         checker found exactly that -- a v3 win followed by a late v2 command, and the v2
         rejection landing against a fence that had regressed to match it. */
      if (version <= myVersion) {
        announce eMRunDone;
        return;
      }
      myVersion = version;
      myEpoch = epoch;
      /* Nothing starts on the strength of the command alone. Check in against the row
         first; only winning that makes this node the owner. */
      announce eMRowPending;
      send store, eConfirmRow, (id = id, epoch = epoch, version = version, node = this);
      announceMe(true);
      announce eMRunDone;
      return;
    }

    localRun = run;
    /* Write my own durable row to match: StartAgentAsync -> upsertAssignmentAsync after
       the start (a re-dispatch to an already-running node is the D4 re-upsert), and
       StopAgentAsync -> RemoveAssignmentAsync (conditional store-side). */
    announce eMRowPending;
    if (run) {
      send store, eAddRow, (id = id,);
    } else {
      send store, eRemoveRow, (id = id,);
    }
    announceMe(budget > 0);
    announce eMRunDone;
  }

  /* The outcome of a check-in. Winning it starts the agent; losing it means the command
     outlived its assignment, and anything already running here is an orphan. */
  fun applyConfirm(ok: bool, epoch: int, version: int) {
    if (version != myVersion || epoch != myEpoch) {
      /* The answer to an attempt this node has already moved past -- or one stamped with a
         stale epoch, which cannot have been this attempt. */
      announce eMRowDone;
      return;
    }
    if (ok) {
      localRun = true;
    } else {
      /* Losing the check-in means this node is an orphan. Reached only for the attempt this
         node is actually on -- see the version guard above, and the monotonicity guard in
         applyRun that keeps that comparison meaningful. */
      localRun = false;
    }
    announceMe(budget > 0);
    announce eMRowDone;
  }

  fun closeFaultIfOwed() {
    if (owesFaultClose) {
      owesFaultClose = false;
      announce eMFault, (delta = -1,);
    }
  }

  fun consumeRefill() {
    if (budget < k) { budget = k; }
    /* Announce busy before signalling the refill consumed (see leader-election). */
    announceMe(true);
    announce eMRefillDone;
  }

  fun chooseLive(live: map[int, bool]): int {
    var i: int;
    var candidates: seq[int];
    foreach (i in keys(live)) {
      if (live[i]) {
        candidates += (sizeof(candidates), i);
      }
    }
    if (sizeof(candidates) == 0) {
      return 0;
    }
    return choose(candidates);
  }

  fun announceMe(busy: bool) {
    announce eMNode, (id = id, localRun = localRun, partitioned = false, alive = true, busy = busy);
  }

  fun announcePartitioned() {
    announce eMNode, (id = id, localRun = localRun, partitioned = true, alive = true, busy = true);
  }

  fun announceDead() {
    announce eMNode, (id = id, localRun = false, partitioned = false, alive = false, busy = false);
  }
}
