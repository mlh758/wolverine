/*
Single-agent ownership on the REAL store schema. See formal/agent-assignment/README.md for
the model, its scope, the bug ledger (committed counterexamples), and the mutant ledger.

Store = the ONE node_assignments row for the agent (PK = agent uri, last-writer-wins
upsert), the live set, and the leader identity, serialized. Because the row names a single
owner, the store can never show two owners at once, and the GH-2602 "two rows -> stop one"
healer is structurally unreachable here — exactly as it is against the RDBMS stores. What
keeps ownership single is the leader's in-memory pending ledger (GH-3698), which dies with
the leader, plus (when enabled) the node-side reconcile sweep proposed in GH-4297.

Node = one process: its health-check tick and the local fact of running the agent.
Leadership correctness is assumed (proved by the leader-election spec); here it can move.
*/

event eJoin: (node: machine, id: int);   // id assigned at construction, self-reported
event eJoined;
event eGo;

/* A health-check tick's round trip. owner = the row's node_id, 0 = no row. */
event eSync: (node: machine, id: int);
event eSyncReply: (owner: int, live: map[int, bool], amLeader: bool);

/* A node writes its OWN row after it starts the agent and removes it after it stops.
   eAddRow is AddAssignmentAsync: an unconditional upsert, last writer wins.
   eRemoveRow is RemoveAssignmentAsync: DELETE ... WHERE node_id = :me — conditional, so a
   stale stop can never delete the new owner's row. */
event eAddRow: (id: int);
event eRemoveRow: (id: int);
/* Leader -> store placement decision (AssignAgent), forwarded over a courier. */
event eAssignReq: (id: int);
event eRun: (run: bool);                  // store -> node, over a courier so it interleaves

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
  var live: map[int, bool];
  var members: map[int, machine];
  var leader: int;
  var armedPartitions: int;
  var armedCrashes: int;
  var armedLeaderKills: int;

  start state Serving {
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
      }
      send m.node, eSyncReply, (owner = owner, live = live, amLeader = leader == m.id);
      announceStore(0, refills);
    }

    /* AddAssignmentAsync: upsert keyed by the agent, so this OVERWRITES whatever node the
       row named before. Only a live node's write is honored (a partitioned node cannot
       reach the database at all). A durable change refills the fleet so the leader (and
       any sweeping node) re-evaluates. */
    on eAddRow do (m: (id: int)) {
      var refills: int;
      if ((m.id in live) && live[m.id] && owner != m.id) {
        owner = m.id;
        refills = refillAll();
        /* Fire one armed owner-fault on the new owner (partition before crash). */
        if (armedPartitions > 0) {
          armedPartitions = armedPartitions - 1;
          new StrikeCourier((store = this, id = m.id, kind = 0));
        } else if (armedCrashes > 0) {
          armedCrashes = armedCrashes - 1;
          new StrikeCourier((store = this, id = m.id, kind = 1));
        }
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
      new RunCourier((target = members[m.id], run = true));
      /* kind 2: the deploy terminates the leader pod with this start still in flight.
         The dead leader takes its pending ledger with it. */
      if (armedLeaderKills > 0 && leader != 0 && (leader in live) && live[leader]) {
        armedLeaderKills = armedLeaderKills - 1;
        new StrikeCourier((store = this, id = leader, kind = 1));
      }
      announceStore(1, 0);
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
      if (owner == m.id) { owner = 0; }
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
      if (owner == m.id) { owner = 0; }
      refills = refillAll();
      announceStore(0, refills);
      if (m.counted) {
        announce eMFault, (delta = -1,);
      }
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
machine RunCourier {
  start state Deliver {
    entry (m: (target: machine, run: bool)) {
      send m.target, eRun, (run = m.run,);
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

   sweep is the GH-4297 node-side reconcile: on every tick, stop my copy if the row names
   another live node, and start my copy if the row names me and nothing is running. */
machine Node {
  var store: machine;
  var id: int;
  var k: int;
  var sweep: bool;
  var budget: int;
  var localRun: bool;
  var pendingTarget: int;
  /* GH-3604/D2: a healed node re-registers and restores its assignment rows, once. */
  var owesReassert: bool;
  /* Owes the monitor a fault-close (paired with the +1 the partition opened); closed on
     heal or death so a partitioned-then-crashed node cannot leak an open fault. */
  var owesFaultClose: bool;

  start state Booting {
    entry (cfg: (store: machine, id: int, k: int, sweep: bool)) {
      store = cfg.store;
      id = cfg.id;
      k = cfg.k;
      sweep = cfg.sweep;
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
    on eRun do (m: (run: bool)) { applyRun(m.run); }
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
    on eRun do (m: (run: bool)) { applyRun(m.run); }
    on eCutoff goto Partitioned;
    on eCrash goto Dead;
    ignore eSyncReply;
  }

  state AwaitSync {
    on eSyncReply do (r: (owner: int, live: map[int, bool], amLeader: bool)) {
      var target: int;
      var work: bool;

      /* GH-3604/D2 re-register after a heal: restore my assignment row for the agent I am
         still running. Faithful detail: this is an unconditional UPSERT, so it tramples
         whatever a peer wrote while I was away — bug ledger entry (b), the partition-heal
         duplicate that the one-row schema can never surface to the leader. */
      if (owesReassert) {
        owesReassert = false;
        if (localRun) {
          announce eMRowPending;
          send store, eAddRow, (id = id,);
        }
      }

      /* GH-4297 node-side reconcile sweep. Stop side: my copy is an orphan the moment the
         row names another live node (never on owner == 0 — my own row write may still be
         in flight, and stopping then reintroduces the lag the sweep exists to avoid
         trading for duplicates). Start side: the row names me but nothing runs here —
         which also heals the row-without-runner state the stop side can leave behind when
         it races this node's own start. */
      if (sweep) {
        if (localRun && r.owner != id && r.owner != 0 && (r.owner in r.live) && r.live[r.owner]) {
          localRun = false;
          announceMe(true);
        } else if (!localRun && r.owner == id) {
          localRun = true;
          announce eMRowPending;
          send store, eAddRow, (id = id,);
          announceMe(true);
        }
      }

      if (r.amLeader) {
        if (r.owner == 0) {
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
        } else {
          /* The row landed: the ledger entry is retired (reconcilePendingAssignments). */
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
      }

      goto Ticking;
    }
    on eRefill do { consumeRefill(); }
    on eRun do (m: (run: bool)) { applyRun(m.run); }
    on eCutoff goto Partitioned;
    on eCrash goto Dead;
  }

  /* Cut off but still running; row/liveness ejected store-side. The partition is temporary. */
  state Partitioned {
    entry {
      owesFaultClose = true;
      /* Whatever this node was leading or waiting on is gone with its reachability. */
      pendingTarget = 0;
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
    on eRun do (m: (run: bool)) { announce eMRunDone; }
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
    on eRun do (m: (run: bool)) { announce eMRunDone; }
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

  fun applyRun(run: bool) {
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
