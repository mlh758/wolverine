# Leader election, model checked

How a Balanced-mode Wolverine cluster elects and keeps exactly one leader
(`NodeAgentController` and its `HeartBeat` partial, `src/Wolverine/Runtime/Agents/`),
checked with [P](https://p-org.github.io/P/). See [`../README.md`](../README.md) for why
these specs exist and how to run them in general.

The election is a pure advisory-lock race. Every node's health-check tick 
calls `TryAttainLeadershipLockAsync`, steps down
when the lock is gone server-side, ejects sustained-stale peers, and resurrects its own
row if a peer deleted it — unless the node may not lead at all (GH-4569:
`Durability.LeadershipEligible = false`, or a warm-up node), in which case it does
everything but contend for the lock. Model last synced to the code at 6.50.2 (`origin/main` 720a6c9c1, 2026-10-15). Because everything routes through the shared database, the whole
protocol is **CP**: a node that can reach the database participates, and a node that
cannot stops leading and stops processing (see [Network partitions](#network-partitions)
below). Leadership changing hands through crashes, deploys, dropped database sessions, and
partitions is exactly the kind of thing distributed systems trip on, so the two properties
worth stating are:

- **Convergence.** Whenever the cluster goes quiet — no tick mid-flight, no fault still
  taking effect — exactly one live *eligible* node believes it is the leader, the advisory
  lock belongs to that node, its row alone carries the `wolverine://leader` assignment,
  every live node has a node row, and no stale or orphaned row survives. With no live
  eligible node left, nobody leads and the lock is free — and a dead leader's row is
  allowed to survive, because the code genuinely cannot destroy it (see below).
- **Quiescence.** The cluster does go quiet. The hot state is what fails if the election
  stalls (a lock wedged under a dead session) or nodes trade leadership forever.

Both live in one monitor (`LeaderElectionSpec.p`), read off the same fact: whether
anything — a tick, a fault, a refill of tick budget — is still carrying work.
Convergence is asserted at every quiescent moment, not only the final one, so a run that
settles, is perturbed, and settles again is checked at both plateaus.

## What is modelled

| Model | Real thing |
| --- | --- |
| `Store` | the `wolverine_nodes` table, the `LeaderUri` assignment row, and the session-scoped advisory lock, serialized the way the database serializes them |
| `Node` | one process: `DoHealthChecksInternalAsync` as a sequence of storage round trips, plus the local `IsLeader` / `_locks` beliefs |
| `SessionReaper` | the database noticing an advisory-lock session died (process exit, network drop, idle cull) — Postgres frees session-level locks the moment the backend session ends |
| `StaleReaper` | a row's heartbeat aging past `StaleNodeTimeout` in the eyes of every observer |
| `eRefill` | the heartbeat/health-check loops running forever: every fault refunds each node a fresh budget of ticks, so quiescence is only reached after the last fault is fully absorbed |

The couriers are load-bearing: a fault's effect lands at a moment of the scheduler's
choosing, so a leader's lock session can die before, during, or long after an election,
and the lock-freed and row-went-stale effects of one crash can land in either order.

Kept faithfully, because the properties turn on them:

- The tick order of `DoHealthChecksInternalAsync`: heartbeat upsert (with the GH-3604/D2
  resurrection of a row a peer deleted, leader mark included), stale-peer ejection, the
  GH-2602 step-down when the lock is gone server-side, then always attain-or-renew.
- The `HasLock` liveness ping (GH-2602): the client's lock belief is re-derived from the
  server, never trusted on its own.
- The idempotent re-grant to the current holder — the client-side short-circuit that
  keeps Postgres session locks from stacking.
- Ejection hysteresis (`StaleNodeEjectionThreshold`, default 2): only a peer stale on
  consecutive observations is ejected, and a streak resets the moment the peer reads
  fresh. Never self (GH-1116 / GH-2682).
- Leader protection: a stale row carrying the leader mark may only be destroyed by the
  node currently holding the lock.
- `DeleteAsync` does not touch the advisory lock — ejecting a dead leader's row does not
  free the lock its dead session still holds.
- `tryStartLeadershipAsync` persists the `LeaderUri` assignment on a pooled connection,
  not the lock session — so a claim can land after the lock has already moved on, and 
  two nodes can transiently both believe they lead. The monitor tolerates the window
  (GH-2602 shrank it; nothing can remove it) and checks the outcome at quiescence.
- The assignment table holds **one** `wolverine://leader/` row (PK = agent uri), so the
  claim is an upsert that *moves* the row to the claimant and takes the mark off the
  previous holder. `stepDownAsync` does not clear it: it stops a local leader agent only
  if one is registered in `Agents`, and nothing ever registers one, so between a step-down
  and the next claim the table still names the ex-leader (`WolverineNode.IsLeader()` reads
  that row). An earlier revision of this model cleared the mark on step-down; the mutant
  below that "leaves the row behind" was therefore testing the model, not the code, and
  has been replaced.
- Graceful `StopAsync`: release the lock only if the node believes it holds it, delete
  own row — plus the process exit closing the lock session, which is the second layer
  that frees the lock when the belief was stale.
- The `CanLead` gate (GH-4569): after the heartbeat, the ejection pass and the local
  sweeps, a node that may not lead returns — no ping, no attain — and if it still holds a
  lease from before its eligibility was revoked, it steps down first. Eligibility can be
  taken away at runtime (`RevokeCourier`), on the leader as often as not.
- A step-down that actually frees the lock (the eligibility case, as opposed to one forced
  by a lock already gone server-side) refills every peer's tick budget: the real loops run
  forever, and the next tick on every peer is an election attempt.

Left out: agent assignment and distribution (`EvaluateAssignmentsAsync`, the
`AssignmentGrid`, capabilities, restrictions) — this model stops at "who leads and what
the node table says", the plane the distribution sits on. Also: the separate heartbeat
loop (GH-3604/D1) — heartbeats ride the tick, and a starved heartbeat is modelled as the
`StaleReaper` blip instead; lease-based lock backends (RavenDb, CosmosDb) with their
renewal/expiry semantics — this is the Postgres/SqlServer session-lock shape; node
number vs node id (one identity here, so GH-1116 and GH-2682 collapse into one
self-check); and storage write failures. One compression: the heartbeat write, the node
state read, and the `HasLock` ping are one round trip here rather than three — all three
happen back-to-back at the top of the real tick, and the model keeps the gap that
matters, the one between reading the state and acting on it.

## When no eligible node is left

`tcIneligibleNode` runs the full chaos load on a cluster where one node may not lead, so
the crash and the stop can take both eligible nodes and leave the ineligible one alone.
The first cut of the model reported a liveness hang there, and the hang is real behaviour:
the survivor sees the dead leader's row go stale, but leader protection lets only the
*lock holder* delete a leader-marked row, and an ineligible node never holds the lock. So
the row — and the dead leader's claim on every inbox and outbox envelope it owned, which
`DeleteAsync` is what releases — survives until an eligible node joins. The docs say "if
every serving node goes away while green is still warming up, nothing gets reassigned";
this is the stronger statement: nothing gets *recovered* either. The spec encodes exactly
that: with no live eligible node, nobody may lead, the lock must be free, every live node
must have a row, no non-leader stale row may survive — and a stale leader-marked row may.
The model stops counting a protected stale leader row as unfinished work for a node that
can never do the work, which is what turned the false liveness bug into a checked
property.

## Network partitions

Everything routes through the shared database, so the database *is* the arbiter — there is
no node-to-node consensus to partition. That makes the behavior CP, and it falls out of
the same mechanics the model already exercises rather than needing a case of its own.

Split the cluster into a side that can reach the database and a side that cannot:

- **The database side** keeps a quorum-of-one: the database. Nodes there go on
  heartbeating, and if the old leader is stranded on the far side, its advisory lock is
  freed the moment its session drops (session-scoped locks die with the connection), so a
  database-side node attains the freed lock and becomes the new leader. From this side, a
  stranded peer is indistinguishable from a crashed one — its heartbeat ages out and its
  row is ejected under the usual hysteresis and leader protection, and its agents are
  redistributed. This is `tcLockSessionDrop` (the lock session dies) and `tcHeartbeatBlip`
  (the row goes stale) happening together, which is exactly what `tcChaos` covers.
- **The stranded side** cannot write its heartbeat or renew its lock. `HasLeadershipLock`
  fails its liveness ping, so a stranded leader steps down (GH-2602), and no node there
  can attain the lock. It elects no one and does no leader/assignment work; its durability
  and subscription agents cannot reach their stores either. It sacrifices availability to
  preserve the single-leader invariant — the CP choice.

So the model's two faults are the two halves of a partition seen from the two sides, and
the safety property — never two leaders believing it at once, at quiescence — is precisely
the guarantee a partition would threaten in an AP design. What the model does **not**
cover is the stranded side's own liveness (it deliberately has no leader while isolated)
or the moment of *healing*, when the stranded node rejoins and its restored agent
assignments collide with what the new leader has already placed. That contention is the
subject of the sibling [`agent-assignment/`](../agent-assignment/) spec, which models the
GH-2602 duplicate healer directly.

## Running it

From the repo root, `nix develop` provides `dotnet` and `p` (see `nix/p.nix`). Then:

```
cd formal/leader-election
p compile --pfiles LeaderElectionModel.p LeaderElectionSpec.p LeaderElectionTest.p --projname LeaderElection --outdir .
p check -tc tcChaos -s 100000
```

The test cases: `tcQuietCluster` (three nodes, nothing goes wrong), `tcCrash` (a
scheduler-chosen node dies dirty — sometimes the leader, sometimes not), `tcGracefulStop`
(the deploy case), `tcLockSessionDrop` (the GH-2602 case: the lock session dies under a
live, healthy leader), `tcHeartbeatBlip` (the GH-3604 case: a live node goes stale in its
peers' eyes and may be ejected), `tcChaos` (all of the above in one run),
`tcIneligibleNode` (GH-4569: chaos with one node that may not lead) and
`tcEligibilityRevoked` (GH-4569: a scheduler-chosen node — the leader, in some schedules —
loses its eligibility while the cluster runs, alongside a crash). A network partition is
`tcLockSessionDrop` and `tcHeartbeatBlip` at once, and `tcChaos` covers that combination —
see [Network partitions](#network-partitions).

`../check-all.sh` runs every case of this spec and the agent-assignment spec, then the
mutant ledger, then bounded PEx budgets, strictly one checker at a time; budget about an
hour. The last two `begin` parameters are the two repairs for bug ledger (c): `reassert`
(the leader re-upserts its row each tick) and `casClaim` (the claim is a compare-and-set
on the leader row's epoch, phase 2 of the stage map); the stock case with both off is the
mutant that shows either is load-bearing.

PEx explores systematically and reports `correct for any depth` when it closes the space —
worth running here, since this state space is small enough that several cases close. The
default random bugfinder is complementary, not inferior (see [`../README.md`](../README.md)
on when each wins); run both. Use `-s`, not `-i`.

```
p compile --pfiles LeaderElectionModel.p LeaderElectionSpec.p LeaderElectionTest.p --projname LeaderElection --outdir . --mode pex
p check --mode pex -tc tcChaos -s 1000000
```

## The bug ledger — a committed counterexample

**(c) `tcLockSessionDrop` / `tcChaos` — a stale claim overwrites the leader row.** Node 1
is granted the lock; its lock session dies before its claim lands; node 2 attains the lock
and claims, so the one `wolverine://leader/` row now names node 2; then node 1's claim
lands — an unconditional upsert on a pooled connection — and moves the row back to node 1.
Node 1 finds out on its next ping and steps down, but step-down does not touch the row,
and nothing else ever writes it until the next election. Checker: `node 2 settled as
leader but its row does not carry the leader mark`, 0.03% of random schedules on
`tcLockSessionDrop` and 0.01% on `tcChaos`. The earlier revision of this model could not
see it, because it cleared the mark on step-down — exactly what the code does not do.

What it costs in the real system: `WolverineNode.IsLeader()` and the grid's `Node.IsLeader`
read that row, so until the next leader change `RunOnLeader` (leader-pinned listeners, the
main store's durability agent on a single-database store) targets the ex-claimant, and
leader protection shields the wrong row. The window — between `TryAttainLeadershipLockAsync`
returning and `AddAssignmentAsync` landing, with the session dying in between — is
milliseconds, but it is the same window GH-2602 lives in.

The model carries two repairs as toggles. `reassert`: a sitting leader re-upserts its row
on every tick its lock is renewed (one idempotent upsert per tick), so the row converges on
the lock holder within one tick — a patch on today's protocol. `casClaim`: the claim is a
compare-and-set on an epoch stamped on the leader row, the design in phase 2 of the stage
map (`.claude/docs/cluster_control_pipeline.md` §11) — a claim carrying an epoch the row
has moved past is refused, the stale claimant releases and stays a follower, and the row
can never be moved back by a late write. `tcLockSessionDropCas` and `tcChaosCas` run the
same faults with the compare-and-set and **no** re-assert; both are clean, so (c) closes on
the claim alone, which is what makes the re-assert unnecessary once phase 2 ships.

## Results

Random bugfinder, 20k schedules per case, on the revision synced to 6.50.2 (`-m 1`):

| Case | Result |
| --- | --- |
| `tcQuietCluster` | no violation |
| `tcCrash` | no violation |
| `tcGracefulStop` | no violation |
| `tcLockSessionDrop` | **VIOLATED (expected)** — bug ledger (c), 0.03% of schedules |
| `tcHeartbeatBlip` | no violation |
| `tcChaos` | **VIOLATED (expected)** — bug ledger (c), 0.01% of schedules |
| `tcIneligibleNode` | no violation |
| `tcEligibilityRevoked` | no violation |
| `tcLockSessionDropReassert` | no violation |
| `tcChaosReassert` | no violation |
| `tcLockSessionDropCas` | no violation |
| `tcChaosCas` | no violation |

The PEx runs recorded for the earlier revision (2M schedules per case; `tcQuietCluster`
and `tcLockSessionDrop` closed as *correct for any depth*, the rest deep bounded searches)
predate the eligibility and step-down changes and have **not** been re-run on this
revision: the full pass (`formal/check-all.sh`, which also runs the mutants and bounded
PEx budgets) was cut short by host memory pressure on 2026-10-15. Treat the table above
as the current evidence, and re-run PEx before claiming a closed space. The earlier
history still holds its lesson: PEx found a real gap in `tcChaos` at ~2k schedules that
100k random schedules had missed (see the last mutant below).

## What the mutants say

A model that passes proves nothing until breaking the design breaks the model. Each of
these is the committed model with one thing removed:

- **The GH-2602 liveness ping** — the client-side short-circuit trusts `_locks` without
  asking the server, so a leader whose lock session died never finds out — **violated**:
  `settled with 2 live nodes believing they are the leader`, found within the first ten
  schedules of `tcLockSessionDrop`. This is the exact two-simultaneous-leaders bug
  GH-2602 fixed, and it needs nothing more exotic than one dropped connection.
- **The GH-3604/D2 resurrection** — a heartbeat miss is silently swallowed instead of
  re-registering the node's real identity — **violated**: `live node 2 has no row at
  quiescence — ejected and never resurrected`, via a heartbeat blip and a peer's
  ejection. Without the resurrection a live node deleted out from under itself is
  permanently invisible to the cluster.
- **The claim does not take the mark off the previous row** (a per-node mark instead of
  the one-row upsert) — **violated**: `node 1's row still carries the leader mark while
  node 2 is the leader`. This is the fidelity point about the schema: because step-down
  leaves the row alone, the claim's upsert is the only thing that ever moves the mark,
  and anything reading leadership off the table (`WolverineNode.IsLeader()`, which leader
  protection itself relies on) depends on it.
- **The database never frees a dead session's lock** — an environment mutant, not a code
  mutant — **violated**: `settled with 0 live nodes believing they are the leader`. This
  is what the election's liveness actually rests on: session-scoped locks dying with
  their session. It is also why `HasLeadershipLock` must ping — the flip side of the
  same coin.
- **Broken quiescence accounting** (a refill swallowed without being counted) —
  **liveness violation**: `detected liveness bug in hot state 'Working' at the end of
  program execution`. Included as the proof that the hot state is not decorative: a run
  that never settles is reported even when no assert ever fires.
- **The `CanLead` gate removed** (an ineligible node contends like any other) — **violated**
  on `tcIneligibleNode` in the first schedule: `every live node is ineligible for
  leadership, yet 1 of them believe they lead`.
- **An ineligible node keeps a lease it already holds** (no step-down on revocation) —
  **violated** on `tcEligibilityRevoked`: `node 1 settled as leader but is not eligible for
  leadership (GH-4569)`.
- **Step-down keeps the advisory lock** — on the earlier revision this was *not falsified*,
  because the only step-downs were forced by a lock already gone. With eligibility it is
  load-bearing: **violated** on `tcEligibilityRevoked`: `settled with 0 live nodes
  believing they are the leader` — the ex-leader has stopped leading but still holds the
  lock, so nobody can be elected. That is the `ReleaseLeadershipLockAsync` call in
  `stepDownAsync` earning its keep.
- **Leader protection removed** — any observer may destroy the leader's row — **not
  falsified** in 30k schedules per case (earlier revision). In this model the wound self-heals: the
  resurrection restores the row and the mark, and the next tick reconciles. **This is
  not a licence to delete the check** — the real damage is on the assignment plane the
  model deliberately leaves out: the ejected leader's agent assignments are wiped with
  its row, and the GH-3604 report is precisely that churn livelock. The model cannot
  see that job at all.
- **Hysteresis removed** (eject on the first stale sighting) — **not falsified**, same
  shape: a premature eject of a live node is healed by resurrection. The threshold
  exists to avoid the eject/resurrect churn and the destroyed assignments, both outside
  this model's scope.

## What PEx caught that random sampling didn't

The mutants above are deliberate wounds. This one was real. An earlier cut of the model
passed the random bugfinder at 100,000 schedules on every case, and PEx then falsified
`tcChaos` at ~2,000 schedules: `a stale row for node 1 survived quiescence`. The defect
was in the model's own bookkeeping, not the protocol — when a late fault refilled a
node's tick budget, `consumeRefill` bumped the budget but did not re-announce the node as
busy before signalling the refill consumed, so for one instant the monitor could read the
cluster as quiescent while a just-marked stale row was still unreconciled. The fix was to
announce the refreshed busy state first (`consumeRefill` in `LeaderElectionModel.p`). It is
the case for keeping PEx in the mix: the bug was a specific interleaving where the crash's
stale-mark was delivered dead last, and random sampling sailed past it a hundred thousand
times. (The converse also happened — random default caught `agent-assignment`'s `tcCrash`
ordering bug a 500k PEx run had not — which is why the guidance is to run both.)

One honest gap: GH-2682 ("never consider self stale on this tick") cannot be exercised
here. The protection guards against a *lagged read* — snapshot isolation or replica lag
showing your own row stale right after you wrote your heartbeat — and this model's
combined write-and-read round trip has no lag by construction. A model that splits the
write from the read and lets the read return stale data would be the way to check it.
