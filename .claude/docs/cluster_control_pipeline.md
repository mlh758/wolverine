# The cluster control tick as a staged pipeline, and feedback on its outputs

Status: **proposal**, written against `origin/main` at 6.50.2 (2026-10-15). Nothing here is
implemented. It is the refactoring target for `NodeAgentController` and its partials, and
the place to settle the shape before touching code. The mechanism as it exists is described
in `docs/guide/durability/leadership-and-agent-distribution.md`; the invariants quoted below
are the ones the P models under `formal/` check, with GH numbers where a comment in the code
records the incident that made the rule load-bearing.

## 1. What is intertwined today, precisely

`NodeAgentController.DoHealthChecksInternalAsync` is already a fixed, linear sequence. The
coupling is not in the control flow, it is in **state**: one class holds two dozen fields,
each annotated "only touched from the serialized health-check path" or guarded by
`_healthCheckGuard` / `_pendingLock`, and three of them have a second writer on another
thread:

| State | Written by | Also written or read by |
| --- | --- | --- |
| `_pendingAssignments` (the ledger) | evaluation (`reconcilePendingAssignments`) | dispatcher lane threads (`ConfirmDispatched`); read through `PendingDispatches` probe |
| `Agents`, `_startingAgents`, `_stoppingAgents`, `_stopRevocations`, `_commandSequence` | remote command handlers (`StartAgentAsync` / `stopAgentAsync` via `DeferredAgentCommandRunner`) | the reconcile sweep and the failed-agent sweep on the tick |
| `_releasedAgents` (embargo) | the failed-agent sweep | the independent heartbeat loop (`buildLocalNode` on a resurrection) |

Everything else (`_staleObservations`, `_reconcileObservations`, `_knownNodes` and the
settle timestamps, `EvaluationsSinceElection`, `OrphanedByStaleNodes`, `_rowsLastTick`,
`_transientRestarts`, `_failedStarts`, `IsLeader`) is single-writer tick state that merely
lives in the wrong place. The tick itself is the easy part.

## 2. Target shape

```
 tickers                 pipeline (one TickContext at a time, stages in fixed order)                 sinks
 ───────                 ─────────────────────────────────────────────────────────                 ─────
 heartbeat ──► [Heartbeat] (own ticker, nothing else; GH-3604 D1)

 health ────► [Heartbeat]─►[Snapshot]─►[Membership]─►[Ejection]─►[LocalSweeps]─►[Leadership]─►[Evaluation]─►[Governor]─► Dispatcher lanes
                               │             │                        │   ▲             │             ▲   │
                               │             └── NodeDeparted ────────┼───┼─────────────┼─────────────┼───┼──► Dispatcher.AbandonLane
                               │                                      ▼   │             │             │   ▼
                               │                              AgentRegistry ◄── remote StartAgents / StopAgents (DeferredAgentCommandRunner)
                               │                                                        │             │
                               └────────────────────────────────────────────────────────┴── PendingAssignmentLedger ◄── lane confirmations
```

Three kinds of thing:

- **Stages** are pure-ish functions `TickContext -> TickContext` with private cross-tick
  state. A stage never blocks on another node. A stage that throws forwards a degraded
  context (see §5); it never faults the pipeline.
- **Actors** own state that has more than one writer: the ledger, the agent registry, the
  lock session, the dispatcher lanes. Each has a mailbox (a `Channel<T>`) and a single
  consumer, and exposes a snapshot for stages to read. Today the same state is protected by
  convention plus two locks.
- **Tickers** produce ticks. The heartbeat ticker runs only the Heartbeat stage. The health
  ticker runs the whole chain. Ticks must never overlap (the `_healthCheckGuard` exists
  because overlapping ticks corrupted `_lastLockIndex` / `_lastLockETag` on the lease
  backends); a tick that arrives while one is running is **dropped**, not queued. With TPL
  Dataflow that is `BoundedCapacity = 1`, `MaxDegreeOfParallelism = 1`, `TryPost`. With a
  plain loop it is the existing CAS guard. Both are fine; see §6.

## 3. The context

```csharp
record TickContext(
    long Sequence,                       // CurrentCommandSequence at tick start (GH-4407): a stop stamped later wins
    DateTimeOffset Now,
    NodeIdentity Self,                   // node id, node number, capabilities, role, CanLead
    // added by Snapshot
    IReadOnlyList<WolverineNode>? Nodes, AgentRestrictions? Restrictions, bool SelfRowPersisted,
    // added by Membership
    IReadOnlySet<Guid>? Stale, IReadOnlySet<Guid>? Departed, IReadOnlySet<Uri>? Orphans, bool WaitForSettle,
    // added by Ejection
    IReadOnlyList<WolverineNode>? Ejected,
    // added by Leadership
    LeadershipState? Leadership,         // Follower | Leader(evaluationsSinceElection) | SteppedDown(reason) | Ineligible
    // added by Evaluation
    AssignmentGrid? Grid, AgentCommands? Commands,
    // accumulated by every stage
    IReadOnlyList<StageFault> Faults,    // which stage degraded and why
    StageMetrics Metrics);               // the sensor record, §8
```

Everything a later stage needs is on the context. No stage reads another stage's fields.

## 4. Stage map

| # | Stage | Reads | Adds to the context | Owns across ticks | Must preserve | Today |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | **Heartbeat** | `Self`, load sample | nothing (side effect: `MarkHealthCheckAsync`; on a miss `ReregisterNodeAsync` + re-upsert own assignment rows) | the captured capability list; reads the embargo set from LocalSweeps' state | Runs on both tickers. Re-registration keeps the **same** node number and capabilities (GH-3604 D2). Load rides every heartbeat (GH-3959). Never shares a guard with the tick (GH-3604 D1) | `WriteHeartbeatAsync`, `ensureLocalNodeRegisteredAsync` |
| 2 | **Snapshot** | — | `Nodes`, `Restrictions`, `SelfRowPersisted`; `Sequence` is captured **before** the load | nothing | Sequence before load (GH-4407). The self row injected for a lagging read is synthetic and carries no agents, and later stages must know that (`SelfRowPersisted`) (GH-2682) | `LoadNodeAgentStateAsync` + the injection in `DoHealthChecksInternalAsync` |
| 3 | **Membership** | `Nodes`, `Now` | `Stale` (never self), `Departed`, `Orphans` (agents of stale + departed rows, GH-4897), `WaitForSettle`; emits `NodeDeparted` to the dispatcher | `_rowsLastTick`, `_knownNodes`, `_lastNodeJoin`, `_waitingSince` | A departure never waits (its agents run nowhere). The settle wait is bounded by `MaxAssignmentSettleTime` and ended by `AssignmentSettleNodeCount`. Runs on every node whatever its role, so a follower inherits a warm membership picture when elected | `noteDepartedRows`, stale filter, `trackMembershipAndDecideWait` |
| 4 | **Ejection** | `Stale`, lock-holder fact from the LockSession actor | `Ejected`; emits `NodeDeparted` | `_staleObservations` | Hysteresis: delete only after `StaleNodeEjectionThreshold` consecutive sightings, streak reset on a fresh read (GH-3604 D1). Never self (GH-1116). A stale **leader** row only by the lock holder. Runs on nodes that cannot lead, so a stale non-leader is still ejected by a warm-up-only cluster. Records `DormantNodeEjected` only for rows actually deleted (GH-3698) | `ejectStaleNodes` |
| 5 | **LocalSweeps** | `Nodes` (own persisted row only), `Restrictions`, `Sequence`, the AgentRegistry snapshot | nothing on the context; side effects through the AgentRegistry (start / stop / claim) | `_reconcileObservations`, `_reportedFailures`, `_restartedWedgedAgents`, `_transientRestarts`, `_failedStarts`, `_releasedAgents`, `_reportedUnreleasable` | Skip when `!SelfRowPersisted`. Act only on a divergence seen `LocalAgentReconciliationThreshold` ticks running; cap per tick; a stop stamped after `Sequence` wins; a stop leaves a peer's row alone; a claim is insert-if-absent (GH-3987, GH-4407, GH-4240). Release only when a live peer advertises the capability, embargo written **before** the row is dropped (GH-3888). Follower-capable by design: the divergences live on the node that has them | `ReportFailedLocalAgentsAsync`, `ReconcileLocalAgentsAsync`, `RestoreExpiredReleaseEmbargoesAsync` |
| 6 | **Leadership** | `Self.CanLead`, the LockSession actor | `Leadership` | `IsLeader`, `EvaluationsSinceElection` | Order inside the stage: eligibility gate → liveness ping → attain-or-renew → claim or step-down. Attain is called **every** tick (renewal on lease backends; client-side short-circuit on PostgreSQL so session locks never stack). A step-down releases the lock (load-bearing for eligibility, see the election README) and does not touch the leader row; the claim upserts it. Claim failure releases the lock. Bug ledger (c): a leader should re-assert its row on every renewed tick | `DoHealthChecksInternalAsync` lower half, `tryStartLeadershipAsync`, `stepDownAsync` |
| 7 | **Evaluation** | `Nodes` minus stale, `Restrictions`, `Orphans`, `Leadership`, `WaitForSettle`, the ledger snapshot | `Grid`, `Commands` (before batching) | nothing — the ledger moves to its actor; `LastAssignments` becomes a stage output | Only when `Leadership` is `Leader` and not `WaitForSettle`. Family order with durability last (GH-3785). Per family: restrictions, then the ledger projection (only this family's agents, GH-4901), then distribute. Holds: takeover / cold-start on bare first-time placements, orphans exempt (GH-4897, GH-4569). Duplicate-row healer (GH-2602). Undeclared-placement tripwire (GH-4555). Arms the ledger from what it emits; the TTL backstop only applies once the dispatcher says the command finished | `EvaluateAssignmentsAsync` |
| 8 | **Governor** | `Commands`, `Metrics`, lane depths, ledger age | the commands actually released this tick, batched (`AgentStartBatchSize`, GH-3604 D3 / GH-4718 / GH-3749) | the control state of §8 | What it withholds must be safe to withhold: evaluation is re-run every tick from the table, so an unemitted command is simply re-decided next tick; the ledger must only be armed for what is **released**, not for what was decided | `batchCommands` today; the rest is new |

Solo mode is a second stage list: Heartbeat (no-op store), Snapshot (restrictions only),
LocalStart (start every known agent not paused, GH-4672). That it fits the same skeleton
with different stages is the test of the pluggability claim.

## 5. Failure policy per stage

Today the sweeps are wrapped in try/catch so "a fault here can never cost this node its
heartbeat or its leadership lease", and the whole tick is wrapped once more in the loop.
In the pipeline each stage catches, records a `StageFault`, and forwards. Two stages are
special:

- **Leadership** must run even if Snapshot or LocalSweeps faulted, because on RavenDb and
  Cosmos the attain call **is** the lease renewal; skipping it for one tick is a step-down.
  It needs `Nodes` only for the claim's `self` row, and can use the synthetic self.
- **Evaluation** must **not** run on a degraded snapshot (a partial node list would be read
  as departures). A fault in Snapshot or Membership short-circuits to Leadership and skips
  Evaluation for the tick.

A faulted Dataflow block propagates and completes the pipeline; that is the wrong semantics
here. Whatever the mechanism, exceptions stop at stage boundaries.

## 6. Mechanism

Because one tick is one object, a middleware chain — `IReadOnlyList<ITickStage>` run in
order by the existing loop, exactly the shape of Wolverine's own handler chain — gives the
isolation and the testability with no new dependency. TPL Dataflow earns its keep for two
things the loop does not have: a bounded mailbox that drops a tick while one is running
(instead of the CAS guard), and a `CancellationToken` that unwinds every stage at shutdown
through one `Complete()`. It costs: completion/fault propagation semantics that fight §5,
and a per-block thread hop between stages that do nothing but add a field. Recommendation:
chain of stages for the tick; `Channel<T>` actors for the four shared-state owners (the
dispatcher already is one); Dataflow only if a stage ever needs its own cadence.

Concurrency rules, all of which hold today and must survive:

1. One tick at a time; a tick that arrives mid-tick is dropped.
2. The heartbeat ticker touches only the Heartbeat stage and the embargo set.
3. Each actor has one consumer. Stages read actor **snapshots**, never actor fields.
4. The ledger is armed by the Governor for released commands and confirmed by the lanes;
   both are messages to the ledger actor, so the `_pendingLock` disappears.
5. The AgentRegistry is the only writer of `Agents` and the in-flight sets; the sweep sends
   it `Start(uri, asOfSequence)` / `Stop(uri, removeAssignment)` / `Claim(uri)` and the
   remote handlers send it batches. The GH-3748 stop-revocation ordering is then enforced in
   one place.

## 7. Pluggability points

Inside the fixed skeleton, not by reordering it:

| Point | Today | As a plug |
| --- | --- | --- |
| stale / ejection policy | `StaleNodeTimeout` + threshold | `IStalePolicy.Classify(nodes, now, streaks)` |
| settle / hold policy | three settle knobs + two hold counters | `IPlacementGate` (see §8: this is where feedback goes) |
| lock provider | `INodeAgentPersistence` lock members | already pluggable; becomes the LockSession actor's backend |
| distribution | `IAgentFamily` | unchanged |
| load signal | `INodeLoadMonitor` | unchanged; its reading becomes a sensor |
| stage list | fixed | `Balanced` and `Solo` lists; tests compose shorter ones |

## 8. Feedback on the stage outputs

### What exists is open-loop

Every churn control shipped so far is feed-forward: a timer or a count chosen by the
operator, applied without measuring its effect.

| Rule | Knob | What it is really guessing |
| --- | --- | --- |
| hold the first pass while nodes join | `AssignmentSettlePeriod`, `MaxAssignmentSettleTime`, `AssignmentSettleNodeCount` | how long a rollout takes |
| hold bare placements after election / on a cold start | `LeaderTakeoverHoldEvaluations`, `LeaderColdStartHoldEvaluations` | how long a predecessor's start takes to surface as a row; how long peers take to register |
| chunk starts, stops and moves | `AgentStartBatchSize` | how much a node can start inside one reply window |
| bound start parallelism on the node | `MaxAgentStartParallelism` | the node's own capacity |
| give up on a batch | `AgentProgressStallTimeout`, `AgentProgressPollInterval`, `ReplyWindowFor(n)` | how slow honest work can be |
| shed one agent per evaluation above a band | `NodeOverloadThreshold`, `OverloadShedBatchSize`, the 10-point receive band | how fast load responds to a move |
| embargo a released capability | `AgentReleaseCooldown` | how long a sick node stays sick |
| ledger TTL | `2 × CheckAssignmentPeriod` | how long row visibility lags |
| restart backoff | `TransientFailureRestartMaxBackoff` | pool recovery time |
| debounce the reconcile sweep | `LocalAgentReconciliationThreshold` | row-write latency |

Each of these was added by a ticket about a fleet where the previous guess was wrong
(GH-3604, GH-3698, GH-3748, GH-3750, GH-3987, GH-4897, GH-4569, GH-4718). That pattern —
one more timer per incident — is the signature of an open loop on a plant whose time
constants vary by two orders of magnitude between deployments (a shard start is 27 s p50 /
215 s max on one fleet and milliseconds on another).

### What the plant looks like

- **Actuators** (what the leader can change): how many starts, stops and moves it releases
  per tick and per destination; whether rebalance moves are allowed at all; the batch size.
  Node-side: start parallelism, restart timing.
- **Sensors** (what is observable, mostly already observed but not recorded): per tick,
  commands decided vs released; per lane, queue depth and the executing command's age;
  confirmations per poll (`AgentPresenceReport` is literally an ACK stream); ledger size and
  the age of its oldest entry; unconfirmed fraction of the last batch; membership change
  rate; each node's `load_factor`; from the sweep, the count of "row but no runner" and
  "runner but no row"; `AssignmentChanged` records per tick (the number the GH-3987 reporter
  measured: 24k for one rollout).
- **Disturbances**: membership changes, load changes, a slow store.
- **Dead time**: large and variable. A decision becomes a row only after dispatch, start,
  and the next snapshot — tens of seconds at best, minutes under a rebuild. The pending
  ledger is a **predictor of in-flight work** that compensates for that dead time (the same
  role a Smith predictor plays); it must stay, and any controller sits on top of it, not
  instead of it.
- **Cost asymmetry**: a move is not cheap or reversible (a projection replays), outputs are
  integers, and the safety rules (one copy, orphans placed now) are not negotiable. So the
  loops wanted are **rate limiters with hysteresis**, not fine proportional control.

### Where closing a loop simplifies

1. **Dispatch pacing: a per-lane window instead of batch size + stall timeout.** The
   dispatcher already has a lane per destination and already polls for progress. Give each
   lane a window of unconfirmed starts; release more only as confirmations arrive; grow the
   window additively on a fully confirmed batch, halve it on a stall (AIMD). That is TCP's
   loop on exactly TCP's signals, and it replaces the fixed `AgentStartBatchSize`, the
   scaled reply window, and `AgentProgressStallTimeout` as failure detector with one
   measured quantity: how fast *this* node confirms. A node mid-rebuild gets a window of a
   handful; an idle node gets hundreds. It is backpressure in the proper sense: the
   Governor releases per lane up to the window, the rest is re-decided next tick for free
   because evaluation is idempotent against the table.
2. **Rebalance gating: a measured quiet signal instead of three settle knobs.** Gate
   non-urgent moves (anything with a live `OriginalNode`) on two sensors: the membership
   change rate, low-passed (a settle period *is* a crude low-pass filter on joins), and the
   fleet-wide unconfirmed fraction. "Do not rebalance while more than x% of the last wave
   is unconfirmed or a node joined within the filter's horizon" needs no rollout-length
   guess, and it is the rule the rolling-deploy model already checks as "urgent placement
   never gated, rebalance gated on stability". The orphan rule stays a rule.
3. **Capacity shedding is already a loop; make it a stated one.** `load_factor` is the
   measurement, the 10-point band is the deadband, `OverloadShedBatchSize` the rate limit,
   and `MemoryPressureLoadMonitor`'s rise-fast-decay-slow is an asymmetric filter. Writing it
   as a controller with an error term and a rate limit would mostly document what is there,
   and would make the next question answerable: whether the shed rate should scale with the
   error (a node at 98% sheds faster than one at 91%).

### Where feedback does not help

- **The takeover hold is about lost information, not rate.** The new leader cannot see its
  predecessor's in-flight starts because the ledger is in memory. No controller recovers
  information that was never recorded; a durable, leader-written claim (the epoch design in
  `formal/agent-assignment/`) does. Feedback would only tune the hold's length.
- **Safety rules** (single copy, owner-scoped deletes, orphans placed immediately, pins and
  pauses outrank everything) stay rules. A controller decides *how much* of the safe plan to
  release, never *which* plan.
- **Loops interacting.** Node-local restart backoff, the release embargo, leader placement
  and capacity shedding all act on the same agents. The standard discipline applies: inner
  loops faster than outer ones (node heals itself before the leader moves it, the leader
  moves before anything scales), each with a deadband wider than the other's step, or they
  oscillate — the receive band under the shed line exists because that oscillation was
  observed. The `AgentReleaseCooldown` is the embargo that keeps the node loop and the
  leader loop from fighting over a sick node; it stays, as a rate limit.

### The Governor stage

A single place that reads the sensor record from the previous ticks and sets the actuators
for this one: per-lane windows, the rebalance gate, the shed rate. Its inputs are stage
outputs that already exist and only need recording; its outputs replace the knobs in the
table above, which remain as ceilings and floors. Concretely, the first cut is two
controllers and a filter: the AIMD lane window, the low-passed membership signal, and the
gate that combines them. Everything else stays open-loop until a measurement says otherwise.

### How to know it worked

- **Safety** in P: the rolling-deploy spec already states "urgent never gated, at most one
  rebalance move while membership is in flux"; a gate driven by a measured signal is a
  different oracle for the same property and slots into that model.
- **Dynamics** on a rig, not in a model: a model checker has no time. The deploy-sim rig
  (`wolverine-deploy-sim`, minikube) measured the GH-3987 churn and the duplicate rate; it
  is the harness for before/after on `AssignmentChanged` records per rollout, time to full
  throughput, and duplicate count, with interleaved runs per the perf-rig ground rules.
- **Tests** per stage with a synthetic sensor history, which is the thing the pipeline buys.

## 9. Migration

1. Introduce `TickContext` and extract the eight stages as methods that take and return it,
   behind the existing `DoHealthChecksInternalAsync`. No behaviour change; the compliance
   tests, `fleet_scale_scenarios` and the chaos soak are the safety net.
2. Move each stage's cross-tick fields into the stage. The ledger and the registry become
   actors with mailboxes; `_pendingLock` and the by-convention serialisation go.
3. Replace the loop body with the stage list; add the Solo list.
4. Record `StageMetrics` on every tick; publish them (they are the GH-3987 reporter's ask
   for per-node counts, and the GH-4593 load advertisement is already one of them).
5. Only then the Governor, one loop at a time, each with a rig measurement.

## 10. Open questions

- Whether `WolverineNode.IsLeader()` should keep reading the assignment row at all once
  the Leadership stage owns the fact (election bug ledger (c) is a consequence of that
  dual source of truth).
- Whether the lane window should be per destination (a slow node) or per family (a slow
  store), or both.
- Which of the knobs in §8 become ceilings for the controllers and which are deleted. The
  compat answer is "ceilings", the honest answer is that several exist only because the
  previous guess failed on one fleet.
