# The cluster control tick as a staged pipeline, and feedback on its outputs

Status: **proposal**, written against `origin/main` at 6.50.2 (2026-10-15), revised
2026-10-16 after a critical review (see §12). Nothing here is implemented. It is the
refactoring target for `NodeAgentController` and its partials, and the place to settle the
shape before touching code. The mechanism as it exists is described in
`docs/guide/durability/leadership-and-agent-distribution.md`; the invariants quoted below are
the ones the P models under `formal/` check, with GH numbers where a comment in the code
records the incident that made the rule load-bearing.

## 1. What is intertwined today, precisely

`NodeAgentController.DoHealthChecksInternalAsync` is already a fixed, linear sequence. The
coupling is in **state** rather than control flow: one class holds two dozen fields, each
annotated "only touched from the serialized health-check path" or guarded by
`_healthCheckGuard` / `_pendingLock`, and three of them have a second writer on another
thread:

| State | Written by | Also written or read by |
| --- | --- | --- |
| `_pendingAssignments` (the ledger) | evaluation (`reconcilePendingAssignments`) | dispatcher lane threads (`ConfirmDispatched`); read through the `PendingDispatches` probe |
| `Agents`, `_startingAgents`, `_stoppingAgents`, `_stopRevocations`, `_commandSequence` | remote command handlers (`StartAgentAsync` / `stopAgentAsync` via `DeferredAgentCommandRunner`) | the reconcile sweep and the failed-agent sweep on the tick |
| `_releasedAgents` (embargo) | the failed-agent sweep | the independent heartbeat loop (`buildLocalNode` on a resurrection) |

Everything else (`_staleObservations`, `_reconcileObservations`, `_knownNodes` and the
settle timestamps, `EvaluationsSinceElection`, `OrphanedByStaleNodes`, `_rowsLastTick`,
`_transientRestarts`, `_failedStarts`, `IsLeader`) is single-writer tick state that could be
more cleanly encapsulated with the step that owns it. Two further callers run the tick's
logic from other threads: `ApplyRestrictionsAsync` runs a full evaluation inline on the
operator's thread, and the `CheckAgentHealth` handler calls `DoHealthChecksAsync`
concurrently with the loop; `_healthCheckGuard` exists for them.

## 2. Target shape

```
 tickers                       pipeline: typed stages, fixed order, one tick at a time                       sinks
 ───────                       ───────────────────────────────────────────────────────                       ─────
 heartbeat ──► Heartbeat   (own ticker; reads the published NodeAdvertisement; GH-3604 D1)

 health ─┐
 requests┴─► coalescing queue (depth 1) ─► Heartbeat ─► Snapshot ─► Membership ─► Ejection ─► LocalSweeps ─► Leadership ─► Evaluation ─► Gate ─► Dispatcher.Enqueue
                                                            │                                     │  ▲                         │
                                                            └── NodeDeparted ─────────────────────┼──┼─────────────────────────┼──► Dispatcher.AbandonLane
                                                                                                  ▼  │                         │
                                                                                          AgentRegistry ◄── remote StartAgents / StopAgents
                                                                                                                               │
                              Dispatcher lane events (Queued / HandedOver / Finished) ─────────────────────────────────────────┘ drained by Snapshot, folded by Evaluation
```

Three kinds of thing:

- **Stages** are functions `In -> Out` with their own small output type and private
  cross-tick state. The pipeline is function composition; "a stage reads only what earlier
  stages produced" is a fact of the types, not a rule. A stage that fails returns its fault
  type; it never faults the pipeline (§5).
- **Actors** own state that genuinely has more than one writer. After §10 there are two: the
  **AgentRegistry** (starts and stops arrive from remote commands and from the sweep) and the
  **Dispatcher** (commands are enqueued by the tick, finished by its lanes). Each exposes an
  immutable snapshot for stages to read and emits events the tick folds.
- **Tickers and requests** produce ticks through one coalescing queue of depth one: a
  scheduled tick that arrives while one is running is dropped; a *request* (operator
  restriction change, health kick, shutdown) replaces whatever is waiting and is never
  dropped, because someone is awaiting it. Ticks never overlap — the `_healthCheckGuard`
  exists because overlapping ticks corrupted `_lastLockIndex` / `_lastLockETag` on the lease
  backends.

## 3. Typed stage outputs

Each stage returns a record; a later stage takes the earlier records it needs as parameters.
There is no shared bag of nullable fields whose validity depends on which stage has run.

```csharp
record Tick(long Sequence, DateTimeOffset Now, NodeIdentity Self, TickReason Reason,
            IReadOnlyList<DispatchEvent> DispatchEvents, IReadOnlyList<AgentEvent> AgentEvents);
                                         // Sequence = the AgentRegistry's command sequence at tick start (GH-4407)

record SnapshotResult(IReadOnlyList<WolverineNode> Nodes, AgentRestrictions Restrictions, bool SelfRowPersisted);
record SnapshotFault(Exception Error);   // Snapshot returns one or the other

record MembershipResult(IReadOnlySet<Guid> Stale, IReadOnlySet<Guid> Departed, IReadOnlySet<Uri> Orphans, bool WaitForSettle);
record EjectionResult(IReadOnlyList<WolverineNode> Ejected);
record LeadershipState;                  // Follower | Leader(int EvaluationsSinceElection) | SteppedDown(string Reason) | Ineligible
record EvaluationResult(AssignmentGrid Grid, IReadOnlyList<IAgentCommand> Decided);
record GateResult(IReadOnlyList<IAgentCommand> Released, IReadOnlyList<IAgentCommand> Withheld);
record TickOutcome(LeadershipState Leadership, GateResult? Gate, IReadOnlyList<StageFault> Faults, StageMetrics Metrics);
```

`TickOutcome` is what a request's awaiter receives and what the metrics record is built from.

## 4. Stage map

| # | Stage | In | Out | Owns across ticks | Must preserve | Today |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | **Heartbeat** | `Tick`, the published `NodeAdvertisement` | — (side effect: `MarkHealthCheckAsync`; on a miss `ReregisterNodeAsync` + re-upsert own assignment rows) | nothing | Runs on both tickers. Re-registration keeps the **same** node number and capabilities (GH-3604 D2). Load rides every heartbeat (GH-3959). Never shares a guard with the tick (GH-3604 D1) | `WriteHeartbeatAsync`, `ensureLocalNodeRegisteredAsync` |
| 2 | **Snapshot** | `Tick` | `SnapshotResult \| SnapshotFault` | nothing | `Sequence` is read **before** the load (GH-4407). The self row injected for a lagging read is synthetic and carries no agents; `SelfRowPersisted` says so (GH-2682) | `LoadNodeAgentStateAsync` + the injection |
| 3 | **Membership** | `SnapshotResult` | `MembershipResult`; emits `NodeDeparted` to the dispatcher | `_rowsLastTick`, `_knownNodes`, `_lastNodeJoin`, `_waitingSince` | Never self stale. A departure never waits (its agents run nowhere). The settle wait is bounded by `MaxAssignmentSettleTime` and ended by `AssignmentSettleNodeCount`. Runs on every node whatever its role, so a follower inherits a warm picture when elected | stale filter, `noteDepartedRows`, `trackMembershipAndDecideWait` |
| 4 | **Ejection** | `MembershipResult`, the lock-holder fact | `EjectionResult`; emits `NodeDeparted` | `_staleObservations` | Delete only after `StaleNodeEjectionThreshold` consecutive sightings, streak reset on a fresh read (GH-3604 D1). Never self (GH-1116). A stale **leader** row only by the lock holder. Runs on nodes that cannot lead. `DormantNodeEjected` only for rows actually deleted (GH-3698) | `ejectStaleNodes` |
| 5 | **LocalSweeps** | `Tick`, `SnapshotResult`, the `RegistrySnapshot` | `Reconcile` message to the registry; the next `NodeAdvertisement` | `_reconcileObservations`, `_reportedFailures`, `_restartedWedgedAgents`, `_transientRestarts`, `_failedStarts`, `_releasedAgents`, `_reportedUnreleasable` | Two parts with different gates. The **failed-agent report and the embargo lift run on every tick** (today they run before the snapshot, ungated); only the **reconcile sweep** is skipped when `!SelfRowPersisted`. Reconcile acts on a divergence seen `LocalAgentReconciliationThreshold` ticks running, capped per tick, decided against `Tick.Sequence`; a stop leaves a peer's row alone; a claim is insert-if-absent (GH-3987, GH-4407, GH-4240). Release only when a live peer advertises the capability, embargo written **before** the row is dropped (GH-3888). Follower-capable by design | `ReportFailedLocalAgentsAsync`, `RestoreExpiredReleaseEmbargoesAsync`, `ReconcileLocalAgentsAsync` |
| 6 | **Leadership** | `Tick.Self`, `SnapshotResult?` (the claim's self row; the synthetic self suffices) | `LeadershipState` | `IsLeader`, `EvaluationsSinceElection`, the lock session | Order: eligibility gate → liveness ping → attain-or-renew → claim or step-down. Attain is called **every** tick (renewal on lease backends; client-side short-circuit on PostgreSQL). Runs even when Snapshot faulted. A step-down releases the lock and does not touch the leader row; the claim upserts it; claim failure releases the lock. Bug ledger (c) in the election README: a leader should re-assert its row on every renewed tick | lower half of `DoHealthChecksInternalAsync`, `tryStartLeadershipAsync`, `stepDownAsync` |
| 7 | **Evaluation** | `SnapshotResult`, `MembershipResult`, `LeadershipState`, `Tick.DispatchEvents` | `EvaluationResult` | the pending ledger (single writer, see §10.1) | Only when `Leader` and not `WaitForSettle`. Family order with durability last (GH-3785). Per family: restrictions, ledger projection for this family's agents only (GH-4901), distribute. Holds on bare first-time placements, orphans exempt (GH-4897, GH-4569). Duplicate-row healer (GH-2602). Undeclared-placement tripwire (GH-4555). Does **not** arm the ledger: that happens on release | `EvaluateAssignmentsAsync` minus `reconcilePendingAssignments` and `batchCommands` |
| 8 | **Gate** | `EvaluationResult`, `MembershipResult`, the last few `StageMetrics` | `GateResult`; arms the ledger for `Released`; hands `Released`, batched (`AgentStartBatchSize`, GH-3604 D3 / GH-4718 / GH-3749), to `Dispatcher.Enqueue` | the gate's filter state (§8) | Only **first-time starts and moves** may be withheld. Stops, the duplicate healer's stops, operator-driven commands and anything for an orphan pass straight through — a withheld stop defers a pause (GH-3663) or keeps a duplicate alive, and withholding the stop half of a move while releasing its start is a second copy. Evaluation re-runs every tick from the table, so a withheld start is simply re-decided; the ledger is armed only for what was released, so a withheld start is not held on a `PendingNode` either | `batchCommands`; the gating is new |

Solo mode is a second stage list: Heartbeat (null store), Snapshot (restrictions only),
LocalStart (start every known agent that is not paused, GH-4672). That it fits the same
skeleton with different stages is the test of the pluggability claim.

## 5. Failure policy, in the types

Today the sweeps are wrapped in try/catch so "a fault here can never cost this node its
heartbeat or its leadership lease", and the whole tick is wrapped once more. With typed
outputs the policy is a pattern match rather than a `Faults` list every stage consults:

- `Snapshot` returns `SnapshotFault` → Membership, Ejection, the reconcile sweep and
  Evaluation are skipped (a partial node list would read as departures); the failed-agent
  report and embargo lift still run; **Leadership still runs** (on RavenDb and Cosmos the
  attain call *is* the lease renewal, and skipping it for a tick is a step-down).
- Any other stage throws → the pipeline records a `StageFault`, substitutes that stage's
  empty result, and continues. Leadership's own failure is the only one that changes the
  outcome (`SteppedDown`), exactly as today's `catch` around `TryAttainLeadershipLockAsync`.

## 6. Mechanism and concurrency rules

Because one tick is a handful of small records, a chain of `ITickStage` implementations run
in order by one loop — the shape of Wolverine's own handler chain — gives the isolation and
the testability with no new dependency. TPL Dataflow would add a bounded mailbox and
`Complete()`-based cancellation, at the cost of completion and fault propagation semantics
that fight §5 and a thread hop per stage. Recommendation: stage chain for the tick,
`Channel<T>` mailboxes for the two actors (the dispatcher already has them), the
coalescing request queue for ticks. Dataflow only if a stage ever needs its own cadence.

Rules, all of which hold today and must survive:

1. One tick at a time. A scheduled tick that arrives mid-tick is dropped; a request waits
   (depth one, newest request wins, and the superseded request is completed with the same
   outcome).
2. The heartbeat ticker runs only the Heartbeat stage and reads only the published
   advertisement.
3. Stages read actor **snapshots**; actor fields are touched by their owner only.
4. `Dispatcher.Enqueue` stays a synchronous call on the tick thread (it has exactly one
   caller, the Gate). Its dedup against work already queued or in flight therefore stays
   synchronous too, which is what keeps the next tick from queuing a second start for the
   same agent before this tick's `Queued` event could be folded.
5. The ledger is armed by the Gate on release and updated from the dispatcher's events at
   the top of the next tick (§10.1). One writer; the `_pendingLock` goes.
6. The AgentRegistry is the only writer of `Agents` and the in-flight sets. Its command
   sequence is the GH-4407 / GH-3748 counter, bumped on every `BeginStop`, and is what
   `Tick.Sequence` and `Reconcile.AsOfSequence` compare against.
7. **Shutdown is a request**, not a cancellation: the `Shutdown` tick runs the sequence the
   code runs today, in this order — stop every local agent (bounded parallelism,
   `MaxAgentStopParallelism`), release the lock if held, delete the node row — and only then
   are the loops stopped and the actors completed. The order matters: releasing before the
   agents are down lets a new leader re-place them while they are still running (duplicates
   during a rolling deploy), and deleting the row while the lock is held leaves a leader with
   no row. The GH-4261 interleaving (a health check pinging the lock session while shutdown
   releases it) cannot occur, because both are ticks on the same queue.

## 7. Pluggability points

Inside the fixed skeleton, not by reordering it:

| Point | Today | As a plug |
| --- | --- | --- |
| stale / ejection policy | `StaleNodeTimeout` + threshold | `IStalePolicy.Classify(nodes, now, streaks)` |
| settle / hold / gate policy | three settle knobs + two hold counters | `IPlacementGate` (§8) |
| lock provider | `INodeAgentPersistence` lock members | unchanged; owned by the Leadership stage |
| distribution | `IAgentFamily` | unchanged |
| load signal | `INodeLoadMonitor` | unchanged; its reading becomes a sensor |
| stage list | fixed | `Balanced` and `Solo` lists; tests compose shorter ones |

## 8. Feedback on the stage outputs

### What exists is open-loop

Every churn control shipped so far is feed-forward: a timer or a count chosen by the
operator, applied without measuring its effect.

| Rule | Knob | What it is really estimating |
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

Each was added by a ticket about a fleet where the previous estimate did not fit (GH-3604,
GH-3698, GH-3748, GH-3750, GH-3987, GH-4897, GH-4569, GH-4718). One more timer per incident
is the signature of an open loop on a plant whose time constants vary by two orders of
magnitude between deployments (a shard start is 27 s p50 / 215 s max on one fleet and
milliseconds on another).

### What the plant looks like

- **Actuators**: how many starts and moves are released per tick and per destination;
  whether rebalance moves are allowed at all; the batch size. Node-side: start
  parallelism, restart timing.
- **Sensors** (observed today, mostly not recorded): commands decided vs released per
  tick; per lane, queue depth and the age of the executing command; confirmations per
  poll (`AgentPresenceReport` is an ACK stream); ledger size and oldest age; unconfirmed
  fraction of the last batch; membership change rate; each node's `load_factor`; from the
  sweep, "row but no runner" and "runner but no row" counts; `AssignmentChanged` records
  per tick (the number the GH-3987 reporter measured: 24k for one rollout).
- **Disturbances**: membership changes, load changes, a slow store.
- **Dead time**: large and variable. A decision becomes a row only after dispatch, start,
  and the next snapshot — tens of seconds at best, minutes under a rebuild. The pending
  ledger is a **predictor of in-flight work** that compensates for that dead time (the role
  a Smith predictor plays); it stays, and any controller sits on top of it.
- **Cost asymmetry**: a move is not cheap or reversible (a projection replays), outputs are
  integers, and the safety rules (one copy, orphans placed now) are not negotiable. The
  loops wanted are **rate limiters with hysteresis**, not fine proportional control.

### Two loops, in two places

**Dispatch pacing lives in the lane.** The lane already has every signal a congestion
window needs: its own queue depth, the confirmations from presence polls and the batch
reply, and the stall it is waiting out. So the lane keeps a window of unconfirmed starts
and releases from its own queue as confirmations arrive — additive increase on a fully
confirmed batch, halve on a stall (AIMD, TCP's loop on TCP's signals). That replaces the
fixed `AgentStartBatchSize` as pace-setter, the scaled reply window, and
`AgentProgressStallTimeout` as failure detector with one measured quantity: how fast *this*
node confirms. A node mid-rebuild gets a window of a handful; an idle node gets hundreds.
The tick never touches pacing, and no stage reads lane state. `AgentStartBatchSize`
remains as the window's ceiling and the wire-size bound (GH-4718).

**Rebalance gating lives in the Gate stage, and that is all the Gate does.** It gates
non-urgent moves (anything with a live `OriginalNode`) on two measured signals: the
membership change rate, low-passed (a settle period *is* a crude low-pass filter on
joins), and the fleet-wide unconfirmed fraction from the folded dispatch events. "Do not
rebalance while more than x% of the last wave is unconfirmed or a node joined within the
filter's horizon" needs no rollout-length estimate, and it is the rule the rolling-deploy
model already checks as "urgent placement never gated, rebalance gated on stability". The
orphan rule stays a rule; stops pass through (§4 row 8).

**Capacity shedding is already a loop; make it a stated one.** `load_factor` is the
measurement, the 10-point band is the deadband, `OverloadShedBatchSize` the rate limit,
and `MemoryPressureLoadMonitor`'s rise-fast-decay-slow is an asymmetric filter. Writing it
down as a controller mostly documents what is there, and makes the next question
answerable: whether the shed rate should scale with the error.

### Where feedback does not help

- **The takeover hold is about lost information, not rate.** The new leader cannot see its
  predecessor's in-flight starts because the ledger is in memory. No controller recovers
  information that was never recorded; a durable, leader-written claim (the epoch design in
  `formal/agent-assignment/`) does. Feedback would only tune the hold's length.
- **Safety rules** (single copy, owner-scoped deletes, orphans placed immediately, pins and
  pauses outrank everything) stay rules. A controller decides *how much* of the safe plan
  to release, never *which* plan — and only the start half of it.
- **Loops interacting.** Node-local restart backoff, the release embargo, leader placement
  and capacity shedding act on the same agents. Inner loops faster than outer ones (the
  node heals itself before the leader moves it, the leader moves before anything scales),
  each with a deadband wider than the other's step, or they oscillate — the receive band
  under the shed line exists because that oscillation was observed. `AgentReleaseCooldown`
  stays as the rate limit between the node loop and the leader loop.

### How to know it worked

- **Safety** in P: the rolling-deploy spec already states "urgent never gated, at most one
  rebalance move while membership is in flux"; a gate driven by a measured signal is a
  different oracle for the same property and slots into that model.
- **Dynamics** on a rig: a model checker has no time. The deploy-sim rig
  (`wolverine-deploy-sim`, minikube) measured the GH-3987 churn and the duplicate rate; it
  is the harness for before/after on `AssignmentChanged` records per rollout, time to full
  throughput, and duplicate count, with interleaved runs per the perf-rig ground rules.
- **Tests** per stage and per lane with a synthetic signal history, which is the thing the
  pipeline buys.

## 9. Migration

1. Introduce the stage records and extract the eight stages as methods behind the existing
   `DoHealthChecksInternalAsync`. No behaviour change; the compliance tests,
   `fleet_scale_scenarios` and the chaos soak are the safety net.
2. Move each stage's cross-tick fields into the stage. The registry becomes an actor; the
   ledger moves into Evaluation with arming moved to the Gate.
3. Replace the loop body with the stage list and the request queue; route
   `ApplyRestrictionsAsync` and `CheckAgentHealth` through requests (§10.4). Add the Solo
   list.
4. Record `StageMetrics` on every tick and publish them (the GH-3987 reporter's ask for
   per-node counts; the GH-4593 load advertisement is already one).
5. The lane window, measured on the rig. Then the Gate's filter, measured on the rig.

## 10. Eliminating the shared state: events in, messages out

The rule: **every cross-cutting input is an event folded at the top of the tick; every
cross-cutting output is a message to a single-consumer actor; the only shared things left
are immutable snapshots swapped atomically.** No stage writes anything another thread reads.

### 10.1 The ledger becomes stage state, fed by events

Today a lane thread writes the ledger (`ConfirmDispatched`) and the evaluation probes the
dispatcher's live maps (`TryFindPendingDestination`). Instead the lanes emit events, the
Snapshot stage drains them into `Tick.DispatchEvents`, and Evaluation folds them into a
ledger it alone owns:

```csharp
abstract record DispatchEvent(long At);
record Queued(IAgentCommand Command, Guid Lane, long At)                 : DispatchEvent(At);
record HandedOver(Uri[] Agents, Guid Node, long At)                      : DispatchEvent(At);  // the lane took the start up; no longer withdrawable (GH-4901)
record Finished(IAgentCommand Command, Guid Lane, Uri[] Confirmed, bool Faulted, long At) : DispatchEvent(At);
```

| Event | Fold |
| --- | --- |
| armed on release (Gate, same thread) | entry is **outstanding** on its node |
| `Queued` / `HandedOver` | still outstanding (today `_inFlight` / `_starting`) |
| `Finished` | no longer outstanding; the TTL backstop starts **at `Finished.At`**, and `Confirmed` names the agents the node reported running (today `ConfirmDispatched` + the probe answering false) |
| a row for the agent on its pending node | entry retired (unchanged) |
| the pending node absent from the grid | entry dropped, agent free to be placed (unchanged: `applyPendingAssignmentsLocked`) |

One clock: the TTL runs from `Finished.At`, not from a re-stamped `SentAt`, so a
confirmation learned a tick late does not eat into the margin. No separate `Abandoned` or
`Confirmed` event: the node-absent rule already frees a dead lane's agents, and `Finished`
covers the executing command, including a cancelled one (`Faulted`).

Latency is the only behavioural difference: a finish reaches the leader's picture up to
one tick late. That is the safe direction — the leader believes a dispatch is outstanding
for slightly longer, which can only delay a re-drive; "not outstanding" is never learned
earlier than today. A dropped (coalesced) tick loses nothing: the fold happens at the start
of each executed tick and the channel keeps what accumulated.

### 10.2 The dispatcher's own bookkeeping

`Enqueue` stays a synchronous call from the Gate (rule 4 of §6); the lanes write on
completion. Those are still two writers of `_queued` / `_inFlight` / `_moving` / `_starting`
/ `_withdrawn` / `_handedOver`, so the dispatcher keeps its internal lock. The
`formal/agent-dispatch/` finding stands either way: the **admission check stays in the
lane**, because the one step no owner can cover is the command running after its node left
the roster (`tcAtomicStale`). Making `Enqueue` asynchronous was considered and rejected: it
opens a window in which the next tick cannot see this tick's queued start and emits a
second one.

### 10.3 The agent registry, in both directions

Remote `StartAgents` / `StopAgents` are slow and run in parallel, so the starts themselves
cannot live inside the tick; the bookkeeping can. A registry actor owns `Agents`, the
starting and stopping sets and the command sequence. Everything else talks to it:

```csharp
// in: from DeferredAgentCommandRunner's workers, the sweep, and operators
abstract record RegistryCommand;
record BeginStart(Uri Agent, long AsOfSequence, TaskCompletionSource<bool> Admitted) : RegistryCommand;
                                        // replies false when a stop newer than AsOfSequence exists (GH-3748); the worker starts nothing then
record EndStart(Uri Agent, bool Running, Exception? Failure)          : RegistryCommand;
record BeginStop(Uri Agent, bool RemoveAssignment)                    : RegistryCommand;   // bumps the sequence first (GH-3748, GH-4240)
record EndStop(Uri Agent);
record Reconcile(IReadOnlyList<ReconcileDecision> Decisions, long AsOfSequence) : RegistryCommand;  // from LocalSweeps

record ReconcileDecision(Uri Agent, ReconcileAction Action);           // Start | StopKeepingRow | Claim

// out: an immutable snapshot, and events for the tick
record RegistrySnapshot(long Sequence, ImmutableHashSet<Uri> Running, ImmutableHashSet<Uri> Starting,
                        ImmutableHashSet<Uri> Stopping, ImmutableDictionary<Uri, AgentStatus> Status);
abstract record AgentEvent(long Sequence);
record AgentStarted(Uri Agent, long Sequence)                          : AgentEvent(Sequence);
record AgentStopped(Uri Agent, long Sequence)                          : AgentEvent(Sequence);
record AgentFailed(Uri Agent, ShardFailure? Failure, long Sequence)    : AgentEvent(Sequence);
record AgentStartFailed(Uri Agent, Exception Failure, long Sequence)   : AgentEvent(Sequence);
```

The sequence is one counter with one meaning: `_commandSequence` as it exists today, bumped
by every stop, read into `Tick.Sequence` at tick start and into `BeginStart.AsOfSequence`
when a batch is accepted. `BeginStart` is a request with a reply because the worker needs
the answer before it calls `StartAsync`. The sweep becomes a pure function: `(my rows from
the snapshot, RegistrySnapshot, streaks) -> decisions`, sent as one
`Reconcile(…, AsOfSequence)`; the actor applies each under the rule the code already has —
a stop stamped after your sequence wins, a start already in flight is not started again, a
claim is insert-if-absent. Presence queries and the Heartbeat stage read the published
snapshot. The node-side assignment-row writes stay with the workers, after the start or
stop, exactly as now.

### 10.4 Operator and health-kick paths become requests

```csharp
record TickRequest(TickReason Reason, TaskCompletionSource<TickOutcome>? Completion);
enum TickReason { Scheduled, HealthKick, RestrictionsChanged, Shutdown }
```

- `ApplyRestrictionsAsync` persists the change first (GH-3666), posts `RestrictionsChanged`,
  awaits the `TickOutcome`, and then awaits the `Finished` events for the commands in
  `Gate.Released` that carry its restriction (so "your pause was applied" still means the
  stop ran, GH-3698, and GH-4871's "did the released agent actually start" check keeps its
  bounded wait). Those commands bypass the Gate's filter (§4 row 8).
- **Intended change:** today a follower that receives the operator call evaluates and
  dispatches commands itself (`EvaluateAssignmentsAsync` has no leader gate). Under this
  design only the leader evaluates, so a follower forwards the request to the leader over
  the control endpoint and awaits its outcome. This closes the one path by which a
  non-leader could dispatch assignment commands.
- `CheckAgentHealth` posts `HealthKick`. Both requests supersede a waiting scheduled tick
  rather than being dropped (§6 rule 1).

### 10.5 The advertisement is versioned, not forward-only

LocalSweeps publishes `NodeAdvertisement(Capabilities, LoadFactor, Version)` — the
captured capabilities minus the embargoed ones — and the Heartbeat stage reads the latest.
The release path's rollback (embargo, `ReregisterNodeAsync` fails, embargo removed,
GH-3888) publishes the rolled-back set as the next version. One writer, one reader, no
`ConcurrentDictionary`.

### 10.6 What stays shared

- `IsLeader` for `WolverineRuntime.IsLeader()`: a `volatile` read of the last
  `LeadershipState`. One writer.
- `LastAssignments` for diagnostics: the last `Grid`, swapped atomically. One writer.
- The dispatcher's internal maps (§10.2), behind its own lock, with the lane-side
  admission check.

After this the shared-state inventory of §1 is: two actors with mailboxes, two volatile
snapshots, and nothing protected by a serialisation convention.

## 11. Open questions

- Whether `WolverineNode.IsLeader()` should keep reading the assignment row at all once the
  Leadership stage owns the fact (election bug ledger (c) is a consequence of that dual
  source of truth).
- Whether the lane window should be per destination (a slow node) or per family (a slow
  store), or both.
- Which of the knobs in §8 become ceilings for the two loops and which are retired. The
  compatible answer is "ceilings"; several exist only because the previous estimate did not
  fit one fleet.

## 12. Review log

2026-10-16, critical review for simplicity (in the "simple, not complected" sense) of the
first draft. Changes made in response: typed per-stage outputs instead of one context of
nullable fields (§3, §5); the AIMD window moved from a Governor stage into the lane, and the
Governor reduced to a Gate that only ever withholds first-time starts and moves (§4 row 8,
§8); the ledger armed on release by the Gate and owned by Evaluation, stated once (§6 rule
5, §10.1); `Enqueue` kept synchronous (§10.2); one TTL clock and no duplicate `Abandoned`
path (§10.1); the failed-agent report and embargo lift no longer behind the
`SelfRowPersisted` gate (§4 row 5); `BeginStart` as request/reply and the registry sequence
identified with `_commandSequence` (§10.3); a coalescing request queue so an awaited
operator request is never dropped, with the operator's inline-failure contract kept and the
follower-forwarding change named as intended (§6 rule 1, §10.4); the advertisement
versioned so the embargo rollback is expressible (§10.5); the shutdown sequence stated
(§6 rule 7).
