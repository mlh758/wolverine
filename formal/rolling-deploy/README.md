# Agent assignment under a rolling deploy, on nodes with finite capacity — model checked

Does the assignment plane survive a Kubernetes rolling deploy — does an agent the durable
table says is owned actually *run* — and when a node dies, is its share redistributed
**within what the survivors can host**, or does it kill them too? This is
[GH-3987](https://github.com/JasperFx/wolverine/issues/3987) (a routine 3-pod rollout on a
~13.5k-agent fleet produced 24k `AssignmentChanged` records, and afterwards hundreds of
agents sat **assigned in `wolverine_node_assignments` but not running** — invisible from
outside, because the table records who *should* own an agent, not who runs one) together
with [GH-3959](https://github.com/JasperFx/wolverine/issues/3959) (34 hours of cascading
overload: one node over its memory limit died, its ~1,465 agents pushed the survivors over
*their* limits, and adding replicas made it worse — distribution is over live nodes with no
notion of what a node can host). The maintainers' scope notes say the two are one design
gap and must be solved together; this spec is that joint design. See
[`../README.md`](../README.md) for why these specs exist and how to run them.

Unlike the sibling specs, this one is not a model of the code as it stands. It is the
**written proposal as an executable model**: the committed model includes three design
changes, the checker shows the design with them is sound, and the mutant ledger shows that
removing any of them reproduces the reported failures as counterexamples.

1. **Stability-gated rebalancing** (GH-3987 asks 1 and 2). Moves of *running* agents —
   rebalance-for-evenness decisions — are gated on "membership has been stable for the
   debounce window". Urgent placement of unowned agents is never gated, and neither is the
   GH-2602 duplicate healer or the capacity shed. One rollout then causes one rebalance,
   not one per pod event, while a terminated pod's agents are still re-placed immediately
   mid-rollout.
2. **Assigned-vs-running reconciliation** (GH-3987 ask 3). Every node's health-check tick
   compares its *own* durable assignment rows against the agents it is actually running,
   and re-drives a start for any row with no runner — skipping agents whose own
   stop/row-removal is still in flight, so the sweep never fights a stop (the GH-4240
   residue class).
3. **A per-node capacity ceiling** (GH-3959) — and a **dynamic** one. Every node
   advertises a capacity *on every heartbeat*, not just at join: capacity is a live
   signal (an agent working off a backlog costs substantially more than an idle one, so a
   node under rebuild should advertise fewer slots — a fixed count can be exactly wrong
   when it matters most).
   Placement only targets nodes with headroom against the *current* advertisement; an
   agent that fits nowhere **waits unassigned** — visible, bounded degradation instead of
   a cascade — and is placed the moment capacity appears. A node found *above* its
   ceiling (an in-flight start landed on a node that filled up meanwhile — no ceiling can
   recall a command on the wire — or the ceiling itself shrank under memory pressure) is
   **shed**: the leader stops the excess and the urgent path re-places it within the
   ceilings. The assignment plane never interprets pressure; it just follows the number
   wherever it moves. How the node computes the number is a separate, node-local concern
   (see fix 3 below).

The properties, all in one monitor (`RollingDeploySpec.p`), asserted at every quiescent
moment — including quiet points *mid-rollout*, which is what makes "the deploy must not
strand agents" checkable:

- **Convergence.** At every quiet point, every agent runs on exactly one live node and the
  durable rows agree. Stated directly: never "assigned in the durable table but not
  running anywhere" — the GH-3987 trap — and never a row surviving on a departed node.
- **Churn.** At most **one** rebalance move is ever dispatched while membership is in
  flux. (The slack of one is the stability-snapshot race: a leader may act once on a
  node-state snapshot that predates the deploy-begin record, exactly as a wall-clock
  debounce window can race one pod event.)
- **Capacity.** No node is ever *at rest* above its **currently advertised** ceiling —
  asserted against the node's own latest advertisement, so a leader acting on a stale
  value cannot pass — and an agent waits unassigned **only** when no live node has
  headroom: waiting while capacity exists is the "parked forever" failure a ceiling must
  not create.
- **The cascade floor.** At most **one** node dies over capacity per run. One death can be
  an already-in-flight start landing on a node that filled up meanwhile; a *second* death
  means redistributing the first node's share overloaded the next node — the GH-3959
  cascade, exactly what the ceiling exists to prevent.
- **Evenness, up to capacity.** Once membership is stable, the settled load is even
  (max − min ≤ 1) among nodes that have headroom — a fuller node next to a node with *no*
  headroom is not an imbalance the leader may "fix".
- **Quiescence.** The cluster does go quiet: a wedged agent is reconciled, an orphan is
  re-placed or legitimately waits, the rollout ends, and the leader doesn't churn forever.

## What is modelled

| Model | Real thing |
| --- | --- |
| `Store` | the `node_assignments` rows (per node, per agent), the live-node set, advertised per-node capacities, leadership, and the stability oracle, serialized the way the database serializes them |
| `Node` | one process: its health-check tick (reconcile my own rows; if I'm the leader, evaluate shed/placement/healing/rebalance), the local facts of running its agents, and a kill line it dies beyond |
| `Deployer` | the rollout: replace nodes one at a time (new pod ready, then old pod terminated), scale down (terminate without replacement), or scale out; brackets everything in deploy-begin/end so the stability oracle is honest |
| `RunCourier` | an `AssignAgent` / `StopRemoteAgent` command in flight to a node |
| `Arsonist` / `StrikeCourier` | the wedge: arms the store, which fires on the next agent start it records, so the fault lands on an agent that really has a durable row |
| `TripCourier` | the overload death racing the leader's relief, the way an OOM kill races a rebalance in production |
| `SqueezeCourier` | rising memory pressure shrinking a hosting node's advertised capacity at runtime |
| `eRefill` | the health-check loop running forever: every durable change refunds tick budgets so the leader keeps polling until the cluster is clean |

The **wedge** is the GH-3987 fault class: an agent silently stops running while its durable
row survives. It abstracts every mechanism with that signature — a daemon shard dying
without a stop, a stop whose `RemoveAssignmentAsync` was lost, a paused agent whose resume
never came. The **trip** is the GH-3959 fault class: a node running past its **kill line**
dies, unless the leader's shed relieves it first. The kill line sits one agent *above* the
advertised ceiling, because a ceiling is a planning limit provisioned with margin below
the real limit (the incident's own numbers: ~6.4 GB planned against a 9.2 GB kill) — no
ceiling can recall a start already on the wire, so one in-flight race must fit in the gap.
The **squeeze** is the dynamic-capacity fault: a hosting node's advertised ceiling
shrinks at runtime (a backlog inflating per-agent cost, a noisy neighbor), and the plane
must shed down to the new number without dropping or duplicating anything. All three
faults are abstract on purpose: the property is that the design heals or bounds the
state, whatever produced it.

Kept faithfully, because the properties turn on them:

- A node writes its **own** durable row after it actually starts an agent and removes it
  after a stop; a graceful shutdown cascades the node's rows away with its node row. An
  overload death folds its stale-eject into the same departure (as the sibling specs fold
  ejection hysteresis).
- Capacity **rides the heartbeat**: the node re-advertises on every sync, the store
  treats a change like any durable change (the leader gets ticks to shed or backfill),
  and the monitor's capacity assertions read the node's own announcements — the source of
  truth — so nothing upstream can satisfy them with a stale number.
- The leader's placement runs on a **snapshot** (the sync reply), so every decision can be
  stale by one round trip — the churn slack, the trip slack, and the healer all exist
  because of this.
- The **GH-3698 pending-assignment ledger**: a dispatched start is held (and counted
  toward its node's share, so the distribution balances around it) until it is confirmed
  by a visible row or its destination leaves; on hold expiry the start is **re-driven to
  the same node** — deterministic re-dispatch is what keeps an unconfirmed start from
  spawning a rival copy elsewhere, and under capacity it is also what keeps a retry
  capacity-neutral. See the model-development notes: both halves of this were forced by
  counterexamples.
- The **GH-2602 duplicate healer**: two live row-holders for one agent, leader stops all
  but one. Leadership can move mid-rollout (the store grants it to a live syncer when the
  leader departs), and a new leader's empty ledger plus in-flight commands from the old
  one is exactly what makes the healer load-bearing here.
- Even distribution as strictly-improving single moves: one move per tick, only when
  max − min ≥ 2 and the target has headroom, so churn is observable and bounded.

Left out, deliberately: crashes and partitions (the [agent-assignment](../agent-assignment/)
spec's territory — membership changes here are graceful, and an overload death is modelled
as one); the advisory-lock election mechanics (the [leader-election](../leader-election/)
spec proves the single-leader invariant this spec assumes); blue/green capability matching
and database-affine / group-affinity placement (a flat capability space — GH-3959 is
explicit that `DistributeByGroupAffinity` itself is right, it is only capacity-blind);
command batching (`AgentStartBatchSize` — real, but orthogonal). Capacity is a **count**
ceiling, not a resource model: "how many agents is too many" is the operator's number
(GH-3959's own proposal), and the model treats it as opaque. The stability oracle is a
deploy-bracketed bit rather than a wall clock — see the honesty notes.

## Running it

From the repo root, `nix develop` provides `dotnet` and `p`. Then:

```
cd formal/rolling-deploy
p compile --pfiles RollingDeployModel.p RollingDeploySpec.p RollingDeployTest.p --projname RollingDeploy --outdir .
p check -tc tcRollingDeploy -s 10000
p compile ... --mode pex
p check --mode pex -tc tcRollingDeploy -s 1000000
```

The cases (`begin(nodes, agents, k, capacity, replaced, joins, removed, wedges)`):

| Case | Shape | What it makes load-bearing |
| --- | --- | --- |
| `tcSteadyState` | 3 nodes × cap 2, 4 agents | baseline placement within ceilings |
| `tcWedge` | 2 × cap 2, 2 agents, 1 wedge | the reconcile sweep (GH-3987's trap) |
| `tcScaleOut` | 3 × cap 2, 4 agents, +1 node | the post-deploy rebalance |
| `tcRollingDeploy` | 3 × cap 2, 4 agents, replace all 3 | the stability gate, healer, leadership handover |
| `tcMiniDeploy` | 2 × cap 2, 2 agents, k=2, replace 1 | mid-rollout availability (shallow quiet windows — see the gate-everything mutant) |
| `tcDeployChaos` | 3 × cap 2, 4 agents, replace 2, 1 wedge | everything at once |
| `tcCrunchRecover` | 2 × cap 2, 3 agents, −1 node then +1 | waiting without headroom, then recovery when capacity returns |
| `tcCascade` | 3 × cap 2, 5 agents, −1 node | the GH-3959 shape: survivors cannot absorb the share; one agent waits, nobody dies |
| `tcSqueeze` | 2 × cap 2, 3 agents, 1 squeeze | dynamic capacity: shed to the shrunken ceiling, displaced agent re-placed |
| `tcSqueezeRecover` | 2 × cap 2, 4 agents, 1 squeeze, +1 node | shrink forces one agent to wait; the join must recover it |

Run both PEx and the random bugfinder — they catch different bugs (see
[`../README.md`](../README.md)) — and use `-s`, not `-i`. Practical note: mutant runs can
livelock by construction (that *is* the counterexample) and then eat unbounded memory in
default mode; run them with `-m 6` (a memory cap) and, when comparing against a pristine
control, the same `-ms` step bound on both — a truncated schedule can report spurious
liveness, so a mutant liveness verdict only counts against a clean control at identical
bounds. Never run two checker processes concurrently on a development machine; the full
suites below were run strictly sequentially after two host-level OOM kills proved the
point.

## Results

Random bugfinder, 10k schedules per case, and PEx with a 1M-schedule budget per case
(no bugs; deep bounded searches — the space did not close):

| Case | Random 10k | PEx 1M budget |
| --- | --- | --- |
| `tcSteadyState` | no violation | no violation (bounded) |
| `tcWedge` | no violation | no violation (bounded) |
| `tcScaleOut` | no violation | no violation (bounded) |
| `tcRollingDeploy` | no violation | no violation (bounded) |
| `tcMiniDeploy` | no violation | no violation (bounded) |
| `tcDeployChaos` | no violation | no violation (bounded) |
| `tcCrunchRecover` | no violation | no violation (bounded) |
| `tcCascade` | no violation | no violation (bounded) |
| `tcSqueeze` | no violation | no violation (bounded) |
| `tcSqueezeRecover` | no violation | no violation (bounded) |

Coverage confirms the interesting paths are exercised rather than passing vacuously: in
`tcDeployChaos` a running agent receives the wedge mid-tick, nodes receive shutdowns
mid-await, the store processes deploy-begin/end, and the monitor observes rebalance moves;
`tcScaleOut` drives the committed rebalance path; `tcRollingDeploy` exercises leadership
handover and the healer; and in `tcCascade` and `tcDeployChaos` the monitor observes both
**shed** events and a genuine **overload trip** — a node really does die over capacity in
some pristine schedules, absorbed by the cascade-floor slack of one, so that property is
tested by live ammunition, not hypothetically. The capacity-crunch cases settle with an
agent legitimately waiting (structurally forced: 5 agents cannot fit in 4 slots), so the
waiting branch of the monitor is exercised on every schedule of `tcCascade`.

## What the mutants say

Each is the committed model with one thing removed (a literal replace on a pristine copy;
see the ledger table below). Percentages are buggy schedules in a 5–20k random run.

- **The reconcile sweep** (fix 2) — **violated** on `tcWedge`, 100%:
  `agent 1 is assigned in the durable table (node 1) but is not running anywhere` —
  GH-3987's exact trap, as a settled safety violation.
- **The stability gate** (fix 1, `if (stable)` → `if (true)`) — **violated** on
  `tcRollingDeploy`, 50%:
  `2 rebalance moves were dispatched while membership was in flux`. Rebalancing eagerly on
  every pod event is the reassignment storm; at 13.5k agents it is 24k records.
- **The gate applied to everything** (urgent placement also debounced) — **violated** on
  `tcMiniDeploy` (0.4% — the window is a fully-drained cluster mid-step):
  `agent 1 is not running anywhere while node 2 still has headroom — it should have been
  placed`. The failure mode of the *naive* debounce: a terminated pod's agents stay down
  for the whole rollout. This mutant is why the proposal gates only rebalance moves.
- **The capacity ceiling AND shed together** (`no-capacity` — i.e. **today's shipped
  design**) — **violated** on `tcCascade`, 100%:
  `node 1 settled with 3 agents but its capacity is 2 — the GH-3959 overload, at rest`.
  The dead node's share is forced onto survivors past their ceilings and the cluster comes
  to rest in the overloaded state that killed the fleet in production.
- **The ceiling alone** (placement unbounded, shed retained) — **violated** on
  `tcCascade`, 100%, as **liveness**: the monitor never leaves its hot state, against a
  clean pristine control at identical `-ms` bounds. The shed keeps pulling the excess off,
  the unbounded placement puts it right back, forever — GH-3959's "a cluster under memory
  pressure cannot converge", verbatim.
- **The shed alone** (ceiling retained) — **violated** on `tcCascade`, 100%:
  `node 2 settled with 3 agents but its capacity is 2`. During the scale-down the leader
  places an orphan off a snapshot that predates a still-in-flight row write, overshooting
  a survivor by one. The kill-line margin makes that survivable; the shed is what
  *repairs* it — without the shed, the raced overshoot becomes the resting state.
- **Capacity treated as a join-time constant** (`stale-caps`: the store ignores
  re-advertisements) — **violated** on `tcSqueeze`, 100%:
  `node 1 settled with 2 agents but its capacity is 1`. The monitor reads capacity from
  the node's own announcements, so a leader planning against the number a node advertised
  when it joined leaves the pressured node overloaded at rest. This is the executable
  argument for advertising capacity on the heartbeat rather than persisting it once.
- **The GH-2602 healer** — **violated** on `tcRollingDeploy` (2%):
  `settled with 2 nodes running agent 3`. Leadership handover mid-rollout plus the old
  leader's still-in-flight start double-places an agent; without the healer both copies
  run forever.
- **The rebalance branch** — **violated** on `tcScaleOut`, 100%:
  `membership is stable but the settled load is uneven: node 1 holds 2 while another node
  with headroom holds 0`. The gate defers rebalancing; this shows deferral isn't quiet
  deletion — the deploy-end refill makes the leader actually do it.
- **The ledger hold expiry** — **history, not currently reproduced.** On the first model
  revision its absence was caught by PEx as safety (`settled with 0 nodes running`: a
  stale rebalance stop crossed a fresh start, consumed it without a row, and the
  unconditional hold parked the agent forever); on the second revision random mode caught
  it as liveness (the leader polls the stuck entry forever). On the final model, 20k
  random schedules on each of `tcWedge` and `tcScaleOut` do **not** reproduce it: the
  same-node re-drive removed the only reachable trace (a stop and a later start crossing
  on the *same* node no longer co-occur under capacity-aware targeting). The expiry is
  retained anyway, because the real ledger's TTL re-decide is *not* same-node-only
  (`applyPendingAssignmentsLocked`'s `PendingRetryDue`), so in the implementation it stays
  load-bearing.

### The checker earned its keep four times while the model was being built

All four are design lessons, recorded because an implementation will meet them too:

- **The unconditional ledger hold** (found by PEx; 3k random schedules had missed it). A
  stale `StopRemoteAgent` from a rebalance detach crossed a *newer* start on the same
  node: the stop landed after the fresh start, stopped it, removed its row — and a hold
  with no expiry then parked the agent's placement forever. The real GH-3698 ledger
  already has the TTL + dispatcher probe; the model omitted it as an apparent
  simplification and the checker demanded it back.
- **Re-drives must not spawn rivals.** The first expiry re-decided placement from scratch,
  like a bare `AssignAgent` to a second node — exactly the pre-GH-3698 bug — and under
  capacity it was worse: the pristine model **violated its own cascade-floor property**
  (two nodes died over capacity in one run) because every re-decide could stack an extra
  copy onto a full node. The fix is the agent-assignment spec's own rule, now doubly
  load-bearing: deterministic re-dispatch **to the same node**, idempotent there and
  capacity-neutral. The real re-decide path is stop-then-start (`ReassignAgent`), which is
  the other valid shape; a bare re-assign elsewhere is not.
- **A ceiling is not a kill line.** With "dies at cap+1" semantics the pristine model
  tripped nodes on ordinary snapshot races and could not hold the cascade floor. The
  incident's own numbers say the ceiling is provisioned with margin below the real limit;
  modelling that margin (kill line = cap + 1) is what makes "at most one raced death" a
  theorem instead of wishful thinking — and it yields concrete operator guidance: set
  `MaxAgentsPerNode` with at least one batch of slack below what actually kills the node.
- **A fault the design leaves unhealed must settle, not hang.** The wedge was first
  modelled as an open monitor fault; the no-reconcile mutant could then only be caught as
  a liveness hang — invisible to PEx. Remodelled so the wedged cluster *settles* into the
  bad state, the same mutant is a plain safety assertion in 100% of schedules.

### Reproducing the mutants

Each mutant is one literal replacement applied to a pristine copy (two for `no-capacity`,
which removes one *feature*) — verify the replacement applied and the file still compiles,
then run the case listed above. On `RollingDeployModel.p`:

| Mutant | Replace | With |
| --- | --- | --- |
| no-reconcile | `if ((id in rows[a]) && rows[a][id] && !running[a] && !stopping[a]) {` | `if (false) {` |
| no-gate | `if (stable) { /* GH-3987: rebalance only once membership has settled */` | `if (true) {` |
| gate-everything | `target = minLoadedWithHeadroom(loads, caps);` (urgent branch) | `target = 0; if (stable) { target = minLoadedWithHeadroom(loads, caps); }` |
| no-healer | `if (sizeof(cl) >= 2) {` | `if (false) {` |
| no-rebalance | `if (maxN != 0 && minN != 0 && loads[maxN] - loads[minN] >= 2 && (minN in caps) && loads[minN] < caps[minN]) {` | `if (false) {` |
| no-ceiling | `target = minLoadedWithHeadroom(loads, caps);` | `target = minLoaded(loads);` |
| no-shed | `if (shedNode != 0) {` | `if (false) {` |
| no-capacity | both `no-ceiling` and `no-shed` replacements | — |
| stale-caps | `if ((m.id in caps) && caps[m.id] != m.cap) {` | `if (false) {` |
| no-ledger-expiry | `live[pending[a]] && pendingAge[a] < 2) {` | `live[pending[a]]) {` |

## Mapping the model changes onto the implementation

This is the proposal part. Model constructs on the left, code on the right.

### Fix 1 — gate rebalance moves on membership stability

- A new `DurabilitySettings.AssignmentStabilityWindow` (`TimeSpan`; `Zero` = today's
  behavior, so it ships compat-safe; a useful value is a bit above the rollout's
  pod-replacement cadence, e.g. 30–60s, and must exceed `CheckAssignmentPeriod` — a
  control loop must not act faster than its telemetry refreshes, a constraint Orleans
  validates in config for the same reason).
- `NodeAgentController` keeps a membership fingerprint (the sorted node ids + capability
  sets from each `LoadNodeAgentStateAsync` snapshot). Any change — join, eject, leave,
  capability shrink — stamps `_lastMembershipChange`.
- `EvaluateAssignmentsAsync` computes
  `stable = now - _lastMembershipChange >= AssignmentStabilityWindow` and passes it to the
  grid. When **not** stable, the distribution must not *move* an agent that is running
  (`Agent.OriginalNode != null` and live): it keeps the agent where it is, the way
  `applyPendingAssignments` already pins in-flight placements. Everything else runs
  untouched: placement of agents with **no live owner**, the GH-2602 healer's stops, the
  capacity shed, operator restrictions (pause/pin), and stops for nodes that left. The
  gate-everything mutant is the executable argument for that boundary.
- The stability oracle in the model is a deploy-bracketed bit rather than a clock (a P
  model has no wall time). What the model checks is that *gating on it* preserves
  convergence, availability mid-rollout, and evenness afterwards — the churn property's
  slack of one move is precisely the window-detection race the clock version has too.
- This also delivers most of GH-3987 ask 2 (churn capping): mid-rollout evaluations only
  re-place orphans, so the storm collapses to one post-rollout rebalance, chunked by the
  existing `AgentStartBatchSize` batching.

### Fix 2 — reconcile assigned-vs-running on every node's tick

- In `DoHealthChecksAsync`, in the follower-capable section next to
  `ReportFailedLocalAgentsAsync` (a follower must be able to heal its own agents): the
  tick already loads the node-state snapshot; take this node's own persisted
  `ActiveAgents` — its `wolverine_node_assignments` rows — and diff them against the
  in-memory truth (`Agents` / `AllRunningAgentUris()`).
- For a row with no locally running agent, re-drive `StartAgentAsync(uri)` — already
  idempotent, already evicts wedged registrations (GH-3519), already re-upserts the row
  (GH-3604/D4) — **skipping any agent in `_stoppingAgents`** or with a stop revocation in
  flight, which is the model's `stopping` guard: a reconcile that races a stop
  manufactures the one-row-two-copies residue GH-4240 closed. A row for an agent this node
  can no longer host (unknown family, restriction) is removed instead.
- This is GH-3987's ask 3 verbatim — the part the reporter "cannot work around ourselves,
  because from the outside the table looks correct" — and it also covers the GH-3698
  symptom the same way.

### Fix 3 — the capacity ceiling (GH-3959), with a pluggable, dynamic number

The plane mechanism and the capacity *number* are separate concerns, and the model checks
the mechanism against a **moving** number, which licenses both providers below.

- The mechanism: each node **advertises** its capacity on its heartbeat (a column next to
  capabilities on `wolverine_nodes`, refreshed by `MarkHealthCheckAsync`); the grid reads
  the per-node advertised values; a change is treated like any node-state change (the
  next evaluation sheds down to a shrunken ceiling or backfills waiting agents into new
  headroom). The `stale-caps` mutant is the executable argument for re-reading it every
  evaluation rather than caching it at registration; the `tcSqueeze`/`tcSqueezeRecover`
  cases prove the plane converges while the number moves.
- **Static provider** — `DurabilitySettings.MaxAgentsPerNode` (`int?`, `null` = today's
  behavior), exactly the issue's proposal: every node advertises the configured constant.
  Simple, predictable, and right when per-agent cost is roughly uniform.
- **Dynamic provider** — the static count has a real failure mode: per-agent cost is not
  constant. In the GH-3959 incident an idle agent and a backlog-working agent differed by
  several-fold (their measurements: ~1 MB vs ~4.4 MB) — the exact ratio won't generalize,
  but the shape will: a cap sized for idle agents overflows the node exactly when a
  rebuild makes every agent expensive.
  A pressure-based provider fixes this *without touching the plane*: on each heartbeat the
  node computes its advertised slots from measured memory pressure —
  `GC.GetGCMemoryInfo()` gives `MemoryLoadBytes` against
  `HighMemoryLoadThresholdBytes`/`TotalAvailableMemoryBytes` and is **container-aware**
  (it reads the cgroup limit, i.e. the very GC hard limit that killed the GH-3959 fleet) —
  roughly:
  `advertised = runningAgents + freeBytesBelowHighWater / observedPerAgentCost`, where
  `observedPerAgentCost = workingSet / max(1, runningAgents)` — a live estimate that
  automatically inflates during a backlog and deflates when caught up. Two guards make it
  safe, both from Orleans's overload machinery (see prior art):
  - **Asymmetric smoothing**: track pressure rises fast, let falls decay slowly, so a node
    just loaded stays unattractive for a while (Orleans's dual-mode Kalman filter on
    CPU/memory signals).
  - **Hysteresis**: shrink the advertisement at a high-water mark, grow it back only below
    a low-water mark, and only in whole steps — the gap must exceed one agent's cost, or
    placing an agent lowers the ceiling that then evicts it, forever. This oscillation
    risk is *endogenous* (the agent's own cost moves the ceiling) and lives entirely in
    the node-side translation; the model deliberately treats capacity movement as
    exogenous and checks that the plane converges for ANY sequence of advertised numbers
    — so the translation only owes eventual stabilization, which hysteresis provides.
  - Clamp to `[MinAgentsPerNode, MaxAgentsPerNode]` so a misbehaving signal cannot
    advertise zero fleet-wide or unbounded growth.
- **Provision the margin either way**: the ceiling must sit at least one start-batch below
  the load that actually kills the node, because a ceiling cannot recall a command on the
  wire. The model's kill-line-vs-ceiling gap and its "at most one raced overload death"
  property are the formal version of this sizing rule.
- In `AssignmentGrid`, capacity is the node's **whole** load — all schemes, all families —
  not the pass being distributed (the GH-3959 prototype's own correction: a node carrying
  a thousand agents of another scheme must not look empty to the next family). Every
  placement fallback that today force-assigns regardless of load
  (`?? ordered.FirstOrDefault(...)`-style) instead leaves the agent **unassigned** when no
  candidate has headroom; `FindDelta` already emits `StopRemoteAgent` for an unassigned
  running agent and nothing otherwise, so "leave it unassigned" needs no new machinery —
  as the prototype found.
- **Unassigned-due-to-capacity must be loud**: a warning log and a metric (the issue's
  reporter asks for per-node assigned-agent counts as a metric too — the model's monitor
  literally asserts on that number, which is the strongest argument it should be
  observable). The model's "waits only when NO node has headroom" property is the
  correctness contract for this state; the "placed as soon as capacity appears" behavior
  falls out of the existing evaluation loop re-running (deploy end, node join, row
  removal all already trigger re-evaluation).
- **The shed**: `EvaluateAssignmentsAsync` finds any node whose persisted row count
  exceeds its ceiling and detaches the excess (a `StopRemoteAgent`; one batch per
  evaluation), letting the urgent path re-place it within ceilings or park it. The no-shed
  mutant shows why this is not optional: an in-flight start that lands on a node that
  filled up meanwhile otherwise becomes the *resting* state. The shed must skip paused and
  pinned agents (operator intent outranks it) and should prefer detaching agents another
  node can actually take.
- Rebalance-for-evenness only targets nodes with headroom, and "even" means even among
  nodes that *have* headroom — a full node next to a fuller node is not a reason to move
  anything (the monitor's evenness property encodes this).

### What must not change while doing it

The pending-assignment ledger's expiry (`PendingRetryDue`: dispatcher probe + TTL) stays,
and its re-decide must remain stop-then-start (`ReassignAgent`) or same-destination —
never a bare `AssignAgent` to a second node. The model violated its own cascade-floor
property when a re-drive could stack a rival copy onto a full node; GH-3698 fixed the same
shape in production for the duplicate-copy reason, and capacity adds a second reason.

## Prior art: how Orleans places grains

Orleans's placement plane (`src/Orleans.Runtime/Placement/` — the
`ResourceOptimizedPlacementDirector`, `ActivationCountPlacementDirector`, the activation
`Repartitioner`/`Rebalancer`, and `LoadSheddingOptions`) is the closest production
analogue, and it is worth being precise about what does and does not transfer.

**How Orleans detects overload** (the question a fixed count begs): dynamically, from
system statistics — a silo is overloaded when smoothed CPU exceeds `CpuThreshold` (95%)
**or** memory utilization exceeds `MemoryThreshold` (90%) (`LoadSheddingOptions`,
`OverloadDetectionLogic`), with the inputs smoothed by an asymmetric dual-mode Kalman
filter that tracks rises fast and decays falls slowly (`EnvironmentStatisticsProvider`),
republished every second. Overloaded silos are excluded from load-aware placement
candidates, and the default director's *score* is itself mostly live resources: CPU 40,
memory-in-use 20, available-memory 20, max-available 5 — activation count is only a
15-weight *relative* signal. Orleans also has opt-in memory-pressure activation shedding
(evict toward a 75% target above an 80% limit). All of that supports making Wolverine's
advertised capacity pressure-derived rather than fixed — while its delivery stays a
count, which is what a placement plane can plan with.

**The structural difference that justifies this design:** Orleans grains are lazily
re-activated on demand — a dead silo's grains re-place one call at a time, spread over the
traffic pattern, so "reassign 1/N of the fleet at once" never arises and Orleans ships
**no absolute per-silo capacity ceiling and no unassigned/waiting state at all**
(placement must return a silo or throw; under whole-cluster overload its default director
degenerates to picking `compatibleSilos[0]` — herding). Wolverine's agents are
leader-assigned and must run continuously; the damping Orleans gets free from laziness has
to be built explicitly here. That is the ceiling, the waiting state, and the stability
gate.

**What corroborates this design:**

- *Exact in-flight accounting beats heuristics.* Orleans's count-based director inflates
  its local estimate of a just-chosen silo by the cluster size, because N independent
  placers herd on stale statistics. A single leader with a pending-assignment ledger
  (GH-3698) counts **exactly** — the model's held-pending load accounting is the strict
  form of Orleans's trick.
- *"Churn without measured improvement" means instability — stop, don't try harder.*
  Orleans's rebalancer runs in sessions that self-terminate after 3 stagnant cycles,
  precisely because a changing cluster makes balance-chasing counterproductive. A rolling
  deploy is a stagnation generator; their inference-based damping and this proposal's
  window-based gate are two implementations of the same judgment, and the window is the
  simpler one to reason about (and to model-check).
- *One exchange per node per recovery period; move volume ramps.* Their repartitioner's
  per-silo recovery period and the rebalancer's gentle-then-ramp move sizing validate the
  model's one-move-per-tick shape as a churn ceiling.
- *Control period ≥ telemetry period.* Orleans validates in configuration that its
  rebalance cycle is at least twice the load-statistics refresh; the analogous constraint
  here is `AssignmentStabilityWindow > CheckAssignmentPeriod`, and it deserves the same
  config-time validation.
- *A cautionary hole not to copy:* Orleans's rebalancer moves grains via placement
  *hints*, which every director honors **before** any overload check — so its own
  rebalancing traffic bypasses overload protection entirely. In this design all moves are
  detach-then-replace through the capacity-aware urgent path, so migration traffic cannot
  pile onto a full node by construction; keep it that way in the implementation.
- *Suspension hooks exist but nothing wires them to membership events* — Orleans leaves
  "don't rebalance during a deploy" to the operator. GH-3987's whole point is that the
  operator shouldn't have to; the stability gate automates it.

## Honesty notes

- The stability oracle is deploy-bracketed, not a clock; the wall-clock inference race is
  represented by the one-move slack, which the checker exercises.
- Capacity is delivered as a count, advertised per heartbeat; the model checks the plane
  against *exogenous* movement of that number (the squeeze) and growth via joins. What it
  does not model: endogenous cost-coupling (an agent whose own cost shrinks the ceiling
  that hosts it — the oscillation risk) and upward re-advertisement without a join; both
  live in the node-side pressure-to-slots translation, whose obligation to the plane is
  only that the number eventually stabilizes (hysteresis). A fleet-wide squeeze with no
  headroom anywhere parks agents — by design, and loudly.
- The kill line (cap + 1) is an abstraction of "the ceiling is provisioned with margin";
  a real fleet whose margin is smaller than one in-flight start batch can still die of a
  race, and no assignment-plane design can prevent that — only sizing can.
- The trip folds OOM-death-plus-stale-eject into one graceful departure; the eject
  protocol itself (hysteresis, leader protection) is the leader-election spec's job.
- Configurations are small (≤ 4 nodes live, ≤ 5 agents, budgets 2–3). PEx results are
  deep bounded searches, not closed spaces. "No bug found" is only as strong as the space
  covered.
