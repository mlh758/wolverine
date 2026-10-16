# Single-agent ownership, model checked

Does a leader-assigned singular agent end up running on exactly one live node? This is the
assignment plane the [leader-election spec](../leader-election/) deliberately leaves out.
See [`../README.md`](../README.md) for why these specs exist and how to run them.

Two facts have to agree in the end: the **durable assignment row** in the store
(`AddAssignmentAsync` / `RemoveAssignmentAsync`) and the **node-local fact** of actually
running the agent (`Agents[uri]`). Faults split them, and the interesting question is
whether they reconverge.

**They didn't, and now they do.** This model is faithful to the shipped RDBMS schema, and
on that schema the system *as shipped before 6.36* settled into permanent duplicates in two
fault scenarios — both committed below as **expected violations** (a bug ledger, the
mirror image of the mutant ledger). Since then two mechanisms shipped and are modelled as
per-test toggles: the node-side reconcile sweep (`ReconcileLocalAgentsAsync`, GH-3987 /
GH-4407, with its consecutive-tick observation threshold and its insert-if-absent claim)
and the takeover hold (GH-4897). The `*Shipped` cases run today's defaults and must
converge; the `Stock` cases keep the pre-6.36 behaviour checkable so the bug ledger stays a
regression test. The sweep and the claim shipped in V6.36.0 on 2026-09-10, one day after the
bug ledger below was measured; the takeover hold in V6.48.2. Model last synced to the code at
6.50.2 (`origin/main` 720a6c9c1, 2026-10-15).

## The schema is the story

An earlier revision of this model kept per-node assignment rows (`rows: map[node, bool]`),
which let the leader observe two owners at once and heal the extra copy (GH-2602). That
gave the model a convergence proof the real system does not deserve. The real table is:

```
id (agent uri)  PRIMARY KEY     -- one row per agent, ever
node_id         FK -> nodes ON DELETE CASCADE
```

with `AddAssignmentAsync` an unconditional last-writer-wins upsert and
`RemoveAssignmentAsync` conditional (`WHERE node_id = :me`). Consequences, all modelled:

- The store can **never represent two owners**, so `grid.DuplicateAgentReports` — the
  GH-2602 healer's only input on an RDBMS store — is structurally unreachable. The healer
  is *absent from this model* because it is dead code against this schema. A duplicate's
  losing row isn't deleted; it is silently overwritten, and the table reads immaculate.
- What actually keeps ownership single on the happy path is the **pending-assignment
  ledger** (GH-3698): leader-local, in-memory, retired when the row lands. Modelled as
  `pendingTarget`: while an entry is live the leader re-drives the *same* node instead of
  re-choosing. Crucially, **the ledger dies with the leader** — a new leader starts empty.
- A node ejection cascades away its assignment rows; a healed node's GH-3604/D2
  re-register **re-upserts** its rows, trampling whatever a peer wrote in the meantime.
- `TryClaimAssignmentAsync` (GH-4407) is the one write that is **not** last-writer-wins:
  `INSERT ... ON CONFLICT DO NOTHING`, used by the sweep to restore a runner's claim only
  when nobody holds the row.

## What shipped, and how it is modelled

| Code | Model |
| --- | --- |
| `ReconcileLocalAgentsAsync` (GH-3987 / GH-4407): on every node's tick, compare my assignment rows with the agents I run; act only on a divergence seen `LocalAgentReconciliationThreshold` (3) ticks in a row; a row naming me with nothing running → start; running here while a live peer's row names it → stop my copy and leave the row; running here with no row anywhere → `TryClaimAssignmentAsync` | `sweep` + `threshold` on `Node`: a `streak` counter per tick, the three actions, `eClaimRow` for the insert-if-absent claim. A divergence under observation refills the tick budget, as the real loop never stops |
| `LeaderTakeoverHoldEvaluations` (GH-4897): for its first evaluation a new leader does not place an agent that has no row and nothing pending, unless the agent is an orphan of a node that left | `hold` on `Node`: `evalsSinceElection` reset on election; the store's `orphan` flag (set when an ejection removes the owner's row, cleared when any row lands) is what the real `OrphanedByStaleNodes` approximates. The real hold applies only when some *other* node holds assignments; with one agent the model assumes that, which is the conservative direction for the duplicate property |
| `MaxLocalAgentReconciliationsPerTick`, the per-tick cap | not modelled: one agent |
| A stop stamped after the snapshot wins over the sweep's start (`snapshotSequence`) | not modelled: no concurrent stop source besides the leader's placement |

## The bug ledger — committed counterexamples

Run these expecting red; the violation *is* the result. Both reproduce shapes measured on
live clusters by the wolverine-deploy-sim rig (RESULTS.md 2026-09-09: duplicates on ~24%
of rolling deploys at defaults, never self-healing, table always immaculate) **on 6.35**.
With the shipped sweep and hold (the `*Shipped` cases) both converge.

**(a) `tcLeaderHandoverStock` — the deploy-sim duplicate.** The leader dispatches a start
and is killed at that instant (the rolling deploy terminating the leader pod, arsonist
kind 2). The started node's row write is still in flight when the new leader — whose
ledger is empty by construction — evaluates: the agent looks unplaced, so it places it
again, free to pick a different node. Both copies run; the single row names whichever
wrote last; nothing ever notices. Checker: `settled with 2 nodes running the single agent`.
The found trace is the observed mechanism exactly — in one schedule the new leader is
itself the node that just started the agent (the sim's `48ww4` trace), re-placing its own
unrecorded copy onto a peer.

**(b) `tcPartitionHealStock` — GH-2602 residue, unhealable on this schema.** The owner is
cut off; the leader places a replacement; the healed original's D2 re-register re-upserts
its row over the replacement's. Two copies run, one row. On the per-node-rows schema the
healer would now see both and stop one; on the real schema it cannot exist. Checker:
`settled with 2 nodes running the single agent`.

## What is modelled

| Model | Real thing |
| --- | --- |
| `Store` | the agent's ONE `node_assignments` row (PK = agent uri, upsert), the live-node set, and the leader identity, serialized the way the database serializes them |
| `Node` | one process: its health-check tick (leader: evaluate placement under the takeover hold; any node: the reconcile sweep when enabled) and the local fact of running the agent |
| `pendingTarget` | the GH-3698 pending-assignment ledger for one agent: leader-local, in-memory, retired when the row lands, dropped on demotion, **lost at leader death** |
| `sweep` / `threshold` (toggles) | the GH-3987 / GH-4407 node-side reconcile: after `threshold` consecutive sightings, stop my copy if the row names another live node, claim the row (insert-if-absent) if nobody owns it, start my copy if the row names me and nothing runs here |
| `hold` (toggle) | the GH-4897 takeover hold: a new leader withholds a bare first-time placement for `hold` evaluations, orphans excepted |
| `RunCourier` | an `AssignAgent` start command in flight to a node |
| `Arsonist` / `StrikeCourier` | kind 0/1: partition/crash whoever is *currently* running the agent; kind 2: crash the LEADER at the moment it dispatches a start |
| `HealCourier` | the partition ending |
| `eRefill` | the health-check loop running forever: faults and durable changes refund tick budgets so nodes keep polling until the cluster is clean |

Faults hit the current owner (or the acting leader), not a fixed node up front, because
that is the interesting case: a fault delivered before the agent is placed just hits an
idle node, and a FIFO mailbox would order a pre-injected cutoff ahead of the start that
would make the node run.

Kept faithfully, because the properties turn on them:

- A node writes its **own** durable row after it actually starts the agent
  (`StartAgentAsync → upsertAssignmentAsync`) and removes it after it stops — with the
  remove conditional on still being the named owner, as the real SQL is. The
  start-then-persist order is the window the handover bug lives in.
- Placement is a **nondeterministic choice** among live nodes when no ledger entry holds —
  which is what the real even-spread distribution looks like from one agent's seat, and is
  what lets a new leader re-place an in-flight agent somewhere else.
- The **D2 re-register** fires once, on heal — not every tick. (The earlier model's
  every-tick "resurrection" was the unfaithful detail that made per-node rows look
  healable.)
- The sweep's stop side deliberately does **not** fire on `owner == 0` — my own row write
  may be the thing in flight — that case is a *claim* instead, and only insert-if-absent;
  the start side is what heals the row-without-runner state the stop side can leave when
  it races this node's own start. All three are load-bearing (see the mutants).

Left out, deliberately: multiple agents and even distribution (this is one singular
agent); command batching and reply windows (GH-3604/D3); blue/green capability matching;
the advisory-lock election mechanics (the sibling spec's job — leadership moving safely is
*assumed* here). A partition is a node excluded from the live set while it keeps running.

## Running it

From the repo root, `nix develop` provides `dotnet` and `p`. Then:

```
cd formal/agent-assignment
p compile --pfiles AgentOwnershipModel.p AgentOwnershipSpec.p AgentOwnershipTest.p --projname AgentOwnership --outdir .
p check -tc tcLeaderHandoverStock -s 5000      # expect the committed violation (pre-6.36 behaviour)
p check -tc tcLeaderHandoverShipped -s 20000   # expect clean (today's defaults)
```

`begin(nodes, partitions, crashes, leaderKills, sweep, threshold, hold, epochMode)`: the
`Stock` cases are `sweep = false, hold = 0`; the `Sweep` cases `sweep = true, threshold =
1, hold = 0` (the sweep acting on first sight, as the original GH-4297 proposal had it);
the `Shipped` cases `sweep = true, threshold = 3, hold = 1`; `tcLeaderHandoverHold` is
`sweep = false, hold = 1` — the hold on its own.

Run both PEx and the random bugfinder — they catch different bugs (see
[`../README.md`](../README.md)) — and use `-s`, not `-i`.

## Results

Default-mode random bugfinder on the revision synced to 6.50.2 (`-m 1`); 20k schedules
per case unless noted:

| Case | Sweep / threshold / hold | Result |
| --- | --- | --- |
| `tcSteadyState` | off / – / 0 | no violation |
| `tcCrash` | off / – / 0 | no violation |
| `tcPartitionHealStock` | off / – / 0 | **VIOLATED (expected)** — `settled with 2 nodes running`, 100% of schedules |
| `tcPartitionHealSweep` | on / 1 / 0 | no violation |
| `tcLeaderHandoverStock` | off / – / 0 | **VIOLATED (expected)** — `settled with 2 nodes running`, 1.27% at first hit |
| `tcLeaderHandoverSweep` | on / 1 / 0 | no violation |
| `tcChaosSweep` (partition+crash+leader kill) | on / 1 / 0 | no violation |
| `tcLeaderHandoverHold` — the hold alone | off / – / 1 | no violation (see below) |
| `tcSteadyStateShipped` | on / 3 / 1 | no violation |
| `tcCrashShipped` | on / 3 / 1 | no violation |
| `tcPartitionHealShipped` | on / 3 / 1 | no violation, 10k schedules |
| `tcLeaderHandoverShipped` | on / 3 / 1 | no violation, 10k schedules |
| `tcChaosShipped` | on / 3 / 1 | no violation, 10k schedules |

The three epoch cases were not re-run on this revision (their mechanics are untouched by
it). The full pass — `formal/check-all.sh`, with the mutants and bounded PEx budgets — was
cut short by host memory pressure on 2026-10-15; the shipped cases above are the 10k
foreground runs that replaced it, and PEx has not been run on this revision at all.

**On the hold alone.** `tcLeaderHandoverHold` asks whether GH-4897's one-evaluation hold,
with no reconcile sweep, closes the handover duplicate. The checker did not find the
duplicate in 20k random schedules. That is weaker than it looks: the duplicate needs the
predecessor's start to land on its node *after* the new leader's second evaluation, which
in this model is one extra store round trip, and the stock case already needed the racy
interleaving (1.27% at first hit). It is not a proof that the hold suffices — it is the
one-cycle timing heuristic the code comments say it is, and a PEx run on this case is the
right next step before relying on the result either way.

The two stock violations differ tellingly in rate: the partition-heal duplicate is forced
(every schedule loses), while the handover duplicate needs the racy interleaving — the row
write still in flight when the new leader's first evaluation reads its snapshot — which is
exactly why the real bug shows up on *some* rolling deploys (~1 in 4 at defaults on the
deploy-sim rig) rather than all of them.

PEx (systematic), 100k schedules on the five passing cases: no violations
(`partially correct` — deep bounded searches; the space did not close).

All violations are **safety** counterexamples at quiescence — the bad state settles and
the monitor's convergence assertion names it — not hot-state liveness, so they reproduce
under PEx too.

## What the mutants say

Each is the committed model with one mechanism removed; each must produce its predicted
counterexample, or the passing cases above are vacuous:

- **The pending ledger** (`pendingTarget` re-drive; the leader re-chooses every tick) —
  **violated on `tcSteadyState`**, no faults needed: while the first start is in flight
  the leader re-places the agent on another node, and with the sweep off the two copies
  settle. `settled with 2 nodes running`. This is GH-3698's job, and why the ledger dying
  with the leader (bug ledger a) matters. (Earlier revision.)
- **The sweep's stop side** — on the shipped configuration, **violated on
  `tcLeaderHandoverShipped`** as a *liveness* bug: the handover duplicate forms, the
  divergence is observed every tick, and the node keeps ticking forever because the sweep
  that would act on it never does. (On the threshold-1 revision it was a safety violation,
  `settled with 2 nodes running`; with the observation streak the unacted divergence is
  unfinished work by construction, so it surfaces as a hang. Same defect, same ledger.)
- **The sweep's start side** — **violated on `tcPartitionHealShipped`**, likewise as
  liveness: the healed node's re-upsert lands after it swept its own copy away, leaving a
  row naming a non-runner, and with no start side the divergence is observed forever.
  This is the model's argument that the sweep must ship both halves.
- **The sweep's claim side** (a runner with no row never restores its claim) — **not
  falsified** on `tcPartitionHealShipped` or `tcChaosShipped` at 5k schedules. In these
  cases a runner with no row is always a node whose own D2 re-register re-upserts the row
  anyway, so the claim never gets to act. The claim exists for the GH-4240 residue — a
  stop's delete racing a restart's upsert — which this single-agent model has no second
  stop source to produce. Honest status: modelled, not exercised.
- **The takeover hold applied to orphans too** — **not falsified** on `tcCrashShipped`.
  Holding an orphan delays its placement by one evaluation; the spec checks what the
  cluster settles into, not how fast, so a one-tick delay is invisible here. The orphan
  exemption is a latency rule (the GH-4897 compliance tests pin it on a ten-minute
  heartbeat), and a latency property is the way to make it load-bearing.
- **Placement of an unowned agent** (the leader never assigns) — **violated on `tcCrash`**:
  the sole owner dies and the agent never runs again. `settled with 0 nodes running`.
  (Earlier revision.)

Model-authoring lessons kept from earlier revisions, all found by the checker: the store
writing the row at dispatch let a row outlive its runner; announcing a death before opening
its fault let the monitor read a false quiescence; and two fault-accounting races in this
revision (a partition strike landing on an already-dead node, whose cutoff a Dead node must
retire like any other stray command). Same lesson every time: tell the monitor about the
work still owed before you announce the step that looks like rest.


## The leader-epoch proposal, as an executable variant

`epochMode` models the design the duplicates keep pointing at: the **leader** writes the
assignment row, stamped with a leadership epoch (a virtual clock bumped on every handover),
and a node may not start anything until it **wins a confirmation on that row** — "mark me
confirmed where this agent's row still names me, at the epoch I was told". The two writes
that make stock lose are gone: there is no unconditional `AddAssignmentAsync` upsert, and no
start that happens before the store has agreed to it.

What that buys, mechanically:

- a command that outlives its assignment loses the compare and starts nothing, so an
  unreachable node waking up late cannot pop a stale message and run a second copy;
- a former leader that rejoins after a partition writes under an epoch the row has moved
  past, so its assignments are ignored rather than obeyed;
- the leader can take a claim back from a node it believes is gone, and know that the node
  cannot quietly resume — taking the row back *is* what makes the old node's check-in fail.

### Results (30k random schedules, `-m 1`, revision of 2026-10-16)

| Test case | Stock | Sweep | Epoch |
| --- | --- | --- | --- |
| `tcLeaderHandover*` | **violation** (bug ledger (a)) | 0 bugs | **0 bugs** |
| `tcPartitionHeal*` | **violation** (bug ledger (b)) | 0 bugs | **0 bugs** |
| `tcChaos*` | — | 0 bugs | **0 bugs** |

The epoch column went green on 2026-10-16 after three more design requirements were found
and modelled (below). Before them the same cases read: handover 0 bugs, partition heal a
rare strand (0.01%), chaos a strand (0.54%) — two different stranding paths and, once those
were closed, a rare duplicate (0.02%). Each is recorded because an implementation will meet
it too.

**The handover duplicate is gone.** That is bug ledger (a) — the deploy-sim shape, the leader
dying with a start in flight and its pending ledger, the new leader re-placing the agent, the
one-row table reading immaculate throughout — and it is the case the epoch is aimed at. It
goes green.

### What the model found in the proposal

Writing the row up front makes it a *claim* rather than a fact, and the first cut of this
variant deadlocked on that: the leader wrote a row for a node that became unreachable before
the command landed, the node never checked in, and nothing ever took the row back. The
checker reported `settled with 0 nodes running the single agent` — not a duplicate, a
**stranded agent**. So the proposal needs a rule stock does not: *a row whose node is not
live is not a placement*, and the leader must be free to re-stamp it. That is modelled now
(`unplaced` under `epochMode`), and it is safe for exactly the reason the epoch exists — the
re-write is what invalidates the old node's check-in.

Anyone implementing this should treat that as part of the design, not an afterthought: an
optimistic claim needs a reclaim rule, or the fix for duplicates buys an outage instead.

### The stamp has to fence the reply, not just the write

The rare partition-path violation took 300k schedules to catch (`0.00% buggy`) and is the
second design requirement this variant surfaced:

```
ConfirmCourier(13) -> eConfirmReply (ok:True,  epoch:2)   node wins, starts the agent
ConfirmCourier(12) -> eConfirmReply (ok:False, epoch:0)   stale answer, arrives second
                                                          -> the node stops what it owns
```

A node that has won a check-in at epoch 2 receives the answer to an *older* attempt and
treats it as "you are an orphan". The result is `settled with 0 nodes running the single
agent` — the design trading a duplicate for a stopped agent.

So the epoch is not only for the write path. **A check-in result for any attempt other than
the one a node is on must be ignored**, or a late rejection stops a healthy agent. The same
holds for anything else that can answer late — an explicit decline under capacity-based
assignment most of all, since rejection makes the unconfirmed window a routine state rather
than a rare one, and a decline that arrives after the leader has re-stamped the row back to
that node would otherwise clear a live claim.

### An attempt is the (epoch, version) pair, and every message carries both

Found by `tcChaosEpoch` on 2026-10-16 (0.54% of schedules): a node on attempt (epoch 3,
version 2), which it had already won, sent its post-partition re-assert stamped with the
epoch of its *previous* win and the *current* version. The store refused it on the epoch;
the reply passed a version-only fence because the version matched; the node read the
refusal as "you are an orphan" and stopped a healthy agent (`settled with 0 nodes running`).
The version does not identify an attempt on its own. Modelled: `applyRun` adopts the
command's epoch together with its version, and `applyConfirm` ignores any reply whose
`(epoch, version)` is not the attempt the node is on.

### An unconfirmed row is a debt the leader keeps paying

Found by `tcChaosEpoch` after the fence fix (0.01%): a partitioned node's last in-flight
heartbeat lands after the cutoff, so the store counts it live; the leader re-stamps the
agent to it; the command is lost to the unreachable node; on heal the node's re-assert at
its old stamp loses and it stops. The row now names a live node at a version that node
never received, unconfirmed, and nothing ever sends it again. A row that is *placed but
unconfirmed* is not a finished placement: the leader must **re-drive the command at the
row's current stamp** (idempotent at the node) on every evaluation until the row confirms
or its node leaves. This is the job today's ledger and presence polls do; under epochs it
is the one piece of leader-side state the design keeps. Modelled as `eRedrive`.

### Durable changes wake the fleet, and a runner with no row is an orphan

Found by `tcPartitionHealEpoch` after both fixes (0.02%, a *duplicate*): a node won its
check-in, was cut off before the reply arrived, had its row removed by the ejection, healed,
and then received the late `ok` — true at commit time — and started. The leader placed the
agent elsewhere. The sweep's stop side would have stopped the orphan, but the node had no
ticks left: under epochs the leader's row write and the confirm were not refilling the
fleet the way `AddAssignmentAsync` does in stock, so the orphaned node never looked again.
Two rules: every durable change (placement write, confirmation) wakes every node, exactly
as in stock; and under epochs "running with no row" is an orphan to stop, never "my write
in flight", because nodes do not write rows. Both are what the reconcile sweep needs to
finish the job the check-in cannot: a late `ok` is not wrong, it is simply stale by the time
it arrives, and the node's next look at the table corrects it.
