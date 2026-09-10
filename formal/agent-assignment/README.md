# Single-agent ownership, model checked

Does a leader-assigned singular agent end up running on exactly one live node? This is the
assignment plane the [leader-election spec](../leader-election/) deliberately leaves out.
See [`../README.md`](../README.md) for why these specs exist and how to run them.

Two facts have to agree in the end: the **durable assignment row** in the store
(`AddAssignmentAsync` / `RemoveAssignmentAsync`) and the **node-local fact** of actually
running the agent (`Agents[uri]`). Faults split them, and the interesting question is
whether they reconverge.

**They don't.** This model is faithful to the shipped RDBMS schema, and on that schema the
shipped system settles into permanent duplicates in two fault scenarios — both committed
below as **expected violations** (a bug ledger, the mirror image of the mutant ledger).
The model also carries the proposed fix, the GH-4297 node-side reconcile sweep, as a
per-test toggle; with it on, every case converges. The point of keeping the bad states
checkable is that a *structural* fix can later be dropped into the model and validated
against exactly the schedules that break stock.

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

## The bug ledger — committed counterexamples

Run these expecting red; the violation *is* the result. Both reproduce shapes measured on
live clusters by the wolverine-deploy-sim rig (RESULTS.md 2026-09-09: duplicates on ~24%
of rolling deploys at defaults, never self-healing, table always immaculate).

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
| `Node` | one process: its health-check tick (leader: evaluate placement; any node: the GH-4297 sweep when enabled) and the local fact of running the agent |
| `pendingTarget` | the GH-3698 pending-assignment ledger for one agent: leader-local, in-memory, retired when the row lands, dropped on demotion, **lost at leader death** |
| `sweep` (toggle) | the GH-4297 node-side reconcile: stop my copy if the row names another live node; start my copy if the row names me and nothing runs here |
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
  may be the thing in flight — and the start side is what heals the row-without-runner
  state the stop side can leave when it races this node's own start. Both halves are
  load-bearing (see the mutants).

Left out, deliberately: multiple agents and even distribution (this is one singular
agent); command batching and reply windows (GH-3604/D3); blue/green capability matching;
the advisory-lock election mechanics (the sibling spec's job — leadership moving safely is
*assumed* here). A partition is a node excluded from the live set while it keeps running.

## Running it

From the repo root, `nix develop` provides `dotnet` and `p`. Then:

```
cd formal/agent-assignment
p compile --pfiles AgentOwnershipModel.p AgentOwnershipSpec.p AgentOwnershipTest.p --projname AgentOwnership --outdir .
p check -tc tcLeaderHandoverStock -s 5000     # expect the committed violation
p check -tc tcLeaderHandoverSweep -s 20000    # expect clean
```

Run both PEx and the random bugfinder — they catch different bugs (see
[`../README.md`](../README.md)) — and use `-s`, not `-i`.

## Results

Default-mode random bugfinder, 20k schedules per case:

| Case | Sweep | Result |
| --- | --- | --- |
| `tcSteadyState` | off | no violation |
| `tcCrash` | off | no violation |
| `tcPartitionHealStock` | off | **VIOLATED (expected)** — `settled with 2 nodes running`, 100% of schedules |
| `tcPartitionHealSweep` | on | no violation |
| `tcLeaderHandoverStock` | off | **VIOLATED (expected)** — `settled with 2 nodes running`, 0.16% of schedules |
| `tcLeaderHandoverSweep` | on | no violation |
| `tcChaosSweep` (partition+crash+leader kill) | on | no violation |

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
  with the leader (bug ledger a) matters.
- **The sweep's stop side** — **violated on `tcLeaderHandoverSweep`**: the handover
  duplicate forms and nothing stops the orphan. `settled with 2 nodes running`.
- **The sweep's start side** — **violated on `tcPartitionHealSweep`**: the healed node's
  re-upsert lands after it swept its own copy away, leaving a row that names a non-runner;
  with no start side nothing re-drives it. `settled with 0 nodes running`. This is the
  model's argument that GH-4297 must ship both halves.
- **Placement of an unowned agent** (the leader never assigns) — **violated on `tcCrash`**:
  the sole owner dies and the agent never runs again. `settled with 0 nodes running`.

Model-authoring lessons kept from earlier revisions, all found by the checker: the store
writing the row at dispatch let a row outlive its runner; announcing a death before opening
its fault let the monitor read a false quiescence; and two fault-accounting races in this
revision (a partition strike landing on an already-dead node, whose cutoff a Dead node must
retire like any other stray command). Same lesson every time: tell the monitor about the
work still owed before you announce the step that looks like rest.
