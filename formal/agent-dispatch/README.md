# Claim ownership in the command dispatcher, model checked

`AgentCommandDispatcher` hands the leader one fact per agent: *is a placement still on its
way, and to where?* Everything the GH-4537 review rounds found was one question about that
fact — **can an agent's claim be held, stolen, or released by a dispatch that does not own
it?** Seven findings on one question is the signature of a missing invariant, so this models
the invariant instead of patching the next window.

It also carries the two candidate designs, because the open question for GH-4537 is whether
the five guards the shipped class has accumulated (ticket identity, lane identity, the
cascade gate, the admission check, claim displacement and rollback) collapse into one lock:

| | |
| --- | --- |
| `atomic = true` | one lock around claim publication and the abandonment scan. Each operation is a single event, so the `Dispatcher` machine's run-to-completion semantics *are* the lock. |
| `atomic = false` | the shipped lock-free design. Each operation is a sequence of events with yields between them, and the step boundaries are the real code's atomic units — individual concurrent-map operations — so an interleaving found here is one the CLR may produce. |

## Status: partial. Read the ledger before trusting a green run.

The headline result is sound and reproduced from a counterexample. The ledger is not yet
strong enough to make the *green* runs mean much.

## Results

```
p compile --pfiles AgentDispatchModel.p AgentDispatchSpec.p AgentDispatchTest.p --projname AgentDispatch --outdir .
p check -tc <testcase> -s 30000
```

| Test case | Fault | Result |
| --- | --- | --- |
| `tcAtomicSteady` / `tcLockFreeSteady` | none | 0 bugs |
| `tcAtomicDeparture` / `tcLockFreeDeparture` | a node leaves the roster **and dies** | 0 bugs |
| `tcAtomicStale` | a node leaves the roster and **keeps running** | **VIOLATION — expected** |
| `tcLockFreeStale` | same | 0 bugs at 200k random schedules; see the honesty note |

### The headline: a lock does not close the admission window

`tcAtomicStale` violates `ClaimsStaySound` with the whole abandonment taken atomically. The
counterexample is the third Copilot finding of round four, and it survives the lock exactly
as predicted, because the one step that cannot be taken inside a lock is running the command:

1. a lane worker is admitted (`eAdmitR ok:True`) and not yet executing;
2. the health check abandons the lane — atomically — releasing the agent's claim and
   reporting it to the leader;
3. the leader drops the matching pending-assignment entry and re-places the agent on node 2;
4. the admitted command *then* runs, starting the agent on node 1, which left the roster but
   is still alive;
5. two live nodes run the one agent.

So the answer to "does one lock subsume the five guards" is **no, not this one**. The
remaining question — whether it subsumes the other four — is what the ledger below cannot
yet answer.

### Why "stale" is the only case that breaks

`tcAtomicDeparture` is clean because a node that leaves the roster *and dies* absorbs the
late command: a start that reaches nothing starts nothing. The damage needs a node absent
from the snapshot but still running, which is precisely the lagging-snapshot case
`StaleNodeEjectionThreshold` exists to make rare. The model says that threshold is not a
tidiness knob; it is the only thing standing between this window and a duplicate agent.

## Mutant ledger — 2 of 5, and that is the honest number

Each mutant re-introduces one defect the review rounds found. `mutants.py` applies literal
replaces to a pristine copy, verifies each applied, recompiles, and re-checks.

| Mutant | Defect | Caught |
| --- | --- | --- |
| `m2-claim-after-entry` | claims published after the queue entry, so an abandonment scan in between strands them (round 3) | **yes** — `tcLockFreeDeparture`, `tcLockFreeStale`: *a claim is still held with nothing in flight* |
| `m5-no-admission-check` | the worker runs a command without re-checking abandonment (round 2) | **yes** — `tcLockFreeStale`: *2 live nodes running the one agent* |
| `m1-no-putback` | an enqueue that loses the dedup keeps the claim it displaced (round 4) | no |
| `m3-scan-by-destination` | the abandonment scan matches on the node rather than the lane (round 2) | no |
| `m4-release-without-ticket` | a stale release clears whatever entry is under the key (round 1) | no |

The three misses share one cause, and it is a gap in the **configuration**, not a claim that
those guards are unnecessary. All three need a placement aimed at a node whose lane is being
abandoned, and this model's leader never produces one: it picks only from the roster it has,
and its pending-assignment ledger blocks a second placement while one is outstanding. In the
real system that placement arrives two ways this model does not yet have — a node that blips
out of the roster and returns (the case the ejection hysteresis is for), and a cascade from
another lane aimed at the departing node. **Until those are modelled, a green run here is not
evidence that m1/m3/m4's guards can be removed.**

## What the shipped dispatcher has that this model does not (October 2026)

The model was cut against the GH-4537 review rounds. The dispatcher that shipped afterwards
carries three more mechanisms, none modelled here:

- **`AbandonLane(nodeId)`** — the health check tells the dispatcher a node has left (ejected
  as stale, or its row gone since the last tick), and the dispatcher drops everything queued
  in that lane, cancels the executing command, and removes the lane. This is the abandonment
  the model's `AbandonOp` stands for, now wired to departures on every node.
- **GH-4901 start withdrawal** — a reassignment aimed at a source whose start for the same
  agent is still only *queued* (never handed to the node) withdraws that start and sends it
  straight to the destination's lane instead of stopping something that never ran. The
  `_starting` / `_withdrawn` / `_handedOver` sets order a lane taking a start up against a
  withdrawal, and a start that was ever handed over is a re-drive, not a first start, so a
  later move must stop it at the source. The model's `m3`/`m1` misses are in exactly this
  territory.
- **The admission check is now per command, with claims released before ownership** (the
  `release(); releaseStarts();` order in the lane's `finally`), which is the model's
  `m5-no-admission-check` and `m2-claim-after-entry` guards, kept.

Until the withdrawal path and the departure-and-return case are modelled, the ledger's
2-of-5 stands.

## What it abstracts

- **One agent.** Claim ownership is per-agent; batching only decides which commands compare
  equal, which is modelled directly by keying a command on its destination.
- **`_moving` and reassignment** are out. Only `_inFlight` and first-time placement are
  modelled, so the cascade gate is not exercised at all.
- **The abandonment scan is one step**, not a loop of individual compare-and-removes.
- **The leader** is cut to what touches the dispatcher: a pending-assignment entry, an
  `isOutstanding` probe, a grid that shows where the agent runs, and a bounded placement
  budget. Its TTL backstop is absent — the budget bounds the run instead.
- **Only the abandonment path reports releases.** An ordinary completion drops its claims
  silently, as `release()` does; the leader learns from the grid. Getting this wrong makes
  the model report duplicates on a fault-free run.

## Next

1. A node that leaves the roster and returns, and a cascade source, so m1/m3/m4 are reachable.
2. PEx on the stale cases — the space looks small enough to close.
3. Only then: whether the lock-free guards can be traded for one lock.
