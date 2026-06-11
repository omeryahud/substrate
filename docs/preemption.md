# Worker Preemption

## Motivation

Agent Substrate's core promise is oversubscription: a large set of **actors**
multiplexed onto a small pool of **workers**, relying on the fact that
agent-like workloads are idle most of the time. Actors are placed on a worker
when they need to serve traffic (`ResumeActor`) and removed when idle
(`SuspendActor`).

Under load, every worker in a pool can be occupied at once. Before preemption,
a `ResumeActor` that arrived at a saturated pool simply failed with
`FailedPrecondition: no free workers available`. That defeats the
oversubscription model — the system could not admit a request even though the
incumbent actors were checkpointable and could be moved aside cheaply.

**Preemption** closes that gap. When no free worker exists, the scheduler evicts
a victim actor — checkpointing it first so no state is lost — and hands the
reclaimed worker to the incoming actor. This is the direct analog of CPU
scheduler preemption, Kubernetes Pod preemption, and memory paging.

## Behavior

Preemption lives in the `AssignWorkerStep` of the `ResumeActor` workflow
(`cmd/ateapi/internal/controlapi`). The assignment logic is now:

1. If the actor already has a worker from a prior (failed) attempt, reuse it.
2. Otherwise, pick a free worker in the target pool.
3. **If there is no free worker and preemption is enabled, preempt a victim** and
   use the worker it frees.
4. If none of the above yields a worker, fail with `no free workers available`
   (e.g. the pool has zero workers, or every worker is mid-flight).

A successful preemption produces exactly the same end state as if a worker had
been free: the requesting actor ends up `RUNNING` on a worker, and the victim
ends up `SUSPENDED` with a fresh `LastSnapshot`, fully recoverable on its next
request (possibly on a different worker).

## Victim selection policy

`preemptionCandidates` (in `preemption.go`) selects victims among the workers in
the **same pool** that host an actor other than the requester.

**v1 policy — longest-resident eviction.** Each worker records
`running_since_unix_nanos`, the wall-clock time its current actor was placed on
it (set on assignment, cleared on release). Candidates are ordered oldest-first;
a zero timestamp (assigned before this field existed, or a transient race) sorts
as oldest and is therefore maximally preemptible.

This approximates round-robin fairness across workers without requiring
per-actor request-activity tracking. It is a deliberately simple, deterministic
starting point. See [Future work](#future-work) for richer policies.

## Mechanism & safety

The `Preemptor` walks the ordered candidates and, for each:

- **Only preempts `RUNNING` actors.** An actor that is `RESUMING` or
  `SUSPENDING` is being mutated by another workflow; the worker record carries
  no status, so the preemptor reads the authoritative actor record and skips
  anything not cleanly `RUNNING`.
- **Suspends via the standard `SuspendActor` workflow.** No state is lost: the
  victim is checkpointed to durable storage exactly as an explicit suspend would
  do, and its worker is released.
- **Confirms the freed worker is still idle** (re-reading it for a fresh version)
  before returning it, so a concurrent resume that grabbed it first just causes
  the preemptor to try the next candidate.

### Locking and deadlock-freedom

`ResumeActor` holds a per-actor distributed lock on the *requesting* actor.
`SuspendActor` acquires a separate per-actor lock on the *victim*. The locks are
keyed by distinct actor IDs, and lock acquisition is non-blocking (try-once
`SETNX`, returning `Aborted` on contention). Therefore:

- There is no self-deadlock from nesting suspend inside resume.
- Two concurrent saturated resumes cannot deadlock on each other: a victim must
  be `RUNNING`, but an actor that is itself mid-resume is `RESUMING`, so the two
  can never select each other as victims. Even if they could, the non-blocking
  lock would turn it into a retry, not a hang.

### Bounded work

A single resume attempt tries at most `maxPreemptionCandidates` (8) victims
before giving up, so one request cannot spend its whole deadline thrashing on
contended candidates. The enclosing `AssignWorkerStep` retry/backoff then
re-drives the attempt if needed.

## Configuration

Preemption is controlled by the `ateapi` server flag:

```
--enable-preemption   (default: true)
```

Set it to `false` to restore the previous "fail when saturated" behavior — the
scheduler never suspends an actor it was not explicitly asked to.

## Limitations & future work

- **No priority/QoS classes.** All actors are equally preemptible. A future
  version could honor a priority class so high-priority actors preempt
  low-priority ones but not vice-versa.
- **Longest-resident ≠ least-recently-used.** Without request-activity tracking,
  the policy may evict a long-lived but actively-serving actor. Threading
  last-activity from the `atenet` gateway into the control plane would enable a
  true LRU/idle-based victim policy.
- **Per-pool policy.** Preemption is a single server-wide switch today; making it
  a `WorkerPool`-level setting would let operators opt specific pools in or out.
- **Over-preemption under heavy contention.** If many saturated resumes race,
  more than the strictly-necessary number of victims can be evicted before the
  freed workers are claimed. The per-attempt bound keeps this safe but not
  optimal.
