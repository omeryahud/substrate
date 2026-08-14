# Request Parking (atenet router)

## Summary

**Request parking** lets the `atenet` router hold ("park") an inbound request
whose target actor cannot be served *yet* because of transient worker-pool
saturation, retrying the resume until the actor becomes routable or a bounded
wait elapses — instead of immediately returning `503` to the client.

## Motivation

When a request arrives for a suspended actor, the router resumes it before
routing:

```
Envoy --(ext_proc RequestHeaders)--> router.handleRequestHeaders
    --> ActorResumer.ResumeActor --> ateapi ResumeActor (gRPC)
```

`ateapi`'s `AssignWorkerStep` claims a free worker from the actor's `WorkerPool`.
In an oversubscribed system — the core premise of Substrate, where many actors
multiplex onto few workers — a burst of traffic can momentarily exhaust the
pool. `AssignWorkerStep` then returns `FailedPrecondition: "no free workers
available"`.

Previously the router mapped that straight to an HTTP `503` and failed the
request. But such saturation is usually momentary: another actor suspends within
milliseconds and frees its worker. Failing fast turns a sub-second blip into a
user-visible error.

## Behavior

With parking enabled (the default), the router treats `FailedPrecondition` and
`Unavailable` from `ResumeActor` as **retryable** conditions (alongside the
existing `Aborted` concurrent-resume conflict) — a parked request rides out
transient pool saturation and control-plane blips (e.g. an ateapi rolling
restart) alike. The request is *parked*: the resumer keeps retrying with
exponential backoff until either

- the resume succeeds (the actor is `RUNNING` and has a worker IP) — the request
  is then routed normally; or
- the **park budget** (`--parked-request-budget`, default `5s`) elapses with the
  resume still blocked on a retryable condition — the underlying capacity error
  is returned, surfacing as `503 "actor <id> unavailable: no free workers
  available"`; or
- the budget elapses with a resume attempt **still in flight**, and the attempt
  does not land within a further bounded wait (**3s**) — the caller then gets
  `503 "actor <id> unavailable"` while the attempt continues to completion in
  the background (see below).

**The budget bounds retries; a committed attempt is never cancelled.** When the
park budget elapses the router stops *starting* resume attempts. An attempt
already in flight is never cancelled, because cancellation cannot be made safe:
the control plane durably claims the worker and marks the actor `RESUMING`
*before* the expensive snapshot restore begins, it rolls back neither write on
cancellation, and no reconciler recovers a `RESUMING` actor whose worker pod is
alive. A cancel therefore discards the restore and strands the worker — the
pool the request was waiting on stays starved for every request behind it
(#675). Nor can the router probe first and cancel only when "nothing is
assigned yet": the claim and the status are two separate, non-atomic store
writes, so any read-then-cancel races the workflow it observed.

Instead, each caller waits up to a further **committed-attempt wait** (3s, a
constant — it absorbs the routine snapshot-restore overshoot under node
contention, which is a property of snapshot size and load, not an operator
policy). Under contention the restore regularly overshoots the budget by a few
hundred milliseconds, and this wait is what turns those into a `200` rather
than a `503` plus a stranded worker. If the attempt misses even the wait, the
caller gets its retryable `503` and the attempt **continues detached** —
bounded by a `9m30s` attempt ceiling that sits strictly inside the control
plane's 10-minute RPC cap (the strict inequality is load-bearing; see the
constant's comment), so even a blackholed control-plane path cannot pin a
flight forever — and the actor still converges to `RUNNING`, the worker is
never stranded, and the next request for the actor is served warm. While the
detached attempt runs, this router keeps its singleflight key held: a new
request for the same actor joins the attempt and is served the moment it lands
(only a request landing on a *different* router replica starts its own
`ResumeActor` and can see `503 "another operation is in progress"` until the
detached one finishes). Trading that *retryable* error for the elimination of
a *permanent* worker strand is the point of the design.

Note what the parking lot does and does not bound after this change: a slot is
held while a **caller** waits, so the lot still caps concurrent waiting
requests (and, via the derived circuit breaker, ext_proc streams) — but a
caller that stops waiting frees its slot while the attempt may still be
running. Detached attempts are bounded per-actor by the singleflight group
(one in-flight `ResumeActor` per actor per router) and in time by the attempt
ceiling; the `parking.resume.detached` counter below is the signal that they
are accumulating. Two second-order costs of holding the key that long, both
bounded by the lot's admission rate times the ceiling: each joiner's result
channel is retained inside the singleflight group until the flight returns,
and the completion fan-out briefly holds the group-wide mutex.

Router **shutdown** waits for detached attempts too: the drain sequence
gracefully stops the ext_proc server (covering every request with an attached
caller) and then waits a short window for flights still running detached,
because they hold no ext_proc stream and process exit would drop the
control-plane connection — cancelling the resume mid-restore. An attempt that
outlives even that window (or a SIGKILL) is cancelled by process exit; no
client-side scheme can prevent that, which is why control-plane-side
reconciliation of stuck `RESUMING` actors is the companion fix, tracked
separately.

The router's worst-case hold on a request is therefore `budget + 3s`, which
everything downstream is derived from: Envoy's ext_proc message timeout is
`budget + 3s + 2s` margin (so the router's own verdict always lands before
Envoy's generic gateway error would replace it), and the drain deadline
derivation and validation use the same worst case. The three move together by
construction; see `ExtProcMessageTimeoutFor`.

To bound resource use and provide backpressure, the router admits requests to a
**parking lot** of fixed capacity (`--parked-request-max`, default `1024`). Each
in-flight resume occupies one slot. When the lot is full, further requests are
shed immediately with `503 "actor <id> unavailable: router at capacity"` rather
than queueing without bound.

Every parked request holds one ext_proc stream — one active request against
Envoy's ext_proc cluster — for its entire wait, while ordinary requests hold
one only for a millisecond-scale header exchange. The cluster's circuit breaker
is therefore the hard ceiling on concurrent parked requests. By default the
router **derives** it as twice `--parked-request-max` (minimum `1024`), so the
lot always fits and an equal share of **fast-path headroom** remains — a
saturated lot cannot starve requests to already-running actors, at any lot
size. `--extproc-max-requests` overrides the derivation; explicit values are
validated `>= --parked-request-max` at startup, because a breaker below the lot
would silently truncate it — Envoy would reject the overflow itself, with 503s
that never reach the lot and never count in `parking.rejected`.

Concurrent requests for the *same* actor are de-duplicated by the resumer's
`singleflight` group: they share a single in-flight `ResumeActor` call and all
park on its result, so a hot actor consumes N parking slots but only one
control-plane RPC.

**Two clocks bound a parked request.** The **retry budget is per-flight**: its
clock starts when a flight's first caller begins the resume, every later
request for the same actor joins that flight and shares its attempts, and no
new attempt starts after `flight start + budget`. A request that joins late may
therefore see `budget_exhausted` after waiting far less than a full budget
itself — the accepted cost of collapsing a hot actor's requests into one
control-plane call. The **wait bound is per-caller**: each request stops
waiting at its own `arrival + budget + 3s`, so a request that joins a flight
whose attempt is mid-restore still gets a full wait of its own and is served
the moment the shared attempt lands. In the saturated regime attempts fail
fast and the flight returns its verdict at the budget, so the per-caller bound
only engages when an attempt genuinely outlives the budget — exactly when
waiting is productive. (`parking.wait.duration` records each request's *own*
parked time, so sub-budget `budget_exhausted` samples are expected under
sustained saturation.)

### What is *not* parked

Only transient conditions — capacity (`FailedPrecondition`), concurrency
(`Aborted`), and control-plane unavailability (`Unavailable`) — are parked.
Errors that will not resolve by waiting are returned immediately (fail fast):

| Resume result                          | Behavior                          |
| -------------------------------------- | --------------------------------- |
| `OK`                                   | Route to worker                   |
| `Aborted` (concurrent resume)          | Retry (always)                    |
| `FailedPrecondition` (no free worker)  | **Park & retry** (when enabled)   |
| `Unavailable` (control-plane blip)     | **Park & retry** (when enabled)   |
| `NotFound`                             | Fail fast → `404`                 |
| `DeadlineExceeded`                     | Fail fast → `504`                 |
| `PermissionDenied` / `Unauthenticated` | Fail fast → `403` / `401`         |

When parking is **disabled** (`--parked-request-max=0`), the router fails fast:
`FailedPrecondition` and `Unavailable` are returned immediately, there is no
admission cap, and only `Aborted` (concurrent-resume) conflicts are retried,
within a `15s` budget. The never-cancel contract applies in this mode too — the
cancel hazard is a property of the resume workflow, not of parking — so an
attempt in flight at the fail-fast budget also gets the committed-attempt wait
and then continues detached. The ext_proc message timeout is derived for this
mode as well (`15s + 3s + 2s` margin = `20s`), so the router's verdict still
lands before Envoy's.

The derivation covers the **Envoy** dataplane, which takes its configuration
from the router's xDS server. The agentgateway overlay is statically
configured and sets no explicit ext_proc timeout; whoever tunes that overlay
must keep its effective timeout above the router's worst-case hold by hand
(see the note in its ConfigMap).

### Parked requests survive router shutdown

A request parked when the router pod receives SIGTERM is **not** reset: the
shutdown sequence keeps the ext_proc server (and, via a preStop handshake, the
Envoy sidecar) alive until in-flight streams finish, and the ext_proc drain
deadline (`--drain-timeout`) defaults to a value derived from the worst-case
parked hold (`--parked-request-budget` + the committed-attempt wait) and is
validated at startup to be `>=` that hold — so a parked request always gets
its full budget *and* wait, and a normal verdict (routed `200` or capacity
`503`) even mid-termination. See the graceful-shutdown knobs (`--drain-delay`,
`--drain-timeout`) in `manifests/ate-install/atenet-router.yaml`.

## Configuration

| Flag                             | Default | Meaning                                                            |
| -------------------------------- | ------- | ------------------------------------------------------------------ |
| `--parked-request-budget`         | `5s`    | Retry budget per resume *flight*: when the router stops starting new attempts. Requests de-duplicated onto an in-flight resume share its attempts; each caller additionally waits up to +3s for an attempt in flight at the budget (see Behavior). |
| `--parked-request-max`            | `1024`  | Max concurrent parked/in-flight resume requests; excess shed (503). `0` disables parking. |
| `--parked-request-retry-interval` | `100ms` | Delay before a parked request's first resume retry.                |
| `--parked-request-retry-factor`   | `1.1`   | Multiplier applied to the retry delay after each attempt (>= 1).   |
| `--parked-request-retry-jitter`   | `0.1`   | Random fraction in `[0, 1)` added per retry to de-synchronize parked requests. |
| `--extproc-max-requests`          | `0` (auto) | Envoy circuit-breaker `max_requests` for the ext_proc cluster. `0` derives twice `--parked-request-max` (min `1024`); explicit values must be `>= --parked-request-max` (enforced at startup). The excess is fast-path headroom (see Behavior). |

The retry backoff deliberately has no cap and no attempt limit: the budget alone
bounds the wait.

## Observability

**Metrics** (OpenTelemetry, meter `atenet-router`):

- `atenet.router.parking.active` — up/down counter: requests currently parked.
- `atenet.router.parking.wait.duration` — histogram (seconds) of time spent
  parked. Recorded **exactly once per admitted request**, at the moment its
  resume attempt completes; never recorded for shed requests (those only
  increment `parking.rejected`) nor when parking is disabled. The `outcome`
  label says how the park ended:

  | `outcome`          | When it is set                                                              |
  | ------------------ | --------------------------------------------------------------------------- |
  | `served`           | The resume succeeded within the budget and the request was routed to its worker. |
  | `served_late`      | Served, but only past the budget — the committed-attempt wait is what saved it. The signal that snapshot restores are drifting into the budget. **Operator note:** these were previously part of `served`, so dashboards or SLOs querying `outcome="served"` alone stop counting past-budget successes after this change — sum both values for "successfully served". |
  | `budget_exhausted` | The park budget (or the caller's wait bound, for an attempt still in flight) elapsed while the resume was still blocked on a retryable condition — the signal that capacity, not a fault, is the bottleneck. |
  | `canceled`         | The client disconnected while parked (request context canceled).            |
  | `timeout`          | The request's own deadline expired while parked (distinct from the park budget). |
  | `error`            | The resume failed with a non-retryable error (`NotFound`, `PermissionDenied`, ...). |

- `atenet.router.parking.rejected` — counter: requests shed because the lot was
  full.
- **Upgrade note:** `parking.wait.duration` gained bucket boundaries at `8`
  and `18` (the two worst-case holds), so `histogram_quantile` and raw
  bucket-count comparisons are discontinuous across the deployment boundary.
- `atenet.router.parking.resume.detached` — counter, labeled `succeeded`:
  resume attempts that ran past the flight's budget + committed-attempt wait.
  The flight's first caller has been answered by then (a later joiner may
  still have been served), and `parking.active` reads 0 once callers leave, so
  this counter — plus a warn log with the actor, elapsed time, and error — is
  the main visibility into long-running attempts. `succeeded=false` means the
  attempt ran long and did **not** end with a running actor — including a
  late *definitive* answer such as the actor being deleted mid-resume — so
  treat it as "needs a look", not automatically "restore failed".

**Operator note on the route metric's `resume` label:** a cold activation
rescued past the flight leader's wait bound may produce **no** `triggered`
sample — the leader was answered with the exhaustion `503` (labeled `none`)
and a caller served by the detached attempt records `joined`. Counting cold
activations as `triggered` alone therefore undercounts exactly the slow
restores this design rescues; treat `triggered` as "a caller initiated AND
was served by a cold activation", not as the activation count.

**Status page** (`/statusz`): a "Request Parking" card shows whether parking is
enabled, the current vs. maximum parked count, and the max wait (the worst-case
hold: budget + committed-attempt wait).
