// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ingress

import (
	"context"
	"log/slog"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/agent-substrate/substrate/cmd/atenet/internal/router/extproc"
	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/singleflight"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/util/wait"
)

// failFastResumeBudget is the total time the resumer spends retrying a resume
// when request parking is disabled. In that mode only concurrent-update
// conflicts are retried; capacity errors fail immediately.
const failFastResumeBudget = 15 * time.Second

// CommittedAttemptWait is the bounded extra time a caller keeps waiting past
// the budget for a resume attempt that was already in flight when the budget
// elapsed. The attempt itself is NEVER cancelled (see ResumeActor): the wait
// only bounds the caller, and an attempt that misses it continues detached
// until the attempt ceiling while the caller gets the usual retryable 503.
//
// It is a constant, not a flag: it absorbs the routine snapshot-restore
// overshoot under node contention, which is a property of snapshot size and
// node load, not an operator policy. Everything sized from the router's
// worst-case hold moves with it through MaxResumeHold — Envoy's ext_proc
// message timeout, the drain deadline, the flag help — and the shutdown
// sequence needs drain-delay + the dataplane window + the derived drain
// deadline to fit the pod's termination grace period, which caps how far this
// can be raised.
const CommittedAttemptWait = 3 * time.Second

// resumeAttemptCeiling is the absolute liveness bound on each ResumeActor
// attempt, applied per-attempt so every attempt gets the full ceiling
// regardless of how much budget preceded it. What it adds over ateapi's own
// server-side cap (MaxDeadlineUnaryInterceptor, 10 minutes — see
// cmd/ateapi/main.go) is an end to the flight when the path blackholes
// without a TCP reset (node partition, conntrack drop), where no server cap
// can help. Two orders of magnitude above any real restore, it is a backstop
// against a wedged control plane, not the budget cancel returning.
//
// Mechanics the classification below depends on: gRPC propagates this
// deadline to the server, so ateapi's handler expires at the SAME 9m30s (its
// clock starting one transit later), and the server's 10m clamp is a no-op
// for this call; meanwhile grpc-go mints the client-side DeadlineExceeded
// locally, so on a responsive path attemptCtx.Err() is always set when the
// error surfaces, and on a wedged path the local timer is the only authority
// — either way ceilingHit classifies correctly. The margin below the server
// cap guards the one configuration that could break this: an independently
// LOWERED ateapi cap. If that cap ever dropped below this ceiling, the
// server's clamp would fire first and its DeadlineExceeded would classify as
// a definitive answer, reaching still-attached callers as a 504 — keep this
// constant strictly under ateapi's cap. When the ceiling fires, the
// caller-facing verdict is the retryable 503 (see the ceiling classification
// in runFlight), never a 504.
const resumeAttemptCeiling = 9*time.Minute + 30*time.Second

// errResumeIncomplete is the verdict when a caller runs out of time with the
// resume attempt still in flight and no completed attempt to blame. With
// nothing observed, fabricating a capacity error would send operators hunting
// a pool-sizing problem that may not exist; the honest answer is "it did not
// finish yet". Unavailable keeps the client-facing mapping a retryable 503.
var errResumeIncomplete = status.Error(codes.Unavailable, "resume did not complete before the caller's wait bound")

// resumeBackoff builds the backoff between resume attempts while a request is
// parked, from the configured retry parameters.
//
// It intentionally sets NO Cap. wait.Backoff's delay() zeroes Steps the moment
// the delay reaches Cap, which would end retries long before the parking budget
// (a Cap of 2s stops the loop in ~7 steps regardless of the budget). A gentle
// Factor keeps the gap small on its own — from 100ms at the default 1.1 the gap
// only grows to ~0.5s over a 5s budget — while Steps is set high so the budget
// context passed to ExponentialBackoffWithContext, not the step count, bounds
// the wait.
func resumeBackoff(interval time.Duration, factor, jitter float64) wait.Backoff {
	return wait.Backoff{
		Steps:    math.MaxInt32,
		Duration: interval,
		Factor:   factor,
		Jitter:   jitter,
	}
}

// budgetExhaustedError marks a resume that was still blocked on a retryable
// condition (e.g. "no free workers available") when the parking budget — or
// the caller's wait bound — elapsed. It wraps the last retryable error, so the
// HTTP boundary still maps the underlying gRPC status faithfully (503 with the
// capacity message), while the parking metrics can report budget exhaustion as
// its own outcome.
type budgetExhaustedError struct{ lastErr error }

func (e *budgetExhaustedError) Error() string { return e.lastErr.Error() }
func (e *budgetExhaustedError) Unwrap() error { return e.lastErr }

// ResumeOutcome indicates the singleflight execution state of an actor resumption request.
type ResumeOutcome string

const (
	ResumeOutcomeNone      ResumeOutcome = ateattr.RouterResumeNone
	ResumeOutcomeTriggered ResumeOutcome = ateattr.RouterResumeTriggered
	ResumeOutcomeJoined    ResumeOutcome = ateattr.RouterResumeJoined
)

type resumeCallResult struct {
	actor *ateapipb.Actor
	// resumed is true if ResumeActor call executed a cold activation
	// false if the actor was already running
	resumed bool
	// leaderID is the unique request ID (reqID) of the leader that initiated
	// the singleflight execution. It helps disambiguates the leader caller
	// (ResumeOutcomeTriggered) from joiner callers (ResumeOutcomeJoined).
	leaderID uint64
	err      error
}

// ActorResumer coordinates safe, deduplicated resumption of actors.
type ActorResumer struct {
	apiClient ateapipb.ControlClient
	flight    singleflight.Group

	// parkEnabled makes transient worker-pool saturation (FailedPrecondition)
	// retryable, so a request is parked and retried until budget rather than
	// failing immediately.
	parkEnabled bool
	// budget bounds the total time a single resume operation retries before the
	// underlying error is returned.
	budget time.Duration
	// backoff paces the retries within the budget.
	backoff wait.Backoff
	// metrics surfaces resume attempts that outlive the caller wait bound and
	// continue detached — the only visibility into them, since the parking
	// gauge reads 0 once every caller has been answered. Nil is a no-op.
	metrics *ParkingMetrics
	// flightsMu guards activeFlights and flightsIdle, which together track
	// in-flight resume flights — detached ones included — so the shutdown
	// sequence can wait for them: a detached flight holds no ext_proc stream,
	// and process exit would cancel its RPC mid-restore, the strand this type
	// exists to prevent. Deliberately NOT a sync.WaitGroup: a flight starting
	// while a waiter is parked would make WaitGroup.Add panic ("Add called
	// concurrently with Wait"), and on the force-stop drain path handlers can
	// still reach DoChan after the ext_proc server stops. See WaitFlights.
	flightsMu     sync.Mutex
	activeFlights int
	flightsIdle   chan struct{} // non-nil only while a waiter is parked; closed when activeFlights hits 0
	// nextID is a counter assigned to each incoming ResumeActor call.
	// Used as a unique ID to identify requests (reqID) and disambiguate the
	// leader vs joiners for singleflight outcome classification.
	nextID uint64
}

// resumerOption configures an ActorResumer.
type resumerOption func(*ActorResumer)

// withParking configures parking behavior from cfg. When parking is enabled,
// FailedPrecondition ("no free workers available") becomes retryable and the
// resume is retried, at cfg's retry cadence, for up to cfg's budget. When
// disabled, the resumer applies fail-fast-on-capacity behavior.
func withParking(cfg ParkedRequestConfig) resumerOption {
	cfg = cfg.Normalized()
	return func(r *ActorResumer) {
		r.parkEnabled = cfg.Enabled()
		if r.parkEnabled {
			r.budget = cfg.Budget
		}
		r.backoff = resumeBackoff(cfg.RetryInterval, cfg.RetryFactor, cfg.RetryJitter)
	}
}

// withMetrics attaches the parking instruments so the resumer can count resume
// attempts that continue detached past the caller wait bound.
func withMetrics(m *ParkingMetrics) resumerOption {
	return func(r *ActorResumer) { r.metrics = m }
}

func NewActorResumer(apiClient ateapipb.ControlClient, opts ...resumerOption) *ActorResumer {
	r := &ActorResumer{
		apiClient: apiClient,
		budget:    failFastResumeBudget,
		backoff: resumeBackoff(DefaultParkedRequestRetryInterval,
			DefaultParkedRequestRetryFactor, DefaultParkedRequestRetryJitter),
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// flightStarted registers a flight with the shutdown tracker.
func (r *ActorResumer) flightStarted() {
	r.flightsMu.Lock()
	r.activeFlights++
	r.flightsMu.Unlock()
}

// flightFinished deregisters a flight and wakes a parked WaitFlights when the
// last one lands.
func (r *ActorResumer) flightFinished() {
	r.flightsMu.Lock()
	r.activeFlights--
	if r.activeFlights == 0 && r.flightsIdle != nil {
		close(r.flightsIdle)
		r.flightsIdle = nil
	}
	r.flightsMu.Unlock()
}

// WaitFlights blocks until every in-flight resume flight — detached ones
// included — has completed, or ctx expires. The shutdown sequence calls this
// after the ext_proc drain: streams with attached callers end with the drain,
// but a detached flight holds no stream, and exiting the process would drop
// the ateapi connection and cancel its resume mid-restore. A flight that
// starts while the wait is parked is counted and waited for too; only one
// starting after the tracker already reported idle is missed, degrading to
// cancel-at-exit for that single flight.
func (r *ActorResumer) WaitFlights(ctx context.Context) error {
	r.flightsMu.Lock()
	if r.activeFlights == 0 {
		r.flightsMu.Unlock()
		return nil
	}
	if r.flightsIdle == nil {
		r.flightsIdle = make(chan struct{})
	}
	idle := r.flightsIdle
	r.flightsMu.Unlock()

	select {
	case <-idle:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// retryable reports whether err warrants another resume attempt while the
// request remains parked. A concurrent-resume conflict (Aborted) is always
// retried. Transient pool saturation (FailedPrecondition, "no free workers
// available") and transient control-plane unavailability (Unavailable, e.g. an
// ateapi rolling restart) are retried only when parking is enabled, turning a
// momentary condition into a bounded wait instead of an immediate failure — a
// parked request should ride out a blip, not fail on it with budget remaining.
// All other codes (NotFound, DeadlineExceeded, PermissionDenied, ...) are
// returned to the caller so the HTTP boundary can map them with full fidelity.
func (r *ActorResumer) retryable(err error) bool {
	switch status.Code(err) {
	case codes.Aborted:
		return true
	case codes.FailedPrecondition, codes.Unavailable:
		return r.parkEnabled
	default:
		return false
	}
}

// runFlight executes one shared resume flight: the retry loop, the
// never-cancel attempt handling, and the terminal classification. It runs
// inside the singleflight group, detached from every caller — a caller that
// disconnects or stops waiting never aborts it (see ResumeActor). Two clocks
// bound the work:
//
//   - The per-FLIGHT retry budget (bgCtx) bounds when the flight stops
//     STARTING resume attempts. Its clock begins with the flight's first
//     caller; later callers de-duplicated onto the flight share its attempts
//     and outcome.
//   - The per-CALLER wait bound (in ResumeActor's select) bounds when a given
//     caller stops WAITING: its own arrival + budget + CommittedAttemptWait.
//
// The attempt itself runs on a context this router NEVER cancels. ateapi
// persists the worker claim and the actor's RESUMING status durably before
// the snapshot restore begins, rolls back neither on cancellation, and no
// reconciler recovers a RESUMING actor whose worker pod is alive — a
// cancelled resume therefore strands the worker and discards the restore.
// Nor can a probe decide when a cancel is safe: the claim and the status are
// two non-atomic store writes, and a cancel issued from a read races the
// workflow it observed (#675). An attempt that outlives every caller
// continues detached, bounded by resumeAttemptCeiling (mirroring ateapi's own
// server-side RPC cap).
func (r *ActorResumer) runFlight(actorRef resources.ActorRef, reqID uint64) (*resumeCallResult, error) {
	r.flightStarted()
	defer r.flightFinished()

	flightStart := time.Now()
	bgCtx, bgCancel := context.WithTimeout(context.Background(), r.budget)
	defer bgCancel()
	// attemptBase strips the budget's cancellation and deadline. bgCtx
	// carries no values today, so this is Background in effect; WithoutCancel
	// keeps any values a future refactor attaches to the flight flowing to
	// the control plane.
	attemptBase := context.WithoutCancel(bgCtx)

	backoff := r.backoff

	var resumeResp *ateapipb.ResumeActorResponse
	var lastRetryErr error
	// attemptErr records a definitive (non-retryable) answer from an attempt.
	// It must pass through untouched however late it lands — including bare
	// context sentinels an attempt might surface — so exhaustion
	// classification below never masks it. ceilingHit instead marks the one
	// error the router itself manufactures: its own attempt ceiling expiring,
	// which must NOT read as a definitive control-plane answer.
	var attemptErr error
	var ceilingHit bool

	err := wait.ExponentialBackoffWithContext(bgCtx, backoff, func(context.Context) (bool, error) {
		// The ceiling is per-attempt: every attempt gets the full allowance
		// no matter how much budget preceded it, so a budget close to the
		// ceiling cannot starve the final attempt.
		attemptCtx, attemptCancel := context.WithTimeout(attemptBase, resumeAttemptCeiling)
		defer attemptCancel()
		var err error
		resumeResp, err = r.apiClient.ResumeActor(attemptCtx, &ateapipb.ResumeActorRequest{
			Actor: actorRef.ToObjectRef(),
		})
		if err == nil {
			return true, nil
		}

		if attemptCtx.Err() == context.DeadlineExceeded && status.Code(err) == codes.DeadlineExceeded {
			// The router's own ceiling ended the attempt; stop the flight.
			ceilingHit = true
			return false, err
		}
		if r.retryable(err) {
			lastRetryErr = err // remember it in case the budget elapses
			return false, nil  // park: retry until the budget elapses
		}
		attemptErr = err
		return false, err
	})

	if elapsed := time.Since(flightStart); elapsed > r.budget+CommittedAttemptWait {
		// The attempt ran past the flight's budget + committed wait. The
		// leader has been answered by now (a joiner with a later arrival may
		// still have been served — see the caller select), and the gauge
		// reads 0 once callers leave, so this counter is the only signal that
		// restores are outliving even the committed wait.
		//
		// Log the loop's MEANINGFUL terminal answer, not the raw loop error:
		// in the common case (budget expiry between retries) the raw error is
		// a bare context.DeadlineExceeded, which an operator would misread as
		// a cancellation in the one code path whose contract is that nothing
		// is ever cancelled.
		lastErr := err
		switch {
		case attemptErr != nil:
			lastErr = attemptErr
		case !ceilingHit && lastRetryErr != nil:
			lastErr = lastRetryErr
		}
		r.metrics.recordResumeDetached(context.Background(), err == nil)
		slog.Warn("resume attempt ran past the flight's budget and committed wait",
			slog.Any("actor", actorRef),
			slog.Duration("elapsed", elapsed),
			slog.Bool("succeeded", err == nil),
			slog.Any("lastErr", lastErr))
	}

	if err != nil {
		if ceilingHit {
			// The router's own liveness ceiling ended the attempt — the most
			// alarming condition this code can hit, distinct from a benign
			// late completion: a control-plane path was unresponsive for the
			// whole ceiling. Logged at Error with its own message so it is
			// alertable apart from the generic detached warn above.
			slog.Error("resume attempt hit the router's liveness ceiling; the control-plane path may be wedged",
				slog.Any("actor", actorRef),
				slog.Duration("ceiling", resumeAttemptCeiling))
			// To any caller still attached this is indistinguishable from
			// "not done yet": surface the retryable incomplete verdict (503,
			// budget_exhausted), not a 504 that would misread the router's
			// backstop as a control-plane timeout.
			return &resumeCallResult{leaderID: reqID, err: &budgetExhaustedError{lastErr: errResumeIncomplete}}, nil
		}
		if attemptErr != nil {
			// A definitive answer (NotFound, a genuine control-plane
			// DeadlineExceeded, ...) is never reported as exhaustion, however
			// late it landed.
			return &resumeCallResult{leaderID: reqID, err: attemptErr}, nil
		}
		// The loop itself ran out of time: between retries, or an attempt
		// came back with a retryable error after the budget had already
		// elapsed. Surface the last retryable error rather than the generic
		// wait error so the HTTP boundary maps it faithfully (e.g. 503 "no
		// free workers available") instead of a misleading 504; the wrapper
		// marks the exhaustion for the parking wait-duration metric. The nil
		// fallback is defensive — the first attempt always completes before
		// the loop can be interrupted, so the reachable zero-attempts path is
		// the caller-side wait bound in ResumeActor — and exists so no
		// refactor can ever wrap a nil error.
		lastErr := lastRetryErr
		if lastErr == nil {
			lastErr = errResumeIncomplete
		}
		return &resumeCallResult{leaderID: reqID, err: &budgetExhaustedError{lastErr: lastErr}}, nil
	}

	return &resumeCallResult{
		actor:    resumeResp.GetActor(),
		resumed:  resumeResp.GetResumed(),
		leaderID: reqID,
	}, nil
}

// ResumeActor ensures the requested actor is running. It deduplicates concurrent
// requests within the process and, when parking is enabled, holds the request
// while retrying transient failures until the budget elapses. The flight
// itself runs in runFlight; this function is the caller-side contract: join
// the shared flight, wait at most the caller's own bound, classify.
func (r *ActorResumer) ResumeActor(ctx context.Context, actorRef resources.ActorRef) (*ateapipb.Actor, ResumeOutcome, error) {
	ctx, span := otel.Tracer(extproc.ServiceName).Start(ctx, "ResumeActor",
		trace.WithAttributes(ateattr.ActorRefAttributes(actorRef)...))
	defer span.End()

	reqID := atomic.AddUint64(&r.nextID, 1)

	ch := r.flight.DoChan(actorRef.String(), func() (interface{}, error) {
		return r.runFlight(actorRef, reqID)
	})

	// The wait bound is per-caller — arrival + budget + CommittedAttemptWait —
	// not per-flight: a caller joining a flight whose attempt is mid-restore
	// still gets a full wait of its own and is served the moment the shared
	// attempt lands. In the saturated regime attempts fail fast, the flight
	// returns at its budget, and every waiter gets the shared verdict then; the
	// timer below only bites when an attempt genuinely outlives the budget,
	// which is exactly when waiting is productive.
	waitBound := time.NewTimer(r.budget + CommittedAttemptWait)
	defer waitBound.Stop()

	var res singleflight.Result
	select {
	case <-ctx.Done():
		// The caller's request context was canceled before the singleflight resume completed.
		// Return early with ResumeOutcomeNone ("none")
		return nil, ResumeOutcomeNone, ctx.Err()
	case <-waitBound.C:
		// Prefer a result that already landed: select picks a ready case at
		// random, and answering a completed resume with a 503 wastes the
		// worker it just claimed. This drain only sees a value that is already
		// buffered — a result being published in the same instant (inside
		// singleflight's mutex) can still lose and get the retryable 503; the
		// next request is then served warm. Deliberately untested: the winning
		// interleaving (timer fired AND result already buffered) cannot be
		// scheduled deterministically even in a synctest bubble; the two
		// deterministic outcomes on either side are what the tests pin.
		select {
		case res = <-ch:
		default:
			// This caller is done waiting; the attempt keeps running detached
			// (the actor still converges to RUNNING, the worker is never
			// stranded, and the next request is served warm). Same verdict
			// shape as flight-side exhaustion so the HTTP boundary returns a
			// retryable 503 and the parking metrics record budget_exhausted
			// rather than a 504/timeout.
			return nil, ResumeOutcomeNone, &budgetExhaustedError{lastErr: errResumeIncomplete}
		}
	case res = <-ch:
	}

	callRes, _ := res.Val.(*resumeCallResult)
	if callRes == nil {
		if res.Err != nil {
			return nil, ResumeOutcomeNone, res.Err
		}
		return nil, ResumeOutcomeNone, status.Error(codes.Internal, "resume call returned nil result")
	}

	// On error, return ResumeOutcomeNone ("none") so the failure is tagged
	// under the 'outcome' label rather than misreported as an activation.
	if callRes.err != nil {
		return nil, ResumeOutcomeNone, callRes.err
	}

	// Disambiguate singleflight resume outcome:
	// - ResumeOutcomeNone ("none"): resumed == false, actor was already active/running.
	// - ResumeOutcomeTriggered ("triggered"): Cold activation leader (resumed == true, caller's reqID == leaderID).
	// - ResumeOutcomeJoined ("joined"): Cold activation joiner (resumed == true, caller's reqID != leaderID).
	outcome := ResumeOutcomeNone
	if callRes.resumed {
		if callRes.leaderID == reqID {
			outcome = ResumeOutcomeTriggered
		} else {
			outcome = ResumeOutcomeJoined
		}
	}

	return callRes.actor, outcome, nil
}
