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
	"errors"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type resumerMockClient struct {
	ateapipb.ControlClient
	resumeFn func(ctx context.Context, in *ateapipb.ResumeActorRequest, opts ...grpc.CallOption) (*ateapipb.ResumeActorResponse, error)
}

func (m *resumerMockClient) ResumeActor(ctx context.Context, in *ateapipb.ResumeActorRequest, opts ...grpc.CallOption) (*ateapipb.ResumeActorResponse, error) {
	if m.resumeFn != nil {
		return m.resumeFn(ctx, in, opts...)
	}
	return nil, status.Error(codes.Unimplemented, "unimplemented")
}

func TestActorResumer_ResumeActor(t *testing.T) {
	const testActorName = "actor-a"
	const testAtespace = "team-a"
	const expectedIP = "10.0.0.52"

	testActorRef := resources.ActorRef{Atespace: testAtespace, Name: testActorName}

	t.Run("SuspendedResumedSuccessfully", func(t *testing.T) {
		var resumeCalled int
		mock := &resumerMockClient{
			resumeFn: func(ctx context.Context, in *ateapipb.ResumeActorRequest, opts ...grpc.CallOption) (*ateapipb.ResumeActorResponse, error) {
				resumeCalled++
				return &ateapipb.ResumeActorResponse{
					Actor: &ateapipb.Actor{
						Status:           ateapipb.Actor_STATUS_RUNNING,
						WorkerAssignment: &ateapipb.WorkerAssignment{WorkerPodIp: expectedIP},
					},
					Resumed: true,
				}, nil
			},
		}

		resumer := NewActorResumer(mock)
		actor, outcome, err := resumer.ResumeActor(context.Background(), testActorRef)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if actor.GetWorkerAssignment().GetWorkerPodIp() != expectedIP {
			t.Errorf("expected IP %q, got %q", expectedIP, actor.GetWorkerAssignment().GetWorkerPodIp())
		}
		if outcome != ResumeOutcomeTriggered {
			t.Errorf("expected outcome %q, got %q", ResumeOutcomeTriggered, outcome)
		}
		if resumeCalled != 1 {
			t.Errorf("expected ResumeActor called 1 time, got %d", resumeCalled)
		}
	})

	t.Run("WarmRouting_Disambiguation", func(t *testing.T) {
		mock := &resumerMockClient{
			resumeFn: func(ctx context.Context, in *ateapipb.ResumeActorRequest, opts ...grpc.CallOption) (*ateapipb.ResumeActorResponse, error) {
				return &ateapipb.ResumeActorResponse{
					Actor: &ateapipb.Actor{
						Metadata:         &ateapipb.ResourceMetadata{Name: testActorName},
						Status:           ateapipb.Actor_STATUS_RUNNING,
						WorkerAssignment: &ateapipb.WorkerAssignment{WorkerPodIp: expectedIP},
					},
					Resumed: false,
				}, nil
			},
		}

		resumer := NewActorResumer(mock)
		_, outcome, err := resumer.ResumeActor(context.Background(), testActorRef)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if outcome != ResumeOutcomeNone {
			t.Errorf("expected outcome %q for warm routing, got %q", ResumeOutcomeNone, outcome)
		}
	})

	t.Run("RetryOnAbortedConflict", func(t *testing.T) {
		var resumeCalled int
		mock := &resumerMockClient{
			resumeFn: func(ctx context.Context, in *ateapipb.ResumeActorRequest, opts ...grpc.CallOption) (*ateapipb.ResumeActorResponse, error) {
				resumeCalled++
				if resumeCalled < 3 {
					return nil, status.Error(codes.Aborted, "concurrent update conflict")
				}
				return &ateapipb.ResumeActorResponse{
					Actor: &ateapipb.Actor{
						Status:           ateapipb.Actor_STATUS_RUNNING,
						WorkerAssignment: &ateapipb.WorkerAssignment{WorkerPodIp: expectedIP},
					},
					Resumed: true,
				}, nil
			},
		}

		resumer := NewActorResumer(mock)
		actor, outcome, err := resumer.ResumeActor(context.Background(), testActorRef)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if actor.GetWorkerAssignment().GetWorkerPodIp() != expectedIP {
			t.Errorf("expected IP %q, got %q", expectedIP, actor.GetWorkerAssignment().GetWorkerPodIp())
		}
		if outcome != ResumeOutcomeTriggered {
			t.Errorf("expected outcome %q, got %q", ResumeOutcomeTriggered, outcome)
		}
		if resumeCalled != 3 {
			t.Errorf("expected ResumeActor called 3 times, got %d", resumeCalled)
		}
	})

	t.Run("ActorNotFound", func(t *testing.T) {
		mock := &resumerMockClient{
			resumeFn: func(ctx context.Context, in *ateapipb.ResumeActorRequest, opts ...grpc.CallOption) (*ateapipb.ResumeActorResponse, error) {
				return nil, status.Error(codes.NotFound, "not found")
			},
		}

		resumer := NewActorResumer(mock)
		_, outcome, err := resumer.ResumeActor(context.Background(), testActorRef)
		if got := status.Code(err); got != codes.NotFound {
			t.Errorf("expected gRPC code NotFound, got %v (err=%v)", got, err)
		}
		if outcome != ResumeOutcomeNone {
			t.Errorf("expected outcome %q on error, got %q", ResumeOutcomeNone, outcome)
		}
	})

	t.Run("SingleflightDeduplication_Disambiguation", func(t *testing.T) {
		var resumeCalled int
		var mu sync.Mutex

		mock := &resumerMockClient{
			resumeFn: func(ctx context.Context, in *ateapipb.ResumeActorRequest, opts ...grpc.CallOption) (*ateapipb.ResumeActorResponse, error) {
				mu.Lock()
				resumeCalled++
				mu.Unlock()
				time.Sleep(20 * time.Millisecond)
				return &ateapipb.ResumeActorResponse{
					Actor: &ateapipb.Actor{
						Status:           ateapipb.Actor_STATUS_RUNNING,
						WorkerAssignment: &ateapipb.WorkerAssignment{WorkerPodIp: expectedIP},
					},
					Resumed: true,
				}, nil
			},
		}

		resumer := NewActorResumer(mock)

		var wg sync.WaitGroup
		const concurrentRequests = 10
		results := make([]*ateapipb.Actor, concurrentRequests)
		outcomes := make([]ResumeOutcome, concurrentRequests)
		errs := make([]error, concurrentRequests)

		wg.Add(concurrentRequests)
		for i := 0; i < concurrentRequests; i++ {
			go func(idx int) {
				defer wg.Done()
				results[idx], outcomes[idx], errs[idx] = resumer.ResumeActor(context.Background(), testActorRef)
			}(i)
		}
		wg.Wait()

		var triggeredCount, joinedCount int
		for i := 0; i < concurrentRequests; i++ {
			if errs[i] != nil {
				t.Fatalf("request %d failed: %v", i, errs[i])
			}
			if results[i].GetWorkerAssignment().GetWorkerPodIp() != expectedIP {
				t.Errorf("request %d expected IP %q, got %q", i, expectedIP, results[i].GetWorkerAssignment().GetWorkerPodIp())
			}
			switch outcomes[i] {
			case ResumeOutcomeTriggered:
				triggeredCount++
			case ResumeOutcomeJoined:
				joinedCount++
			default:
				t.Errorf("unexpected outcome for request %d: %q", i, outcomes[i])
			}
		}

		if triggeredCount != 1 {
			t.Errorf("expected exactly 1 request to have outcome 'triggered', got %d", triggeredCount)
		}
		if joinedCount != concurrentRequests-1 {
			t.Errorf("expected %d requests to have outcome 'joined', got %d", concurrentRequests-1, joinedCount)
		}

		mu.Lock()
		defer mu.Unlock()
		if resumeCalled != 1 {
			t.Errorf("expected ResumeActor called exactly once by singleflight, got %d", resumeCalled)
		}
	})
}

// TestActorResumer_Parking runs each case inside a synctest bubble, so the
// parked retry loop's waits are fake time.
func TestActorResumer_Parking(t *testing.T) {
	const (
		testActorName = "actor-park"
		testAtespace  = "team-a"
		expectedIP    = "10.0.0.77"
	)
	testActorRef := resources.ActorRef{Atespace: testAtespace, Name: testActorName}

	t.Run("ParksThenSucceedsOnCapacityError", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			var mu sync.Mutex
			var calls int
			mock := &resumerMockClient{
				resumeFn: func(ctx context.Context, in *ateapipb.ResumeActorRequest, opts ...grpc.CallOption) (*ateapipb.ResumeActorResponse, error) {
					mu.Lock()
					calls++
					n := calls
					mu.Unlock()
					if n < 3 {
						// Worker pool momentarily saturated.
						return nil, status.Error(codes.FailedPrecondition, "no free workers available")
					}
					return &ateapipb.ResumeActorResponse{
						Actor: &ateapipb.Actor{Metadata: &ateapipb.ResourceMetadata{Name: testActorName}, Status: ateapipb.Actor_STATUS_RUNNING, WorkerAssignment: &ateapipb.WorkerAssignment{WorkerPodIp: expectedIP}},
					}, nil
				},
			}

			resumer := NewActorResumer(mock, withParking(ParkedRequestConfig{Max: 1, Budget: 5 * time.Second}))
			actor, _, err := resumer.ResumeActor(context.Background(), testActorRef)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if actor.GetWorkerAssignment().GetWorkerPodIp() != expectedIP {
				t.Errorf("expected IP %q, got %q", expectedIP, actor.GetWorkerAssignment().GetWorkerPodIp())
			}
			mu.Lock()
			defer mu.Unlock()
			if calls != 3 {
				t.Errorf("expected 3 resume attempts (parked through 2 capacity errors), got %d", calls)
			}
		})
	})

	t.Run("BudgetExpiryReturnsUnderlyingCapacityError", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			const budget = 1500 * time.Millisecond
			var mu sync.Mutex
			var calls int
			var attemptStarts []time.Duration
			base := time.Now()
			mock := &resumerMockClient{
				resumeFn: func(ctx context.Context, in *ateapipb.ResumeActorRequest, opts ...grpc.CallOption) (*ateapipb.ResumeActorResponse, error) {
					mu.Lock()
					calls++
					attemptStarts = append(attemptStarts, time.Since(base))
					mu.Unlock()
					return nil, status.Error(codes.FailedPrecondition, "no free workers available")
				},
			}

			// Budget large enough for a few ~100ms-spaced retries before it elapses;
			// the pool never frees up.
			resumer := NewActorResumer(mock, withParking(ParkedRequestConfig{Max: 1, Budget: budget}))
			_, _, err := resumer.ResumeActor(context.Background(), testActorRef)
			elapsed := time.Since(base)
			// The client must see the meaningful capacity error, not a generic
			// timeout: status.Code must unwrap through the budget-exhaustion marker.
			if got := status.Code(err); got != codes.FailedPrecondition {
				t.Errorf("expected FailedPrecondition after park budget elapsed, got %v (err=%v)", got, err)
			}
			var budgetErr *budgetExhaustedError
			if !errors.As(err, &budgetErr) {
				t.Errorf("expected the error to be marked as budget exhaustion, got %T (%v)", err, err)
			}
			// With attempts failing fast the flight returns AT the budget: the
			// caller must get the shared verdict then, not be held to its own
			// wait bound (that bound exists for attempts still in flight).
			if elapsed < budget || elapsed > budget+500*time.Millisecond {
				t.Errorf("verdict after %v, want at the %v budget", elapsed, budget)
			}
			mu.Lock()
			defer mu.Unlock()
			if calls < 2 {
				t.Errorf("expected the resume to be retried at least twice while parked, got %d", calls)
			}
			// Retries stop at the budget: no attempt may START after it.
			for i, s := range attemptStarts {
				if s >= budget {
					t.Errorf("attempt %d started at %v, after the %v budget", i+1, s, budget)
				}
			}
		})
	})

	t.Run("ParksThroughUnavailableBlip", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			var mu sync.Mutex
			var calls int
			mock := &resumerMockClient{
				resumeFn: func(ctx context.Context, in *ateapipb.ResumeActorRequest, opts ...grpc.CallOption) (*ateapipb.ResumeActorResponse, error) {
					mu.Lock()
					calls++
					n := calls
					mu.Unlock()
					if n < 3 {
						// Control plane momentarily unreachable (e.g. rolling restart).
						return nil, status.Error(codes.Unavailable, "connection refused")
					}
					return &ateapipb.ResumeActorResponse{
						Actor: &ateapipb.Actor{Metadata: &ateapipb.ResourceMetadata{Name: testActorName}, Status: ateapipb.Actor_STATUS_RUNNING, WorkerAssignment: &ateapipb.WorkerAssignment{WorkerPodIp: expectedIP}},
					}, nil
				},
			}

			resumer := NewActorResumer(mock, withParking(ParkedRequestConfig{Max: 1, Budget: 5 * time.Second}))
			actor, _, err := resumer.ResumeActor(context.Background(), testActorRef)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if actor.GetWorkerAssignment().GetWorkerPodIp() != expectedIP {
				t.Errorf("expected IP %q, got %q", expectedIP, actor.GetWorkerAssignment().GetWorkerPodIp())
			}
			mu.Lock()
			defer mu.Unlock()
			if calls != 3 {
				t.Errorf("expected 3 resume attempts (parked through 2 Unavailable blips), got %d", calls)
			}
		})
	})

	t.Run("DisabledFailsFastOnUnavailable", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			var mu sync.Mutex
			var calls int
			mock := &resumerMockClient{
				resumeFn: func(ctx context.Context, in *ateapipb.ResumeActorRequest, opts ...grpc.CallOption) (*ateapipb.ResumeActorResponse, error) {
					mu.Lock()
					calls++
					mu.Unlock()
					return nil, status.Error(codes.Unavailable, "connection refused")
				},
			}

			resumer := NewActorResumer(mock)
			_, _, err := resumer.ResumeActor(context.Background(), testActorRef)
			if got := status.Code(err); got != codes.Unavailable {
				t.Errorf("expected Unavailable, got %v (err=%v)", got, err)
			}
			mu.Lock()
			defer mu.Unlock()
			if calls != 1 {
				t.Errorf("expected exactly 1 resume attempt when parking disabled, got %d", calls)
			}
		})
	})

	t.Run("DisabledFailsFastOnCapacityError", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			var mu sync.Mutex
			var calls int
			mock := &resumerMockClient{
				resumeFn: func(ctx context.Context, in *ateapipb.ResumeActorRequest, opts ...grpc.CallOption) (*ateapipb.ResumeActorResponse, error) {
					mu.Lock()
					calls++
					mu.Unlock()
					return nil, status.Error(codes.FailedPrecondition, "no free workers available")
				},
			}

			// Default constructor => parking disabled => fail-fast.
			resumer := NewActorResumer(mock)
			_, _, err := resumer.ResumeActor(context.Background(), testActorRef)
			if got := status.Code(err); got != codes.FailedPrecondition {
				t.Errorf("expected FailedPrecondition, got %v (err=%v)", got, err)
			}
			mu.Lock()
			defer mu.Unlock()
			if calls != 1 {
				t.Errorf("expected exactly 1 resume attempt when parking disabled, got %d", calls)
			}
		})
	})
}

// TestActorResumer_CommittedAttempt pins the never-cancel contract (#675): a
// resume attempt in flight when the budget elapses is NEVER cancelled — ateapi
// binds the worker before the snapshot restore and rolls nothing back on
// cancellation, so a cancel strands the worker. Retries still stop at the
// budget; each caller waits up to its own arrival + budget +
// CommittedAttemptWait and then gets a retryable 503 while the attempt runs on
// detached.
func TestActorResumer_CommittedAttempt(t *testing.T) {
	const (
		testActorName = "actor-committed"
		testAtespace  = "team-a"
		expectedIP    = "10.0.0.66"
	)
	testActorRef := resources.ActorRef{Atespace: testAtespace, Name: testActorName}
	runningActor := func() *ateapipb.ResumeActorResponse {
		return &ateapipb.ResumeActorResponse{
			Actor:   &ateapipb.Actor{Metadata: &ateapipb.ResourceMetadata{Name: testActorName}, Status: ateapipb.Actor_STATUS_RUNNING, WorkerAssignment: &ateapipb.WorkerAssignment{WorkerPodIp: expectedIP}},
			Resumed: true,
		}
	}

	t.Run("OvershootServedWithinWait", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			// The flake's shape: a saturated first attempt, then a restore that
			// overshoots the budget by a fraction. The old budget-cancels-RPC
			// behavior turned this into a 503 plus a stranded worker; now it is
			// a 200.
			const budget = 2 * time.Second
			var mu sync.Mutex
			var calls int
			var attemptStarts []time.Duration
			base := time.Now()
			mock := &resumerMockClient{
				resumeFn: func(ctx context.Context, in *ateapipb.ResumeActorRequest, opts ...grpc.CallOption) (*ateapipb.ResumeActorResponse, error) {
					mu.Lock()
					calls++
					n := calls
					attemptStarts = append(attemptStarts, time.Since(base))
					mu.Unlock()
					if n == 1 {
						return nil, status.Error(codes.FailedPrecondition, "no free workers available")
					}
					time.Sleep(budget) // lands past the budget, inside the wait
					return runningActor(), nil
				},
			}

			resumer := NewActorResumer(mock, withParking(ParkedRequestConfig{Max: 1, Budget: budget}))
			actor, _, err := resumer.ResumeActor(context.Background(), testActorRef)
			elapsed := time.Since(base)
			if err != nil {
				t.Fatalf("expected the overshooting resume to be served, got %v", err)
			}
			if actor.GetWorkerAssignment().GetWorkerPodIp() != expectedIP {
				t.Errorf("expected IP %q, got %q", expectedIP, actor.GetWorkerAssignment().GetWorkerPodIp())
			}
			if elapsed <= budget || elapsed >= budget+CommittedAttemptWait {
				t.Errorf("served after %v, want inside (budget, budget+wait) = (%v, %v)", elapsed, budget, budget+CommittedAttemptWait)
			}
			mu.Lock()
			defer mu.Unlock()
			for i, s := range attemptStarts {
				if s >= budget {
					t.Errorf("attempt %d started at %v: the wait must never buy another retry round", i+1, s)
				}
			}
		})
	})

	t.Run("AbandonedCallerGets503AtWaitBound", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			// An attempt that misses even the committed wait: the caller must
			// get the retryable-503 verdict at exactly budget+wait — never the
			// raw deadline error, which the HTTP boundary maps to a misleading
			// 504 and the metrics mislabel as `timeout` (the BudgetExhaustion
			// flake in run 30476804701) — and the RPC must keep running
			// uncancelled.
			const budget = 2 * time.Second
			proceed := make(chan struct{})
			var mu sync.Mutex
			var ctxErrAfterAbandon error
			var flightDone bool
			mock := &resumerMockClient{
				resumeFn: func(ctx context.Context, in *ateapipb.ResumeActorRequest, opts ...grpc.CallOption) (*ateapipb.ResumeActorResponse, error) {
					<-proceed
					mu.Lock()
					ctxErrAfterAbandon = ctx.Err()
					flightDone = true
					mu.Unlock()
					return runningActor(), nil
				},
			}

			resumer := NewActorResumer(mock, withParking(ParkedRequestConfig{Max: 1, Budget: budget}))
			base := time.Now()
			_, outcome, err := resumer.ResumeActor(context.Background(), testActorRef)
			elapsed := time.Since(base)

			if got := status.Code(err); got != codes.Unavailable {
				t.Errorf("expected Unavailable (retryable 503), got %v (err=%v)", got, err)
			}
			var budgetErr *budgetExhaustedError
			if !errors.As(err, &budgetErr) {
				t.Errorf("expected the error to be marked as budget exhaustion, got %T (%v)", err, err)
			}
			if got := parkOutcomeFor(err); got != parkOutcomeBudgetExhausted {
				t.Errorf("outcome = %q, want %q", got, parkOutcomeBudgetExhausted)
			}
			if outcome != ResumeOutcomeNone {
				t.Errorf("resume outcome = %q, want %q", outcome, ResumeOutcomeNone)
			}
			wantBound := budget + CommittedAttemptWait
			if elapsed < wantBound || elapsed > wantBound+100*time.Millisecond {
				t.Errorf("caller released after %v, want the %v wait bound", elapsed, wantBound)
			}

			// The decisive assertion for #675: the caller has been answered,
			// and the in-flight RPC was NOT cancelled — it completes normally.
			close(proceed)
			synctest.Wait()
			mu.Lock()
			defer mu.Unlock()
			if !flightDone {
				t.Fatal("the abandoned attempt never completed")
			}
			if ctxErrAfterAbandon != nil {
				t.Errorf("the abandoned attempt's context was cancelled (%v); the router must never cancel an in-flight resume", ctxErrAfterAbandon)
			}
		})
	})

	t.Run("JoinerSharesDetachedFlight", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			// The wait bound is per-caller, and the singleflight key stays held
			// while the attempt runs on: a joiner arriving mid-restore gets its
			// own full wait and is served by the SAME control-plane call the
			// leader started — no fresh ResumeActor racing the actor lock.
			const budget = 2 * time.Second
			var mu sync.Mutex
			var calls int
			mock := &resumerMockClient{
				resumeFn: func(ctx context.Context, in *ateapipb.ResumeActorRequest, opts ...grpc.CallOption) (*ateapipb.ResumeActorResponse, error) {
					mu.Lock()
					calls++
					mu.Unlock()
					time.Sleep(budget + 4*time.Second) // outlives the leader's bound (budget+3s), lands inside the joiner's
					return runningActor(), nil
				},
			}

			resumer := NewActorResumer(mock, withParking(ParkedRequestConfig{Max: 2, Budget: budget}))

			leaderErrCh := make(chan error, 1)
			go func() {
				_, _, err := resumer.ResumeActor(context.Background(), testActorRef)
				leaderErrCh <- err
			}()

			time.Sleep(3 * time.Second) // join mid-restore
			joinerBase := time.Now()
			actor, _, joinerErr := resumer.ResumeActor(context.Background(), testActorRef)
			joinerElapsed := time.Since(joinerBase)

			leaderErr := <-leaderErrCh
			if got := status.Code(leaderErr); got != codes.Unavailable {
				t.Errorf("leader: expected Unavailable at its wait bound, got %v (err=%v)", got, leaderErr)
			}
			var budgetErr *budgetExhaustedError
			if !errors.As(leaderErr, &budgetErr) {
				t.Errorf("leader: expected budget exhaustion, got %T (%v)", leaderErr, leaderErr)
			}
			if joinerErr != nil {
				t.Fatalf("joiner: expected to be served by the detached attempt, got %v", joinerErr)
			}
			if actor.GetWorkerAssignment().GetWorkerPodIp() != expectedIP {
				t.Errorf("joiner IP = %q, want %q", actor.GetWorkerAssignment().GetWorkerPodIp(), expectedIP)
			}
			if joinerBound := budget + CommittedAttemptWait; joinerElapsed >= joinerBound {
				t.Errorf("joiner waited %v, beyond its own %v bound", joinerElapsed, joinerBound)
			}
			mu.Lock()
			defer mu.Unlock()
			if calls != 1 {
				t.Errorf("expected the joiner to share the leader's single RPC, got %d calls", calls)
			}
		})
	})

	t.Run("LateDefinitiveAnswerIsNotExhaustion", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			// A definitive answer that lands after the budget — here the actor
			// was deleted while its resume attempt ran long — must reach the
			// caller as itself (404), never relabeled as budget exhaustion.
			const budget = 2 * time.Second
			var calls int
			var mu sync.Mutex
			mock := &resumerMockClient{
				resumeFn: func(ctx context.Context, in *ateapipb.ResumeActorRequest, opts ...grpc.CallOption) (*ateapipb.ResumeActorResponse, error) {
					mu.Lock()
					calls++
					n := calls
					mu.Unlock()
					if n == 1 {
						return nil, status.Error(codes.FailedPrecondition, "no free workers available")
					}
					time.Sleep(budget) // returns past the budget
					return nil, status.Error(codes.NotFound, "actor deleted meanwhile")
				},
			}

			resumer := NewActorResumer(mock, withParking(ParkedRequestConfig{Max: 1, Budget: budget}))
			_, _, err := resumer.ResumeActor(context.Background(), testActorRef)
			if got := status.Code(err); got != codes.NotFound {
				t.Errorf("expected the late NotFound to pass through, got %v (err=%v)", got, err)
			}
			var budgetErr *budgetExhaustedError
			if errors.As(err, &budgetErr) {
				t.Errorf("a definitive answer must never be relabeled as budget exhaustion (err=%v)", err)
			}
		})
	})

	t.Run("BareContextSentinelIsNotExhaustion", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			// An attempt error that happens to satisfy errors.Is(err,
			// context.Canceled) is a definitive answer from the attempt, not
			// the retry loop running out of time — it must not be wrapped as
			// exhaustion (which would turn it into a 503 with the capacity
			// body).
			mock := &resumerMockClient{
				resumeFn: func(ctx context.Context, in *ateapipb.ResumeActorRequest, opts ...grpc.CallOption) (*ateapipb.ResumeActorResponse, error) {
					return nil, context.Canceled
				},
			}

			resumer := NewActorResumer(mock, withParking(ParkedRequestConfig{Max: 1, Budget: 2 * time.Second}))
			_, _, err := resumer.ResumeActor(context.Background(), testActorRef)
			if !errors.Is(err, context.Canceled) {
				t.Errorf("expected the bare sentinel to pass through, got %v", err)
			}
			var budgetErr *budgetExhaustedError
			if errors.As(err, &budgetErr) {
				t.Errorf("a bare context sentinel from an attempt must not be relabeled as exhaustion (err=%v)", err)
			}
		})
	})

	t.Run("CeilingEndsWedgedFlightAsRetryable503", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			// A blackholed control-plane path: the first RPC never returns on
			// its own. The per-attempt ceiling is the only thing that ends the
			// flight and releases the singleflight key — and its verdict must
			// be the retryable incomplete 503, never a 504 manufactured by the
			// router's own backstop.
			const budget = 2 * time.Second
			var mu sync.Mutex
			var calls int
			var firstAttemptCtxErr error
			mock := &resumerMockClient{
				resumeFn: func(ctx context.Context, in *ateapipb.ResumeActorRequest, opts ...grpc.CallOption) (*ateapipb.ResumeActorResponse, error) {
					mu.Lock()
					calls++
					n := calls
					mu.Unlock()
					if n == 1 {
						<-ctx.Done() // wedged until the router's ceiling fires
						mu.Lock()
						firstAttemptCtxErr = ctx.Err()
						mu.Unlock()
						return nil, status.FromContextError(ctx.Err()).Err()
					}
					return runningActor(), nil
				},
			}

			resumer := NewActorResumer(mock, withParking(ParkedRequestConfig{Max: 2, Budget: budget}))

			// The leader gives up at its wait bound, long before the ceiling.
			base := time.Now()
			_, _, leaderErr := resumer.ResumeActor(context.Background(), testActorRef)
			if got := status.Code(leaderErr); got != codes.Unavailable {
				t.Fatalf("leader: expected the wait-bound 503, got %v", leaderErr)
			}

			// A joiner attached when the ceiling fires receives the flight's
			// ceiling verdict directly: budget-exhausted wrapping the
			// incomplete error (503), not the raw DeadlineExceeded (504).
			time.Sleep(resumeAttemptCeiling - time.Since(base) - time.Second)
			joinerErrCh := make(chan error, 1)
			go func() {
				_, _, err := resumer.ResumeActor(context.Background(), testActorRef)
				joinerErrCh <- err
			}()
			joinerErr := <-joinerErrCh
			if got := status.Code(joinerErr); got != codes.Unavailable {
				t.Errorf("joiner: expected Unavailable from the ceiling verdict, got %v (err=%v)", got, joinerErr)
			}
			var budgetErr *budgetExhaustedError
			if !errors.As(joinerErr, &budgetErr) {
				t.Errorf("joiner: ceiling expiry must classify as budget exhaustion, got %T (%v)", joinerErr, joinerErr)
			}

			// The ceiling — not anything else — ended the wedged attempt, and
			// the flight's end released the key: a fresh request starts a new
			// RPC and is served.
			mu.Lock()
			if firstAttemptCtxErr != context.DeadlineExceeded {
				t.Errorf("first attempt ctx.Err() = %v, want DeadlineExceeded from the ceiling", firstAttemptCtxErr)
			}
			mu.Unlock()
			actor, _, err := resumer.ResumeActor(context.Background(), testActorRef)
			if err != nil {
				t.Fatalf("post-ceiling request: expected a fresh flight to serve, got %v", err)
			}
			if actor.GetWorkerAssignment().GetWorkerPodIp() != expectedIP {
				t.Errorf("post-ceiling IP = %q, want %q", actor.GetWorkerAssignment().GetWorkerPodIp(), expectedIP)
			}
			mu.Lock()
			defer mu.Unlock()
			if calls != 2 {
				t.Errorf("expected the ceiling to release the key for exactly one fresh RPC, got %d calls", calls)
			}
		})
	})

	t.Run("DisabledNeverCancelsInFlight", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			// The cancel hazard is not parking-specific: with parking disabled
			// the fail-fast budget used to cut the RPC at 15s just the same.
			// The wait bound and the never-cancel contract apply in both modes.
			proceed := make(chan struct{})
			var mu sync.Mutex
			var ctxErrAfterAbandon error
			mock := &resumerMockClient{
				resumeFn: func(ctx context.Context, in *ateapipb.ResumeActorRequest, opts ...grpc.CallOption) (*ateapipb.ResumeActorResponse, error) {
					<-proceed
					mu.Lock()
					ctxErrAfterAbandon = ctx.Err()
					mu.Unlock()
					return runningActor(), nil
				},
			}

			resumer := NewActorResumer(mock)
			base := time.Now()
			_, _, err := resumer.ResumeActor(context.Background(), testActorRef)
			elapsed := time.Since(base)

			if got := status.Code(err); got != codes.Unavailable {
				t.Errorf("expected Unavailable, got %v (err=%v)", got, err)
			}
			wantBound := failFastResumeBudget + CommittedAttemptWait
			if elapsed < wantBound || elapsed > wantBound+100*time.Millisecond {
				t.Errorf("caller released after %v, want the %v wait bound", elapsed, wantBound)
			}

			close(proceed)
			synctest.Wait()
			mu.Lock()
			defer mu.Unlock()
			if ctxErrAfterAbandon != nil {
				t.Errorf("fail-fast mode cancelled the in-flight resume (%v); it must not", ctxErrAfterAbandon)
			}
		})
	})
}

// TestActorResumer_WaitFlights pins the shutdown seam: WaitFlights blocks
// while a flight — even one whose callers have all been answered — is still
// running, honors its context, and returns promptly once the flight lands.
func TestActorResumer_WaitFlights(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		proceed := make(chan struct{})
		mock := &resumerMockClient{
			resumeFn: func(ctx context.Context, in *ateapipb.ResumeActorRequest, opts ...grpc.CallOption) (*ateapipb.ResumeActorResponse, error) {
				<-proceed
				return &ateapipb.ResumeActorResponse{
					Actor: &ateapipb.Actor{Status: ateapipb.Actor_STATUS_RUNNING, WorkerAssignment: &ateapipb.WorkerAssignment{WorkerPodIp: "10.0.0.1"}},
				}, nil
			},
		}
		resumer := NewActorResumer(mock, withParking(ParkedRequestConfig{Max: 1, Budget: time.Second}))

		// The caller has been answered (wait bound), but the flight runs on.
		_, _, err := resumer.ResumeActor(context.Background(),
			resources.ActorRef{Atespace: "team-a", Name: "actor-wait-flights"})
		if status.Code(err) != codes.Unavailable {
			t.Fatalf("expected the abandoned caller's 503, got %v", err)
		}

		waitCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		if err := resumer.WaitFlights(waitCtx); err == nil {
			t.Error("WaitFlights returned nil while a detached flight was still running")
		}
		cancel()

		// The load-bearing path for shutdown: a PARKED waiter must be woken
		// by the last flight landing (flightFinished closing flightsIdle) —
		// not merely observe an already-idle tracker after the fact.
		parkedWait := make(chan error, 1)
		go func() {
			parkedWait <- resumer.WaitFlights(context.Background())
		}()
		synctest.Wait() // the waiter is durably parked on flightsIdle

		close(proceed)
		synctest.Wait()
		if err := <-parkedWait; err != nil {
			t.Errorf("parked WaitFlights = %v, want nil once the last flight lands", err)
		}
		if err := resumer.WaitFlights(context.Background()); err != nil {
			t.Errorf("WaitFlights after the flight landed = %v, want nil", err)
		}
	})
}

// TestHandlerWaitResumeFlightsDelegates pins the shutdown seam end to end at
// the handler layer: the drain sequence talks to ingress.Handler, and a
// regression that disconnects the delegation to the resumer would silently
// remove the detached-flight wait while both layer-local tests stay green.
func TestHandlerWaitResumeFlightsDelegates(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		proceed := make(chan struct{})
		mock := &resumerMockClient{
			resumeFn: func(ctx context.Context, in *ateapipb.ResumeActorRequest, opts ...grpc.CallOption) (*ateapipb.ResumeActorResponse, error) {
				<-proceed
				return &ateapipb.ResumeActorResponse{
					Actor: &ateapipb.Actor{Status: ateapipb.Actor_STATUS_RUNNING, WorkerAssignment: &ateapipb.WorkerAssignment{WorkerPodIp: "10.0.0.1"}},
				}, nil
			},
		}
		h := New(mock, ParkedRequestConfig{Max: 1, Budget: time.Second}, nil, false)

		// Drive a flight through the handler's own resumer; the caller is
		// answered at its wait bound while the flight runs on detached.
		_, _, err := h.resumer.ResumeActor(context.Background(),
			resources.ActorRef{Atespace: "team-a", Name: "actor-handler-seam"})
		if status.Code(err) != codes.Unavailable {
			t.Fatalf("expected the abandoned caller's 503, got %v", err)
		}

		waitCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		if err := h.WaitResumeFlights(waitCtx); err == nil {
			t.Error("WaitResumeFlights returned nil while a detached flight was still running")
		}
		cancel()

		close(proceed)
		synctest.Wait()
		if err := h.WaitResumeFlights(context.Background()); err != nil {
			t.Errorf("WaitResumeFlights after the flight landed = %v, want nil", err)
		}
	})
}

// TestActorResumer_DetachedCounter pins the wiring of the detached-attempt
// instrument end to end for BOTH label values: an attempt that runs past the
// flight's budget + committed wait increments
// atenet.router.parking.resume.detached with succeeded reflecting whether the
// late attempt landed. The two values drive opposite operator responses
// (benign late restore vs "needs a look"), so each side is pinned — the only
// visibility into detached attempts, since the parking gauge reads 0 once
// callers leave.
func TestActorResumer_DetachedCounter(t *testing.T) {
	runDetachedFlight := func(t *testing.T, succeed bool) metricdata.DataPoint[int64] {
		t.Helper()
		var point metricdata.DataPoint[int64]
		synctest.Test(t, func(t *testing.T) {
			const budget = time.Second
			reader := sdkmetric.NewManualReader()
			meter := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test")
			detached, err := meter.Int64Counter(parkingResumeDetachedMetricName)
			if err != nil {
				t.Fatalf("creating test counter: %v", err)
			}
			metrics := &ParkingMetrics{resumeDetached: detached}

			proceed := make(chan struct{})
			mock := &resumerMockClient{
				resumeFn: func(ctx context.Context, in *ateapipb.ResumeActorRequest, opts ...grpc.CallOption) (*ateapipb.ResumeActorResponse, error) {
					<-proceed
					time.Sleep(100 * time.Millisecond) // land strictly past budget+wait
					if !succeed {
						return nil, status.Error(codes.Unavailable, "restore failed after running long")
					}
					return &ateapipb.ResumeActorResponse{
						Actor:   &ateapipb.Actor{Status: ateapipb.Actor_STATUS_RUNNING, WorkerAssignment: &ateapipb.WorkerAssignment{WorkerPodIp: "10.0.0.1"}},
						Resumed: true,
					}, nil
				},
			}

			resumer := NewActorResumer(mock, withParking(ParkedRequestConfig{Max: 1, Budget: budget}), withMetrics(metrics))
			_, _, callErr := resumer.ResumeActor(context.Background(),
				resources.ActorRef{Atespace: "team-a", Name: "actor-detached"})
			if got := status.Code(callErr); got != codes.Unavailable {
				t.Fatalf("expected the abandoned caller's 503 verdict, got %v", callErr)
			}

			close(proceed)
			// Advance the fake clock past the mock's trailing sleep so the
			// flight lands strictly after budget+wait, then let it finish
			// recording.
			time.Sleep(200 * time.Millisecond)
			synctest.Wait()

			var rm metricdata.ResourceMetrics
			if err := reader.Collect(context.Background(), &rm); err != nil {
				t.Fatalf("collecting metrics: %v", err)
			}
			var points []metricdata.DataPoint[int64]
			for _, sm := range rm.ScopeMetrics {
				for _, m := range sm.Metrics {
					if m.Name != parkingResumeDetachedMetricName {
						continue
					}
					sum, ok := m.Data.(metricdata.Sum[int64])
					if !ok {
						t.Fatalf("unexpected data type %T", m.Data)
					}
					points = sum.DataPoints
				}
			}
			if len(points) != 1 {
				t.Fatalf("detached counter datapoints = %d, want 1", len(points))
			}
			if points[0].Value != 1 {
				t.Errorf("detached counter = %d, want 1", points[0].Value)
			}
			point = points[0]
		})
		return point
	}

	t.Run("FailedAttemptIsLabeledUnsucceeded", func(t *testing.T) {
		point := runDetachedFlight(t, false)
		if got, ok := point.Attributes.Value("succeeded"); !ok || got.AsBool() {
			t.Errorf("succeeded attribute = %v (present=%v), want false for a failed detached attempt", got, ok)
		}
	})

	t.Run("LateSuccessIsLabeledSucceeded", func(t *testing.T) {
		point := runDetachedFlight(t, true)
		if got, ok := point.Attributes.Value("succeeded"); !ok || !got.AsBool() {
			t.Errorf("succeeded attribute = %v (present=%v), want true for a restore that landed late", got, ok)
		}
	})
}

// TestActorResumer_CallerCancelDoesNotAbortFlight pins the detached-context
// contract from both sides: a caller that disconnects while parked gets
// context.Canceled (classified as the `canceled` outcome) WITHOUT aborting the
// shared in-flight resume, which keeps running and serves a later caller from
// the same single RPC.
func TestActorResumer_CallerCancelDoesNotAbortFlight(t *testing.T) {
	synctest.Test(t, testCallerCancelDoesNotAbortFlight)
}

func testCallerCancelDoesNotAbortFlight(t *testing.T) {
	const (
		testActorName = "actor-cancel"
		testAtespace  = "team-a"
		expectedIP    = "10.0.0.88"
	)
	testActorRef := resources.ActorRef{Atespace: testAtespace, Name: testActorName}

	var mu sync.Mutex
	var calls int
	started := make(chan struct{})
	proceed := make(chan struct{})
	mock := &resumerMockClient{
		resumeFn: func(ctx context.Context, in *ateapipb.ResumeActorRequest, opts ...grpc.CallOption) (*ateapipb.ResumeActorResponse, error) {
			mu.Lock()
			calls++
			n := calls
			mu.Unlock()
			if n == 1 {
				close(started)
			}
			// Hold the flight open until the test releases it.
			<-proceed
			return &ateapipb.ResumeActorResponse{
				Actor: &ateapipb.Actor{Metadata: &ateapipb.ResourceMetadata{Name: testActorName}, Status: ateapipb.Actor_STATUS_RUNNING, WorkerAssignment: &ateapipb.WorkerAssignment{WorkerPodIp: expectedIP}},
			}, nil
		},
	}

	resumer := NewActorResumer(mock, withParking(ParkedRequestConfig{Max: 2, Budget: 5 * time.Second}))

	// Caller 1 starts the flight, then disconnects while parked.
	ctx1, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, _, err := resumer.ResumeActor(ctx1, testActorRef)
		errCh <- err
	}()
	<-started
	cancel()

	err := <-errCh
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("disconnected caller: expected context.Canceled, got %v", err)
	}
	if got := parkOutcomeFor(err); got != parkOutcomeCanceled {
		t.Errorf("disconnected caller outcome = %q, want %q", got, parkOutcomeCanceled)
	}

	// Caller 2 arrives after caller 1 left; the flight is still in its first
	// RPC, so it must join that flight rather than start a new one.
	type result struct {
		actor *ateapipb.Actor
		err   error
	}
	resCh := make(chan result, 1)
	go func() {
		a, _, rerr := resumer.ResumeActor(context.Background(), testActorRef)
		resCh <- result{a, rerr}
	}()
	// Let caller 2 reach the flight before releasing it, so the call-count
	// assertion proves it shared the first RPC. Inside the bubble this is exact
	// rather than a hopeful sleep: Wait blocks until every other goroutine here
	// — caller 2 included — is durably blocked, i.e. parked on the flight.
	synctest.Wait()
	close(proceed)

	res := <-resCh
	if res.err != nil {
		t.Fatalf("second caller: unexpected error: %v", res.err)
	}
	if res.actor.GetWorkerAssignment().GetWorkerPodIp() != expectedIP {
		t.Errorf("second caller IP = %q, want %q", res.actor.GetWorkerAssignment().GetWorkerPodIp(), expectedIP)
	}
	mu.Lock()
	defer mu.Unlock()
	if calls != 1 {
		t.Errorf("expected the canceled caller's flight to be shared (1 RPC), got %d", calls)
	}
}

func TestResumeBackoffHasNoCap(t *testing.T) {
	// Regression: the resume backoff must NOT set wait.Backoff.Cap. delay() zeroes
	// Steps the moment the delay reaches Cap, which would end parking retries far
	// short of the budget (a 2s Cap stops the loop in ~7 steps / ~5s). The budget
	// context — not the step count or a cap — must bound how long a request parks.
	b := resumeBackoff(DefaultParkedRequestRetryInterval, DefaultParkedRequestRetryFactor, DefaultParkedRequestRetryJitter)
	if b.Cap != 0 {
		t.Errorf("resume backoff must not set Cap (it would stop retries at the cap); got %v", b.Cap)
	}
	if b.Steps < 1<<20 {
		t.Errorf("resume backoff Steps must be high so the budget bounds the wait; got %d", b.Steps)
	}
}
