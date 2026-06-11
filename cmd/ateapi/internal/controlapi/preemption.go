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

package controlapi

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"sort"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// maxPreemptionCandidates bounds how many victim actors a single resume attempt
// will try to suspend before giving up. It keeps one ResumeActor call from
// spending its whole deadline thrashing on contended or unsuspendable
// candidates.
const maxPreemptionCandidates = 8

// victimSuspender suspends a running actor, checkpointing its state and freeing
// its worker. It is satisfied by *ActorWorkflow.
type victimSuspender interface {
	SuspendActor(ctx context.Context, id string) (*ateapipb.Actor, error)
}

// Preemptor frees a worker in a saturated pool by suspending a victim actor.
//
// Preemption is what lets Substrate honor its oversubscription promise under
// load. When every worker in a pool is occupied, a new resume would otherwise
// fail with "no free workers available". Instead the scheduler evicts a victim
// — checkpointing it first so no state is lost — and hands the reclaimed worker
// to the incoming actor. The victim is fully recoverable: its next request
// resumes it from the snapshot, possibly on a different worker.
type Preemptor struct {
	store     store.Interface
	suspender victimSuspender
}

// NewPreemptor builds a Preemptor that suspends victims via the given suspender.
func NewPreemptor(s store.Interface, suspender victimSuspender) *Preemptor {
	return &Preemptor{store: s, suspender: suspender}
}

// Preempt attempts to free exactly one worker in the given pool by suspending a
// RUNNING victim actor. It returns the freed worker (ready to be claimed by the
// caller) or nil if no worker could be preempted.
//
// workers is the snapshot the caller already loaded; Preempt re-reads the
// individual actor and worker records from the store before acting so it never
// operates on stale state. It never preempts the requesting actor itself.
func (p *Preemptor) Preempt(ctx context.Context, workers []*ateapipb.Worker, poolNamespace, poolName, requestingActorID string) (*ateapipb.Worker, error) {
	candidates := preemptionCandidates(workers, poolNamespace, poolName, requestingActorID)

	attempts := 0
	for _, candidate := range candidates {
		if attempts >= maxPreemptionCandidates {
			break
		}

		// Only RUNNING actors are safe to preempt. An actor that is mid-flight
		// (RESUMING/SUSPENDING) is being mutated by another workflow, and
		// suspending it would race. The worker record carries no status, so we
		// consult the authoritative actor record.
		victim, err := p.store.GetActor(ctx, candidate.GetActorId())
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				continue // worker points at an actor that is already gone; skip.
			}
			return nil, fmt.Errorf("while loading preemption candidate %q: %w", candidate.GetActorId(), err)
		}
		if victim.GetStatus() != ateapipb.Actor_STATUS_RUNNING {
			continue
		}

		attempts++

		// SuspendActor acquires its own per-actor lock, distinct from the
		// requesting actor's lock held by the enclosing resume, so there is no
		// self-deadlock. If the victim is concurrently busy the call returns
		// Aborted; we just move on to the next candidate.
		if _, err := p.suspender.SuspendActor(ctx, candidate.GetActorId()); err != nil {
			slog.InfoContext(ctx, "Preemption: failed to suspend victim, trying next candidate",
				slog.String("victim", candidate.GetActorId()),
				slog.String("worker", candidate.GetWorkerPod()),
				slog.Any("err", err))
			continue
		}

		// The victim is suspended and its worker should now be free. Re-read the
		// worker for a fresh version and to confirm nobody else claimed it in the
		// meantime.
		freed, err := p.store.GetWorker(ctx, candidate.GetWorkerNamespace(), candidate.GetWorkerPool(), candidate.GetWorkerPod())
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				continue // worker pod vanished (e.g. deleted); try another.
			}
			return nil, fmt.Errorf("while reloading preempted worker %q: %w", candidate.GetWorkerPod(), err)
		}
		if freed.GetActorId() != "" {
			// A concurrent resume grabbed the freed worker first. Keep trying.
			continue
		}

		slog.InfoContext(ctx, "Preemption: evicted victim to free a worker",
			slog.String("victim", candidate.GetActorId()),
			slog.String("requesting_actor", requestingActorID),
			slog.String("worker", candidate.GetWorkerPod()))
		return freed, nil
	}

	return nil, nil
}

// preemptionCandidates returns the workers in the given pool that host an actor
// other than the requester, ordered best-victim-first.
//
// Policy (v1): longest-resident eviction — the worker whose actor was assigned
// earliest (smallest running_since_unix_nanos) is preferred. A zero timestamp
// (worker assigned before this field existed, or a transient race) sorts as
// oldest, making it maximally preemptible. This approximates round-robin
// fairness across workers without requiring per-actor activity tracking; a
// future policy could instead evict by least-recent request activity.
func preemptionCandidates(workers []*ateapipb.Worker, poolNamespace, poolName, requestingActorID string) []*ateapipb.Worker {
	var candidates []*ateapipb.Worker
	for _, w := range workers {
		if w.GetWorkerNamespace() != poolNamespace || w.GetWorkerPool() != poolName {
			continue
		}
		if w.GetActorId() == "" || w.GetActorId() == requestingActorID {
			continue
		}
		candidates = append(candidates, w)
	}

	sort.SliceStable(candidates, func(i, j int) bool {
		return preemptionRank(candidates[i]) < preemptionRank(candidates[j])
	})
	return candidates
}

// preemptionRank maps a worker to its eviction priority key, where a smaller
// value means "evict sooner". A zero running_since is treated as the oldest
// possible value so freshly-migrated or pre-existing workers are preferred.
func preemptionRank(w *ateapipb.Worker) int64 {
	if w.GetRunningSinceUnixNanos() == 0 {
		return math.MinInt64
	}
	return w.GetRunningSinceUnixNanos()
}
