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
	"testing"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storetest"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/go-cmp/cmp"
)

func candidateWorker(pod, namespace, pool, actorID string, runningSince int64) *ateapipb.Worker {
	return &ateapipb.Worker{
		WorkerNamespace:       namespace,
		WorkerPool:            pool,
		WorkerPod:             pod,
		ActorId:               actorID,
		RunningSinceUnixNanos: runningSince,
	}
}

// TestPreemptionCandidates verifies the victim-selection filtering and ordering.
func TestPreemptionCandidates(t *testing.T) {
	workers := []*ateapipb.Worker{
		candidateWorker("w-idle", "ns1", "pool1", "", 50),          // idle: excluded
		candidateWorker("w-self", "ns1", "pool1", "requester", 10), // requester: excluded
		candidateWorker("w-other-pool", "ns1", "pool2", "a", 10),   // wrong pool: excluded
		candidateWorker("w-other-ns", "ns2", "pool1", "b", 10),     // wrong namespace: excluded
		candidateWorker("w-new", "ns1", "pool1", "actor-new", 300), // candidate
		candidateWorker("w-zero", "ns1", "pool1", "actor-zero", 0), // candidate (unknown -> oldest)
		candidateWorker("w-old", "ns1", "pool1", "actor-old", 100), // candidate
		candidateWorker("w-mid", "ns1", "pool1", "actor-mid", 200), // candidate
	}

	got := preemptionCandidates(workers, "ns1", "pool1", "requester")

	var gotPods []string
	for _, w := range got {
		gotPods = append(gotPods, w.GetWorkerPod())
	}

	// Oldest-resident first; a zero running_since sorts as oldest of all.
	wantPods := []string{"w-zero", "w-old", "w-mid", "w-new"}
	if diff := cmp.Diff(wantPods, gotPods); diff != "" {
		t.Errorf("preemptionCandidates order mismatch (-want +got):\n%s", diff)
	}
}

func TestPreemptionCandidates_NoCandidates(t *testing.T) {
	workers := []*ateapipb.Worker{
		candidateWorker("w-idle", "ns1", "pool1", "", 0),
		candidateWorker("w-self", "ns1", "pool1", "requester", 0),
	}
	if got := preemptionCandidates(workers, "ns1", "pool1", "requester"); got != nil {
		t.Errorf("expected no candidates, got %d", len(got))
	}
}

// fakeSuspender emulates ActorWorkflow.SuspendActor against a real store: it
// frees the worker hosting the actor and marks the actor suspended, unless an
// error is programmed for that actor id.
type fakeSuspender struct {
	store store.Interface
	errs  map[string]error
	calls []string
}

func (f *fakeSuspender) SuspendActor(ctx context.Context, id string) (*ateapipb.Actor, error) {
	f.calls = append(f.calls, id)
	if err := f.errs[id]; err != nil {
		return nil, err
	}

	workers, err := f.store.ListWorkers(ctx)
	if err != nil {
		return nil, err
	}
	for _, w := range workers {
		if w.GetActorId() == id {
			w.ActorId = ""
			w.ActorNamespace = ""
			w.ActorTemplate = ""
			w.RunningSinceUnixNanos = 0
			if err := f.store.UpdateWorker(ctx, w, w.GetVersion()); err != nil {
				return nil, err
			}
			break
		}
	}

	actor, err := f.store.GetActor(ctx, id)
	if err != nil {
		return nil, err
	}
	actor.Status = ateapipb.Actor_STATUS_SUSPENDED
	actor.AteomPodNamespace = ""
	actor.AteomPodName = ""
	actor.AteomPodIp = ""
	if err := f.store.UpdateActor(ctx, actor, actor.GetVersion()); err != nil {
		return nil, err
	}
	return actor, nil
}

// seedRunningActor registers a worker hosting a RUNNING actor in the store.
func seedRunningActor(t *testing.T, s store.Interface, pod, actorID string, runningSince int64) {
	t.Helper()
	ctx := context.Background()
	if err := s.CreateWorker(ctx, candidateWorker(pod, "ns1", "pool1", actorID, runningSince)); err != nil {
		t.Fatalf("CreateWorker(%s): %v", pod, err)
	}
	if err := s.CreateActor(ctx, &ateapipb.Actor{
		ActorId:           actorID,
		Status:            ateapipb.Actor_STATUS_RUNNING,
		AteomPodNamespace: "ns1",
		AteomPodName:      pod,
	}); err != nil {
		t.Fatalf("CreateActor(%s): %v", actorID, err)
	}
}

// TestPreemptor_Preempt_FreesOldestVictim verifies the preemptor suspends the
// longest-resident running actor and returns its freed worker.
func TestPreemptor_Preempt_FreesOldestVictim(t *testing.T) {
	s, cleanup := storetest.SetupTestStore(t)
	defer cleanup()
	ctx := context.Background()

	seedRunningActor(t, s, "w-young", "actor-young", 300)
	seedRunningActor(t, s, "w-old", "actor-old", 100)

	suspender := &fakeSuspender{store: s}
	p := NewPreemptor(s, suspender)

	workers, err := s.ListWorkers(ctx)
	if err != nil {
		t.Fatalf("ListWorkers: %v", err)
	}

	freed, err := p.Preempt(ctx, workers, "ns1", "pool1", "requester")
	if err != nil {
		t.Fatalf("Preempt: %v", err)
	}
	if freed == nil {
		t.Fatal("expected a freed worker, got nil")
	}
	if freed.GetWorkerPod() != "w-old" {
		t.Errorf("expected oldest victim's worker w-old freed, got %q", freed.GetWorkerPod())
	}
	if freed.GetActorId() != "" {
		t.Errorf("expected freed worker to be idle, still has actor %q", freed.GetActorId())
	}
	if len(suspender.calls) != 1 || suspender.calls[0] != "actor-old" {
		t.Errorf("expected exactly one suspend of actor-old, got %v", suspender.calls)
	}
}

// TestPreemptor_Preempt_SkipsNonRunning verifies a candidate whose actor is not
// RUNNING is never suspended, and the preemptor moves to the next candidate.
func TestPreemptor_Preempt_SkipsNonRunning(t *testing.T) {
	s, cleanup := storetest.SetupTestStore(t)
	defer cleanup()
	ctx := context.Background()

	// Oldest candidate is mid-flight (RESUMING) and must be skipped.
	if err := s.CreateWorker(ctx, candidateWorker("w-resuming", "ns1", "pool1", "actor-resuming", 100)); err != nil {
		t.Fatalf("CreateWorker: %v", err)
	}
	if err := s.CreateActor(ctx, &ateapipb.Actor{
		ActorId:           "actor-resuming",
		Status:            ateapipb.Actor_STATUS_RESUMING,
		AteomPodNamespace: "ns1",
		AteomPodName:      "w-resuming",
	}); err != nil {
		t.Fatalf("CreateActor: %v", err)
	}
	seedRunningActor(t, s, "w-running", "actor-running", 200)

	suspender := &fakeSuspender{store: s}
	p := NewPreemptor(s, suspender)

	workers, err := s.ListWorkers(ctx)
	if err != nil {
		t.Fatalf("ListWorkers: %v", err)
	}

	freed, err := p.Preempt(ctx, workers, "ns1", "pool1", "requester")
	if err != nil {
		t.Fatalf("Preempt: %v", err)
	}
	if freed == nil || freed.GetWorkerPod() != "w-running" {
		t.Fatalf("expected w-running freed, got %v", freed)
	}
	if len(suspender.calls) != 1 || suspender.calls[0] != "actor-running" {
		t.Errorf("expected only actor-running to be suspended, got %v", suspender.calls)
	}
}

// TestPreemptor_Preempt_FallsThroughOnSuspendError verifies that when the
// preferred victim cannot be suspended (e.g. lock contention), the preemptor
// tries the next candidate instead of failing.
func TestPreemptor_Preempt_FallsThroughOnSuspendError(t *testing.T) {
	s, cleanup := storetest.SetupTestStore(t)
	defer cleanup()
	ctx := context.Background()

	seedRunningActor(t, s, "w-old", "actor-old", 100)
	seedRunningActor(t, s, "w-new", "actor-new", 200)

	suspender := &fakeSuspender{
		store: s,
		errs:  map[string]error{"actor-old": errors.New("lock conflict")},
	}
	p := NewPreemptor(s, suspender)

	workers, err := s.ListWorkers(ctx)
	if err != nil {
		t.Fatalf("ListWorkers: %v", err)
	}

	freed, err := p.Preempt(ctx, workers, "ns1", "pool1", "requester")
	if err != nil {
		t.Fatalf("Preempt: %v", err)
	}
	if freed == nil || freed.GetWorkerPod() != "w-new" {
		t.Fatalf("expected fallback to w-new, got %v", freed)
	}
	wantCalls := []string{"actor-old", "actor-new"}
	if diff := cmp.Diff(wantCalls, suspender.calls); diff != "" {
		t.Errorf("suspend call sequence mismatch (-want +got):\n%s", diff)
	}
}

// TestPreemptor_Preempt_NoVictims verifies that an empty/idle pool yields no
// preemption and no suspends.
func TestPreemptor_Preempt_NoVictims(t *testing.T) {
	s, cleanup := storetest.SetupTestStore(t)
	defer cleanup()
	ctx := context.Background()

	suspender := &fakeSuspender{store: s}
	p := NewPreemptor(s, suspender)

	freed, err := p.Preempt(ctx, nil, "ns1", "pool1", "requester")
	if err != nil {
		t.Fatalf("Preempt: %v", err)
	}
	if freed != nil {
		t.Errorf("expected no freed worker, got %v", freed)
	}
	if len(suspender.calls) != 0 {
		t.Errorf("expected no suspends, got %v", suspender.calls)
	}
}
