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

package anchor

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"sort"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// HoldConfig bounds what the anchor holds and how it wakes actors.
type HoldConfig struct {
	// MaxActors is the most actor stacks this anchor keeps, attached or held.
	MaxActors int
	// MaxConnections is the most TCP connections across all actor stacks.
	MaxConnections int
	// WakeInterval is the least time between two resume attempts for one
	// actor woken by data.
	WakeInterval time.Duration
	// UnquiesceAfter is how long after an attach held writes are let go when
	// no readiness probe reported the actor back sooner. A segment that
	// reaches a sandbox still being restored can be reset, so writes wait for
	// a sign of life.
	UnquiesceAfter time.Duration
}

// armResumeWrites schedules the release of held writes after an attach. A
// successful readiness probe releases them sooner through resumeWrites.
func (a *Anchor) armResumeWrites(entry *actorEntry, activationID string) {
	entry.mu.Lock()
	defer entry.mu.Unlock()
	if entry.resumeTimer != nil {
		entry.resumeTimer.Stop()
	}
	delay := a.cfg.Hold.UnquiesceAfter
	if delay <= 0 {
		delay = 3 * time.Second
	}
	entry.resumeTimer = time.AfterFunc(delay, func() {
		entry.mu.Lock()
		current := entry.activationID == activationID && entry.detach != nil
		entry.mu.Unlock()
		if current {
			entry.stack.Unquiesce()
		}
	})
}

// resumeWrites lets held writes go now: the actor answered a probe.
func (a *Anchor) resumeWrites(entry *actorEntry) {
	entry.mu.Lock()
	if entry.resumeTimer != nil {
		entry.resumeTimer.Stop()
		entry.resumeTimer = nil
	}
	entry.mu.Unlock()
	entry.stack.Unquiesce()
}

// tcpRetriesFor is how many retransmits a held connection may take before
// gVisor aborts it: enough to outlive the hold TTL at the maximum RTO, plus
// the ramp up to it.
func tcpRetriesFor(holdTTL time.Duration) uint64 {
	const maxRTO = 120 * time.Second
	const ramp = 20
	if holdTTL <= 0 {
		return 1 << 20
	}
	return uint64(holdTTL/maxRTO) + ramp
}

var (
	errTooManyActors      = errors.New("anchor: actor limit reached")
	errTooManyConnections = errors.New("anchor: connection limit reached")
)

// markHeld is the transition into the held state: writes toward the actor
// stop, and the first one that is held wakes the actor if its template asked
// for it. Nothing retransmits toward a sandbox that is not there.
func (a *Anchor) markHeld(entry *actorEntry) {
	entry.stack.Quiesce()
}

// wake asks the control plane to resume a held actor because data arrived
// for it. Attempts are rate limited per actor; the resume itself is
// idempotent for an actor that is already running or resuming.
func (a *Anchor) wake(entry *actorEntry) {
	if a.controlPlane == nil {
		return
	}
	entry.mu.Lock()
	if !entry.wakeOnData || entry.detach != nil || a.clock().Sub(entry.lastWake) < a.cfg.Hold.WakeInterval {
		entry.mu.Unlock()
		return
	}
	entry.lastWake = a.clock()
	entry.mu.Unlock()

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, err := a.controlPlane.ResumeActor(ctx, &ateapipb.ResumeActorRequest{
			Actor: &ateapipb.ObjectRef{Atespace: entry.ref.Atespace, Name: entry.ref.Name},
		})
		if err != nil {
			slog.WarnContext(ctx, "anchor: wake on data failed", slog.Any("actor", entry.ref), slog.Any("err", err))
			a.metrics.recordWake(ctx, outcomeFailed)
			return
		}
		slog.InfoContext(ctx, "anchor: woke actor on data", slog.Any("actor", entry.ref))
		a.metrics.recordWake(ctx, outcomeOK)
	}()
}

// connections counts the TCP connections established across all actor
// stacks. Callers hold a.mu.
func (a *Anchor) connectionsLocked() int {
	total := 0
	for _, e := range a.actors {
		total += int(e.stack.Stack().Stats().TCP.CurrentEstablished.Value())
	}
	return total
}

// connectionAllowed reports whether one more TCP connection fits under the
// anchor-wide cap, and counts the refusal when it does not.
func (a *Anchor) connectionAllowed(ctx context.Context) bool {
	if a.cfg.Hold.MaxConnections <= 0 {
		return true
	}
	a.mu.Lock()
	n := a.connectionsLocked()
	a.mu.Unlock()
	if n < a.cfg.Hold.MaxConnections {
		return true
	}
	a.metrics.recordCapRejection(ctx, "connections")
	return false
}

// actorStatus is one row of /statusz.
type actorStatus struct {
	Atespace      string    `json:"atespace"`
	Name          string    `json:"name"`
	ActorUID      string    `json:"actorUID"`
	Attached      bool      `json:"attached"`
	HeldSince     time.Time `json:"heldSince,omitempty"`
	ActivationID  string    `json:"activationID"`
	Connections   int       `json:"connections"`
	FramesToActor int64     `json:"framesToActor"`
	FramesFrom    int64     `json:"framesFromActor"`
	EgressGateway string    `json:"egressGateway,omitempty"`
	WakeOnData    bool      `json:"wakeOnData"`
}

type statusReport struct {
	Actors      []actorStatus `json:"actors"`
	Attached    int           `json:"attached"`
	Held        int           `json:"held"`
	Connections int           `json:"connections"`
	MaxActors   int           `json:"maxActors"`
	MaxConns    int           `json:"maxConnections"`
	HoldTTL     string        `json:"holdTTL"`
}

func (a *Anchor) status() statusReport {
	a.mu.Lock()
	defer a.mu.Unlock()
	report := statusReport{MaxActors: a.cfg.Hold.MaxActors, MaxConns: a.cfg.Hold.MaxConnections, HoldTTL: a.cfg.HoldTTL.String()}
	for _, e := range a.actors {
		e.mu.Lock()
		row := actorStatus{
			Atespace: e.ref.Atespace, Name: e.ref.Name, ActorUID: e.actorUID,
			Attached: e.detach != nil, HeldSince: e.heldSince, ActivationID: e.activationID,
			Connections:   int(e.stack.Stack().Stats().TCP.CurrentEstablished.Value()),
			FramesToActor: e.framesToActor.Load(), FramesFrom: e.framesFromActor.Load(),
			WakeOnData: e.wakeOnData,
		}
		e.mu.Unlock()
		if e.egress != nil {
			e.egress.mu.Lock()
			row.EgressGateway = e.egress.gateway
			e.egress.mu.Unlock()
		}
		if row.Attached {
			report.Attached++
		} else {
			report.Held++
		}
		report.Connections += row.Connections
		report.Actors = append(report.Actors, row)
	}
	sort.Slice(report.Actors, func(i, j int) bool {
		if report.Actors[i].Atespace != report.Actors[j].Atespace {
			return report.Actors[i].Atespace < report.Actors[j].Atespace
		}
		return report.Actors[i].Name < report.Actors[j].Name
	})
	return report
}

// serveStatusz answers with the anchor's actors as JSON.
func (a *Anchor) serveStatusz(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(a.status())
}
