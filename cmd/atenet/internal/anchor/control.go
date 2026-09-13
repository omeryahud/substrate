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
	"log/slog"
	"net/http"
	"time"

	"github.com/agent-substrate/substrate/internal/resources"
)

// maxQuiesceTimeout caps how long one quiesce request may hold the control
// connection.
const maxQuiesceTimeout = 5 * time.Second

// serveQuiesce stops feeding bytes toward an actor and waits, bounded, until
// the stack has nothing more in flight toward it. ateapi calls it right
// before the checkpoint, so nothing retransmits toward a frozen sandbox. The
// hold is lifted when a worker attaches again.
func (a *Anchor) serveQuiesce(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	q := r.URL.Query()
	ref := resources.ActorRef{Atespace: q.Get("atespace"), Name: q.Get("actor")}
	if !resources.IsValidResourceName(ref.Atespace) || !resources.IsValidResourceName(ref.Name) {
		http.Error(w, "bad actor reference", http.StatusBadRequest)
		return
	}
	timeout := 250 * time.Millisecond
	if raw := q.Get("timeout"); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil || d <= 0 {
			http.Error(w, "bad timeout", http.StatusBadRequest)
			return
		}
		timeout = min(d, maxQuiesceTimeout)
	}
	entry := a.lookup(ref)
	if entry == nil {
		http.Error(w, "actor not attached", http.StatusNotFound)
		return
	}
	entry.mu.Lock()
	activation := entry.activationID
	entry.mu.Unlock()
	if want := q.Get("activationID"); want != "" && want != activation {
		http.Error(w, "activation is not current", http.StatusConflict)
		return
	}
	start := a.clock()
	entry.stack.Quiesce()
	ctx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()
	drained := entry.stack.WaitDrained(ctx)
	a.metrics.recordQuiesce(r.Context(), a.clock().Sub(start), drained)
	slog.InfoContext(r.Context(), "anchor: actor quiesced", slog.Any("actor", ref), slog.Bool("drained", drained))
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"drained": drained})
}

// serveRelease drops an actor's stack, resetting every connection it held.
// ateapi calls it when the actor is deleted. Releasing an unknown actor
// succeeds: the outcome is the same.
func (a *Anchor) serveRelease(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	q := r.URL.Query()
	ref := resources.ActorRef{Atespace: q.Get("atespace"), Name: q.Get("actor")}
	if !resources.IsValidResourceName(ref.Atespace) || !resources.IsValidResourceName(ref.Name) {
		http.Error(w, "bad actor reference", http.StatusBadRequest)
		return
	}
	released := a.release(r.Context(), ref, q.Get("actorUID"))
	if released {
		slog.InfoContext(r.Context(), "anchor: actor released", slog.Any("actor", ref), slog.String("reason", q.Get("reason")))
	}
	a.metrics.recordRelease(r.Context(), released)
	w.WriteHeader(http.StatusOK)
}

// release drops the actor's stack when it exists and, if actorUID is given,
// belongs to that incarnation. It reports whether a stack was dropped.
func (a *Anchor) release(ctx context.Context, ref resources.ActorRef, actorUID string) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	e, ok := a.actors[ref]
	if !ok || (actorUID != "" && e.actorUID != actorUID) {
		return false
	}
	a.dropLocked(ctx, e)
	return true
}
