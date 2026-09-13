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
	"log/slog"
	"net"
	"net/http"
	"strconv"

	"github.com/agent-substrate/substrate/internal/ateomnet/actoraddr"
	"github.com/agent-substrate/substrate/internal/atunnel"
)

// serveConnect is the raw CONNECT relay a worker's atunnel serves on its own
// CONNECT listener: the router's tunnel to an actor port becomes a TCP
// connection inside the actor's stack, so it is held across suspend like any
// other. The authority names the actor and the port.
func (a *Anchor) serveConnect(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodConnect {
		http.Error(w, "CONNECT required", http.StatusMethodNotAllowed)
		return
	}
	ref, ok := actorRefFromRequest(r)
	if !ok {
		a.reject(w, r, a.clock())
		return
	}
	entry := a.lookup(ref)
	if entry == nil {
		a.reject(w, r, a.clock())
		return
	}
	_, port, err := net.SplitHostPort(r.Host)
	if err != nil {
		http.Error(w, "CONNECT authority must include a port", http.StatusBadRequest)
		return
	}
	p, ok := atunnel.ParsePort(port)
	if !ok {
		http.Error(w, "invalid CONNECT port", http.StatusBadRequest)
		return
	}
	upstream, err := dialInStack(r.Context(), entry.stack, net.JoinHostPort(actoraddr.ActorVethIP, strconv.Itoa(p)))
	if err != nil {
		slog.WarnContext(r.Context(), "anchor CONNECT into the actor failed", slog.Any("actor", ref), slog.Int("port", p), slog.Any("err", err))
		http.Error(w, "bad gateway", http.StatusBadGateway)
		return
	}
	defer upstream.Close()
	a.metrics.recordConnect(r.Context())
	atunnel.RelayConnect(r.Context(), w, r, upstream)
}
