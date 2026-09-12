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
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/agent-substrate/substrate/internal/atunnel"
	"github.com/agent-substrate/substrate/internal/resources"
)

func TestActorRefFromRequest(t *testing.T) {
	ref := resources.ActorRef{Atespace: "team-a", Name: "chatbot-1"}
	dns := resources.ActorDNSName(ref)
	for _, tc := range []struct {
		name   string
		host   string
		header string
		want   resources.ActorRef
		ok     bool
	}{
		{"hostOnly", dns, "", ref, true},
		{"hostWithPort", dns + ":80", "", ref, true},
		{"originalHostHeaderWins", "ignored.example", dns, ref, true},
		{"upperCase", "CHATBOT-1.TEAM-A.actors.resources.substrate.ate.dev", "", ref, true},
		{"notActor", "example.com", "", resources.ActorRef{}, false},
		{"badHostPort", "a:b:c", "", resources.ActorRef{}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, "/", nil)
			r.Host = tc.host
			if tc.header != "" {
				r.Header.Set(atunnel.OriginalHostHeader, tc.header)
			}
			got, ok := actorRefFromRequest(r)
			if ok != tc.ok || got != tc.want {
				t.Errorf("actorRefFromRequest = (%v, %v), want (%v, %v)", got, ok, tc.want, tc.ok)
			}
		})
	}
}

// TestGetOrCreate covers the hold contract: a restore reuses the held stack,
// a fresh boot replaces it, and unrelated actors do not share a stack.
func TestGetOrCreate(t *testing.T) {
	a := &Anchor{actors: map[resources.ActorRef]*actorEntry{}}
	ref := resources.ActorRef{Atespace: "team-a", Name: "chatbot-1"}

	first, err := a.getOrCreate(ref, false)
	if err != nil {
		t.Fatalf("first getOrCreate: %v", err)
	}
	defer first.stack.Close()

	again, err := a.getOrCreate(ref, false)
	if err != nil {
		t.Fatalf("restore getOrCreate: %v", err)
	}
	if again != first {
		t.Error("restore attach did not reuse the held stack")
	}

	fresh, err := a.getOrCreate(ref, true)
	if err != nil {
		t.Fatalf("fresh getOrCreate: %v", err)
	}
	defer fresh.stack.Close()
	if fresh == first {
		t.Error("fresh boot reused the held stack instead of replacing it")
	}

	other, err := a.getOrCreate(resources.ActorRef{Atespace: "team-a", Name: "other"}, false)
	if err != nil {
		t.Fatalf("other getOrCreate: %v", err)
	}
	defer other.stack.Close()
	if other == fresh {
		t.Error("two actors share one stack")
	}
	if a.lookup(ref) != fresh {
		t.Error("lookup does not return the current stack")
	}
}

// TestServeHTTP_UnknownActorIs421: ingress for an actor no worker attached is
// rejected as a stale assignment, the signal the router re-resolves on.
func TestServeHTTP_UnknownActorIs421(t *testing.T) {
	a := &Anchor{actors: map[resources.ActorRef]*actorEntry{}}
	r := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	r.Host = resources.ActorDNSName(resources.ActorRef{Atespace: "team-a", Name: "nobody"})
	w := httptest.NewRecorder()
	a.ServeHTTP(w, r)
	if w.Code != http.StatusMisdirectedRequest {
		t.Errorf("status = %d, want 421", w.Code)
	}
	if w.Header().Get(atunnel.StaleAssignmentHeader) == "" {
		t.Error("missing stale-assignment header")
	}
}
