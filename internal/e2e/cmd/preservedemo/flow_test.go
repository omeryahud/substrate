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

package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestStageEventsCarryWhatTheFlowPageNeeds: stage events are tagged, timed,
// and delivered to subscribers; plain log lines carry no stage.
func TestStageEventsCarryWhatTheFlowPageNeeds(t *testing.T) {
	d := &demo{subscribers: map[chan event]struct{}{}}
	ch := make(chan event, 4)
	d.subscribers[ch] = struct{}{}

	d.stagef("request-sent", "egress", "Actor called http.Get(%q)", "http://x/delay?d=30s")
	d.emit(event{Stage: "state", Kind: "state", Text: "Actor is RUNNING on worker w1", State: "RUNNING", Worker: "w1"})
	d.logf("info", "plain line")

	got := []event{<-ch, <-ch, <-ch}
	if got[0].Stage != "request-sent" || got[0].TS == 0 || got[0].At == "" {
		t.Fatalf("stage event = %+v, want stage, ts and at set", got[0])
	}
	if got[1].State != "RUNNING" || got[1].Worker != "w1" {
		t.Fatalf("state event = %+v", got[1])
	}
	raw, err := json.Marshal(got[2])
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), `"stage"`) {
		t.Fatalf("plain log line carries a stage: %s", raw)
	}
	if len(d.log) != 3 {
		t.Fatalf("log kept %d events, want 3", len(d.log))
	}
}

// TestFlowPageKnowsEveryStage: the page handles each stage the demo emits.
func TestFlowPageKnowsEveryStage(t *testing.T) {
	rec := httptest.NewRecorder()
	serveFlow(rec, httptest.NewRequest(http.MethodGet, "/flow", nil))
	if rec.Code != http.StatusOK || !strings.Contains(rec.Header().Get("Content-Type"), "text/html") {
		t.Fatalf("flow page = %d %q", rec.Code, rec.Header().Get("Content-Type"))
	}
	body := rec.Body.String()
	for _, stage := range []string{"request-sent", "suspending", "suspended", "woken", "delivered", "done", "state"} {
		if !strings.Contains(body, "'"+stage+"'") {
			t.Errorf("flow page does not handle stage %q", stage)
		}
	}
	for _, id := range []string{"b-a", "b-b", "b-anchor", "b-gw", "b-echo", "b-api", "gate-box", "a-sh", "b-sh"} {
		if !strings.Contains(body, `id="`+id+`"`) {
			t.Errorf("flow page lacks component %q", id)
		}
	}
}
