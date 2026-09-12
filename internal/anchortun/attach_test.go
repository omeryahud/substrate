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

package anchortun

import (
	"strings"
	"testing"
)

func validHeader() AttachHeader {
	return AttachHeader{
		Atespace:     "team-a",
		ActorName:    "chatbot-1",
		WorkerPodUID: "3fa9c1e2-0000-4444-8888-abcdefabcdef",
		ActivationID: "act-42",
		Boot:         BootRestore,
	}
}

func TestAttachHeader_RoundTrip(t *testing.T) {
	want := validHeader()
	b, err := MarshalAttachHeader(want)
	if err != nil {
		t.Fatalf("MarshalAttachHeader: %v", err)
	}
	got, err := UnmarshalAttachHeader(b)
	if err != nil {
		t.Fatalf("UnmarshalAttachHeader: %v", err)
	}
	if got != want {
		t.Errorf("round-trip mismatch: got %+v, want %+v", got, want)
	}
	if got.Ref() != want.Ref() {
		t.Errorf("Ref mismatch: got %v, want %v", got.Ref(), want.Ref())
	}
}

func TestAttachHeader_Valid(t *testing.T) {
	for _, tc := range []struct {
		name    string
		mutate  func(*AttachHeader)
		wantErr bool
	}{
		{"valid", func(*AttachHeader) {}, false},
		{"freshBoot", func(h *AttachHeader) { h.Boot = BootFresh }, false},
		{"emptyAtespace", func(h *AttachHeader) { h.Atespace = "" }, true},
		{"badAtespace", func(h *AttachHeader) { h.Atespace = "Not Valid" }, true},
		{"emptyActor", func(h *AttachHeader) { h.ActorName = "" }, true},
		{"emptyWorkerUID", func(h *AttachHeader) { h.WorkerPodUID = "" }, true},
		{"nonUUIDWorkerUID", func(h *AttachHeader) { h.WorkerPodUID = "worker-1" }, true},
		{"emptyActivation", func(h *AttachHeader) { h.ActivationID = "" }, true},
		{"tooLongActivation", func(h *AttachHeader) { h.ActivationID = strings.Repeat("a", 65) }, true},
		{"badCharsActivation", func(h *AttachHeader) { h.ActivationID = "act 42" }, true},
		{"emptyBoot", func(h *AttachHeader) { h.Boot = "" }, true},
		{"unknownBoot", func(h *AttachHeader) { h.Boot = "warm" }, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := validHeader()
			tc.mutate(&h)
			err := h.Valid()
			if tc.wantErr != (err != nil) {
				t.Errorf("Valid() error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

func TestUnmarshalAttachHeader_Rejects(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   string
	}{
		{"notJSON", "{not json"},
		{"invalidFields", `{"atespace":"","actorName":"","workerPodUID":"","activationID":"","boot":""}`},
		{"unknownBoot", `{"atespace":"a","actorName":"b","workerPodUID":"3fa9c1e2-0000-4444-8888-abcdefabcdef","activationID":"i","boot":"warm"}`},
		{"unknownField", `{"atespace":"a","actorName":"b","workerPodUID":"3fa9c1e2-0000-4444-8888-abcdefabcdef","activationID":"i","boot":"restore","extra":"x"}`},
		{"trailingGarbage", `{"atespace":"team-a","actorName":"chatbot-1","workerPodUID":"3fa9c1e2-0000-4444-8888-abcdefabcdef","activationID":"act-42","boot":"restore"}GARBAGE`},
		{"trailingObject", `{"atespace":"team-a","actorName":"chatbot-1","workerPodUID":"3fa9c1e2-0000-4444-8888-abcdefabcdef","activationID":"act-42","boot":"restore"}{"atespace":"x"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := UnmarshalAttachHeader([]byte(tc.in)); err == nil {
				t.Errorf("UnmarshalAttachHeader(%q) succeeded, want error", tc.in)
			}
		})
	}
}

func TestMarshalAttachHeader_RejectsInvalid(t *testing.T) {
	h := validHeader()
	h.ActivationID = ""
	if _, err := MarshalAttachHeader(h); err == nil {
		t.Error("MarshalAttachHeader with empty activation succeeded, want error")
	}
}
