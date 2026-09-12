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
	"encoding/json"
	"fmt"

	"github.com/agent-substrate/substrate/internal/resources"
)

// BootKind tells the anchor how to treat an actor's held connections when a
// worker attaches.
type BootKind string

const (
	// BootRestore means the worker is restoring the actor from a snapshot, so
	// its held connections belong to it and must be reattached.
	BootRestore BootKind = "restore"
	// BootFresh means the worker is cold-booting a new actor process, so any
	// held connections are stale and the anchor must reset them.
	BootFresh BootKind = "fresh"
)

// AttachHeader is sent by a worker's frame shuttle when it opens the tunnel.
// It identifies the actor and the exact activation, so the anchor can bind the
// tunnel to the right per-actor stack and reject a stale or misdirected
// worker.
type AttachHeader struct {
	Atespace     string   `json:"atespace"`
	ActorName    string   `json:"actorName"`
	WorkerPodUID string   `json:"workerPodUID"`
	ActivationID string   `json:"activationID"`
	Boot         BootKind `json:"boot"`
}

// Valid reports whether the header names a well-formed actor, a worker, an
// activation, and a known boot kind. The anchor still checks the values
// against the control plane; this only rejects malformed input early.
func (h AttachHeader) Valid() error {
	if !resources.IsValidResourceName(h.Atespace) {
		return fmt.Errorf("anchortun: invalid atespace %q", h.Atespace)
	}
	if !resources.IsValidResourceName(h.ActorName) {
		return fmt.Errorf("anchortun: invalid actor name %q", h.ActorName)
	}
	if h.WorkerPodUID == "" {
		return fmt.Errorf("anchortun: empty worker pod UID")
	}
	if h.ActivationID == "" {
		return fmt.Errorf("anchortun: empty activation ID")
	}
	if h.Boot != BootRestore && h.Boot != BootFresh {
		return fmt.Errorf("anchortun: invalid boot kind %q", h.Boot)
	}
	return nil
}

// Ref returns the actor reference the header names.
func (h AttachHeader) Ref() resources.ActorRef {
	return resources.ActorRef{Atespace: h.Atespace, Name: h.ActorName}
}

// MarshalAttachHeader encodes a valid header for the tunnel handshake.
func MarshalAttachHeader(h AttachHeader) ([]byte, error) {
	if err := h.Valid(); err != nil {
		return nil, err
	}
	return json.Marshal(h)
}

// UnmarshalAttachHeader decodes and validates a header from the handshake.
func UnmarshalAttachHeader(b []byte) (AttachHeader, error) {
	var h AttachHeader
	if err := json.Unmarshal(b, &h); err != nil {
		return AttachHeader{}, fmt.Errorf("anchortun: decoding attach header: %w", err)
	}
	if err := h.Valid(); err != nil {
		return AttachHeader{}, err
	}
	return h, nil
}
