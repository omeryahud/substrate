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
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"regexp"

	"github.com/agent-substrate/substrate/internal/resources"
)

// workerPodUIDRE matches a Kubernetes pod UID, which is a UUID.
var workerPodUIDRE = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

// tokenRE bounds the actor UID and the activation id: short tokens of safe
// characters. The bound stops an untrusted shuttle from supplying an
// arbitrarily long or odd value.
var tokenRE = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)

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
	Atespace  string `json:"atespace"`
	ActorName string `json:"actorName"`
	// ActorUID tells one actor from a later one with the same name, so held
	// connections of a deleted actor are never handed to its namesake.
	ActorUID     string   `json:"actorUID"`
	WorkerPodUID string   `json:"workerPodUID"`
	ActivationID string   `json:"activationID"`
	Boot         BootKind `json:"boot"`
	// EgressGateway is the host:port of the egress gateway the anchor opens
	// the actor's outbound connections through. Empty leaves the actor
	// without egress, as the control plane decided.
	EgressGateway string `json:"egressGateway,omitempty"`
	// WakeOnData asks the anchor to resume the actor when data arrives for
	// it while it is suspended.
	WakeOnData bool `json:"wakeOnData,omitempty"`
}

// Valid reports whether the header names a well-formed actor, a worker, an
// activation, and a known boot kind. It bounds the format of every field so an
// untrusted shuttle cannot supply arbitrary or oversized input. It is not an
// authorization gate: the anchor must still cross-check the actor, the worker,
// and the activation against the control plane before it trusts the tunnel.
func (h AttachHeader) Valid() error {
	if !resources.IsValidResourceName(h.Atespace) {
		return fmt.Errorf("anchortun: invalid atespace %q", h.Atespace)
	}
	if !resources.IsValidResourceName(h.ActorName) {
		return fmt.Errorf("anchortun: invalid actor name %q", h.ActorName)
	}
	if !tokenRE.MatchString(h.ActorUID) {
		return fmt.Errorf("anchortun: invalid actor UID %q", h.ActorUID)
	}
	if !workerPodUIDRE.MatchString(h.WorkerPodUID) {
		return fmt.Errorf("anchortun: invalid worker pod UID %q", h.WorkerPodUID)
	}
	if !tokenRE.MatchString(h.ActivationID) {
		return fmt.Errorf("anchortun: invalid activation ID %q", h.ActivationID)
	}
	if h.Boot != BootRestore && h.Boot != BootFresh {
		return fmt.Errorf("anchortun: invalid boot kind %q", h.Boot)
	}
	if h.EgressGateway != "" {
		host, port, err := net.SplitHostPort(h.EgressGateway)
		if err != nil || host == "" || port == "" || len(h.EgressGateway) > 253 {
			return fmt.Errorf("anchortun: invalid egress gateway %q", h.EgressGateway)
		}
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

// UnmarshalAttachHeader decodes and validates a header from the handshake. It
// rejects unknown fields so a protocol mismatch fails loudly rather than being
// silently dropped.
func UnmarshalAttachHeader(b []byte) (AttachHeader, error) {
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	var h AttachHeader
	if err := dec.Decode(&h); err != nil {
		return AttachHeader{}, fmt.Errorf("anchortun: decoding attach header: %w", err)
	}
	// Reject trailing bytes after the object: a handshake carries exactly one
	// header, and json.Decoder alone would accept a second object or garbage.
	if err := dec.Decode(new(json.RawMessage)); err != io.EOF {
		return AttachHeader{}, fmt.Errorf("anchortun: unexpected trailing data after attach header")
	}
	if err := h.Valid(); err != nil {
		return AttachHeader{}, err
	}
	return h, nil
}
