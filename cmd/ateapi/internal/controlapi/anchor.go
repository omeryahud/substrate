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
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"

	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
)

const (
	// ConnectionPolicyAnnotation on an ActorTemplate selects how an Actor's
	// open TCP connections behave across Suspend and Resume.
	ConnectionPolicyAnnotation = "ate.dev/connection-policy"
	// ConnectionPolicyPreserve terminates the Actor's connections in the
	// connection anchor so they survive Suspend and Resume on any worker.
	ConnectionPolicyPreserve = "Preserve"
)

// AnchorConfig names the connection anchor's listeners. All three must be set
// for anchoring to be available; an empty config disables it cluster-wide,
// and templates without the Preserve annotation never use it.
type AnchorConfig struct {
	IngressAddress string
	AttachAddress  string
	ControlAddress string
}

func (c AnchorConfig) enabled() bool {
	return c.IngressAddress != "" && c.AttachAddress != "" && c.ControlAddress != ""
}

func preservesConnections(tmpl *atev1alpha1.ActorTemplate) bool {
	return tmpl != nil && tmpl.Annotations[ConnectionPolicyAnnotation] == ConnectionPolicyPreserve
}

// anchored reports whether an Actor of tmpl has its connections anchored.
func (w *ActorWorkflow) anchored(tmpl *atev1alpha1.ActorTemplate) bool {
	return w.anchor.enabled() && preservesConnections(tmpl)
}

// connectionAnchor is what the worker needs to attach the Actor's frame
// tunnel, or nil when the Actor uses the worker's local path.
func (w *ActorWorkflow) connectionAnchor(tmpl *atev1alpha1.ActorTemplate) *ateletpb.ConnectionAnchor {
	if !w.anchored(tmpl) {
		return nil
	}
	return &ateletpb.ConnectionAnchor{
		AttachAddress:  w.anchor.AttachAddress,
		ControlAddress: w.anchor.ControlAddress,
	}
}

// anchorIngressAddress is where the router sends the Actor's ingress, or empty
// when it goes to the worker.
func (w *ActorWorkflow) anchorIngressAddress(tmpl *atev1alpha1.ActorTemplate) string {
	if !w.anchored(tmpl) {
		return ""
	}
	return w.anchor.IngressAddress
}
