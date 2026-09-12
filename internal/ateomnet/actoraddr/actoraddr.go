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

// Package actoraddr holds the constant addressing of an actor's network. Every
// worker builds the same point-to-point link, so these values are identical on
// every worker and inside every snapshot. That is what lets an actor's TCP
// state stay valid after a restore on a different worker, and what lets the
// connection anchor own the gateway address for every actor. It has no build
// constraints, so components that never touch a netns can share it.
package actoraddr

const (
	// HostVethName is the worker-side end of the actor veth.
	HostVethName = "ateom0"
	// ActorVethName is the sandbox-side end of the actor veth.
	ActorVethName = "eth0"

	HostVethCIDR     = "169.254.17.1/30"
	ActorVethCIDR    = "169.254.17.2/30"
	ActorVethGateway = "169.254.17.1"
	ActorVethIP      = "169.254.17.2"
	// ActorVethSubnet is the point-to-point /30 the actor veth lives on.
	ActorVethSubnet = "169.254.17.0/30"
	// PrefixLen is the /30 the actor and its gateway share.
	PrefixLen = 30

	// GatewayMAC is the fixed MAC of the actor's gateway. The micro-VM class
	// pins it so a restored guest's frozen ARP entry stays valid; the
	// connection anchor answers ARP with the same value.
	GatewayMAC = "02:a8:1e:00:00:01"
)
