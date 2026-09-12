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

// Package anchornet builds a userspace network stack for one actor, the way
// the connection anchor terminates that actor's TCP connections. The stack's
// single NIC is a channel-backed link whose frames are carried by the frame
// tunnel to the actor's sandbox. Because the endpoints live here and not on a
// worker, they survive the actor being suspended and resumed on any worker:
// only the tunnel underneath moves.
package anchornet

import (
	"context"
	"fmt"
	"net"

	"gvisor.dev/gvisor/pkg/tcpip"
	"gvisor.dev/gvisor/pkg/tcpip/header"
	"gvisor.dev/gvisor/pkg/tcpip/link/channel"
	"gvisor.dev/gvisor/pkg/tcpip/network/ipv4"
	"gvisor.dev/gvisor/pkg/tcpip/stack"
	"gvisor.dev/gvisor/pkg/tcpip/transport/tcp"
)

const (
	// nicID is the single NIC every actor stack has.
	nicID tcpip.NICID = 1
	// linkMTU is the link MTU. The shuttle disables offloads so frames stay at
	// this size.
	linkMTU = 1500
	// channelDepth bounds how many outbound frames wait for the tunnel.
	channelDepth = 256
)

// Stack is one actor's userspace network stack plus the link its frames flow
// through.
type Stack struct {
	stack *stack.Stack
	link  *channel.Endpoint
	addr  tcpip.Address
}

// NewStack builds a stack that owns localCIDR (for example 169.254.17.1/30 for
// the anchor side, or 169.254.17.2/30 for the sandbox side in a test), with a
// default route out of its NIC. The returned stack has no frame path until its
// Link is relayed to a peer.
func NewStack(localIP string, prefixLen int) (*Stack, error) {
	s := stack.New(stack.Options{
		NetworkProtocols:   []stack.NetworkProtocolFactory{ipv4.NewProtocol},
		TransportProtocols: []stack.TransportProtocolFactory{tcp.NewProtocol},
	})

	link := channel.New(channelDepth, linkMTU, "")

	if err := s.CreateNIC(nicID, link); err != nil {
		return nil, fmt.Errorf("anchornet: creating NIC: %s", err)
	}

	ip4 := net.ParseIP(localIP).To4()
	if ip4 == nil {
		return nil, fmt.Errorf("anchornet: %q is not an IPv4 address", localIP)
	}
	addr := tcpip.AddrFromSlice(ip4)
	protoAddr := tcpip.ProtocolAddress{
		Protocol:          ipv4.ProtocolNumber,
		AddressWithPrefix: tcpip.AddressWithPrefix{Address: addr, PrefixLen: prefixLen},
	}
	if err := s.AddProtocolAddress(nicID, protoAddr, stack.AddressProperties{}); err != nil {
		return nil, fmt.Errorf("anchornet: adding address %s: %s", localIP, err)
	}

	s.AddRoute(tcpip.Route{Destination: header.IPv4EmptySubnet, NIC: nicID})

	return &Stack{stack: s, link: link, addr: addr}, nil
}

// Stack returns the underlying tcpip stack, for the gonet dial and listen
// helpers.
func (s *Stack) Stack() *stack.Stack { return s.stack }

// Addr returns the stack's own address.
func (s *Stack) Addr() tcpip.Address { return s.addr }

// Link returns the channel link endpoint whose frames the tunnel carries.
func (s *Stack) Link() *channel.Endpoint { return s.link }

// Close tears the stack down and releases its resources.
func (s *Stack) Close() {
	s.link.Close()
	s.stack.Close()
	s.stack.Wait()
}

// Relay carries frames between two link endpoints in both directions until ctx
// is canceled. It is the in-process stand-in for the frame tunnel: detaching it
// (canceling ctx) models the actor suspending or its worker going away, and
// starting a fresh Relay models the tunnel reattaching, possibly on another
// worker. The persistent stacks keep their TCP state across the swap.
func Relay(ctx context.Context, a, b *channel.Endpoint) {
	done := make(chan struct{}, 2)
	go pump(ctx, a, b, done)
	go pump(ctx, b, a, done)
	<-done
	<-done
}

// pump moves outbound frames from src into dst as inbound, until ctx ends.
func pump(ctx context.Context, src, dst *channel.Endpoint, done chan<- struct{}) {
	defer func() { done <- struct{}{} }()
	for {
		pkt := src.ReadContext(ctx)
		if pkt == nil {
			return
		}
		proto := pkt.NetworkProtocolNumber
		out := stack.NewPacketBuffer(stack.PacketBufferOptions{
			Payload: pkt.ToBuffer(),
		})
		dst.InjectInbound(proto, out)
		out.DecRef()
		pkt.DecRef()
	}
}
