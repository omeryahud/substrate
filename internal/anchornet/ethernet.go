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

package anchornet

import (
	"fmt"
	"net"

	"gvisor.dev/gvisor/pkg/tcpip"
	"gvisor.dev/gvisor/pkg/tcpip/header"
	"gvisor.dev/gvisor/pkg/tcpip/link/channel"
	"gvisor.dev/gvisor/pkg/tcpip/link/ethernet"
	"gvisor.dev/gvisor/pkg/tcpip/network/arp"
	"gvisor.dev/gvisor/pkg/tcpip/network/ipv4"
	"gvisor.dev/gvisor/pkg/tcpip/stack"
	"gvisor.dev/gvisor/pkg/tcpip/transport/tcp"
	"gvisor.dev/gvisor/pkg/tcpip/transport/udp"
)

// NewEthernetStack builds a stack whose NIC speaks Ethernet with a fixed MAC
// and answers ARP for localIP. This is the anchor's real configuration: the
// frames the worker's shuttle carries are Ethernet frames, and the sandbox
// resolves its gateway with ARP. The frames on Link include the Ethernet
// header in both directions.
func NewEthernetStack(localIP string, prefixLen int, mac string) (*Stack, error) {
	hw, err := net.ParseMAC(mac)
	if err != nil {
		return nil, fmt.Errorf("anchornet: parsing MAC %q: %w", mac, err)
	}
	s := stack.New(stack.Options{
		NetworkProtocols:   []stack.NetworkProtocolFactory{ipv4.NewProtocol, arp.NewProtocol},
		TransportProtocols: []stack.TransportProtocolFactory{tcp.NewProtocol, udp.NewProtocol},
	})

	ep := channel.New(channelDepth, linkMTU, tcpip.LinkAddress(hw))
	if err := s.CreateNIC(nicID, ethernet.New(ep)); err != nil {
		return nil, fmt.Errorf("anchornet: creating NIC: %s", err)
	}

	addr, err := addAddressAndDefaultRoute(s, localIP, prefixLen)
	if err != nil {
		return nil, err
	}
	return &Stack{stack: s, link: ep, addr: addr, prefixLen: prefixLen, gate: newWriteGate()}, nil
}

// addAddressAndDefaultRoute gives the NIC localIP and a default route out of it.
func addAddressAndDefaultRoute(s *stack.Stack, localIP string, prefixLen int) (tcpip.Address, error) {
	ip4 := net.ParseIP(localIP).To4()
	if ip4 == nil {
		return tcpip.Address{}, fmt.Errorf("anchornet: %q is not an IPv4 address", localIP)
	}
	addr := tcpip.AddrFromSlice(ip4)
	protoAddr := tcpip.ProtocolAddress{
		Protocol:          ipv4.ProtocolNumber,
		AddressWithPrefix: tcpip.AddressWithPrefix{Address: addr, PrefixLen: prefixLen},
	}
	if err := s.AddProtocolAddress(nicID, protoAddr, stack.AddressProperties{}); err != nil {
		return tcpip.Address{}, fmt.Errorf("anchornet: adding address %s: %s", localIP, err)
	}
	s.AddRoute(tcpip.Route{Destination: header.IPv4EmptySubnet, NIC: nicID})
	return addr, nil
}
