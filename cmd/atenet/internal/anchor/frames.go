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
	"fmt"
	"strings"

	"gvisor.dev/gvisor/pkg/tcpip/header"
	"gvisor.dev/gvisor/pkg/tcpip/stack"
)

// summarizeFrame renders one Ethernet frame as a short line for --log-frames:
// the link addresses, then the ARP or IPv4 header, then the TCP header.
func summarizeFrame(frame []byte) string {
	if len(frame) < header.EthernetMinimumSize {
		return fmt.Sprintf("short frame (%d bytes)", len(frame))
	}
	eth := header.Ethernet(frame)
	var b strings.Builder
	fmt.Fprintf(&b, "%s > %s", eth.SourceAddress(), eth.DestinationAddress())
	payload := frame[header.EthernetMinimumSize:]
	switch eth.Type() {
	case header.ARPProtocolNumber:
		summarizeARP(&b, payload)
	case header.IPv4ProtocolNumber:
		summarizeIPv4(&b, payload)
	default:
		fmt.Fprintf(&b, " type 0x%04x len %d", uint16(eth.Type()), len(payload))
	}
	return b.String()
}

func summarizeARP(b *strings.Builder, payload []byte) {
	arp := header.ARP(payload)
	if !arp.IsValid() {
		b.WriteString(" arp (invalid)")
		return
	}
	op := "reply"
	if arp.Op() == header.ARPRequest {
		op = "request"
	}
	fmt.Fprintf(b, " arp %s who-has %v tell %v (%s)", op,
		ipString(arp.ProtocolAddressTarget()), ipString(arp.ProtocolAddressSender()),
		macString(arp.HardwareAddressSender()))
}

func summarizeIPv4(b *strings.Builder, payload []byte) {
	ip := header.IPv4(payload)
	if len(payload) < header.IPv4MinimumSize || !ip.IsValid(len(payload)) {
		fmt.Fprintf(b, " ipv4 (invalid, %d bytes)", len(payload))
		return
	}
	fmt.Fprintf(b, " %s > %s", ip.SourceAddress(), ip.DestinationAddress())
	if ip.TransportProtocol() != header.TCPProtocolNumber {
		fmt.Fprintf(b, " proto %d len %d", ip.TransportProtocol(), len(ip.Payload()))
		return
	}
	tcp := header.TCP(ip.Payload())
	if len(tcp) < header.TCPMinimumSize {
		b.WriteString(" tcp (truncated)")
		return
	}
	dataLen := len(tcp) - int(tcp.DataOffset())
	if dataLen < 0 {
		dataLen = 0
	}
	fmt.Fprintf(b, " tcp %d > %d %s seq %d ack %d win %d len %d",
		tcp.SourcePort(), tcp.DestinationPort(), tcp.Flags(), tcp.SequenceNumber(), tcp.AckNumber(), tcp.WindowSize(), dataLen)
}

func ipString(b []byte) string {
	if len(b) != 4 {
		return fmt.Sprintf("%x", b)
	}
	return fmt.Sprintf("%d.%d.%d.%d", b[0], b[1], b[2], b[3])
}

func macString(b []byte) string {
	parts := make([]string, len(b))
	for i, x := range b {
		parts[i] = fmt.Sprintf("%02x", x)
	}
	return strings.Join(parts, ":")
}

// summarizeNeighbors renders an ARP cache for the attach and detach logs.
func summarizeNeighbors(entries []stack.NeighborEntry) []string {
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		out = append(out, fmt.Sprintf("%s=%s(%s)", e.Addr, e.LinkAddr, e.State))
	}
	return out
}
