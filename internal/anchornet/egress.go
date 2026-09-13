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
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"gvisor.dev/gvisor/pkg/tcpip"
	"gvisor.dev/gvisor/pkg/tcpip/adapters/gonet"
	"gvisor.dev/gvisor/pkg/tcpip/header"
	"gvisor.dev/gvisor/pkg/tcpip/network/ipv4"
	"gvisor.dev/gvisor/pkg/tcpip/stack"
	"gvisor.dev/gvisor/pkg/tcpip/transport/tcp"
	"gvisor.dev/gvisor/pkg/waiter"
)

const (
	// DNSPort is the port RedirectEgress catches DNS on.
	DNSPort = 53
	// dnsTimeout bounds one forwarded query.
	dnsTimeout = 5 * time.Second
	// maxDNSMessage is the largest UDP DNS message.
	maxDNSMessage = 65535
)

// RedirectEgress makes the stack catch the sandbox's outbound traffic, the
// way the nftables redirect on a worker does: every TCP connection to an
// address other than the stack's own lands on tcpPort, and every UDP datagram
// to port 53 elsewhere lands on dnsPort, both at the stack's own address. The
// original destination of a redirected TCP connection is available through
// ListenRedirectedTCP. Replies are rewritten back, so the sandbox sees them
// come from the address it talked to.
func (s *Stack) RedirectEgress(tcpPort, dnsPort uint16) {
	all := tcpip.AddrFrom4([4]byte{255, 255, 255, 255})
	notLocal := func(proto tcpip.TransportProtocolNumber) stack.IPHeaderFilter {
		f := stack.EmptyFilter4()
		f.Protocol = proto
		f.CheckProtocol = true
		f.Dst = s.addr
		f.DstMask = all
		f.DstInvert = true
		return f
	}
	accept := stack.Rule{Filter: stack.EmptyFilter4(), Target: &stack.AcceptTarget{NetworkProtocol: ipv4.ProtocolNumber}}
	table := stack.Table{
		Rules: []stack.Rule{
			{Filter: notLocal(header.TCPProtocolNumber), Target: &stack.RedirectTarget{Port: tcpPort, NetworkProtocol: ipv4.ProtocolNumber}},
			{Filter: notLocal(header.UDPProtocolNumber), Matchers: []stack.Matcher{udpDestinationPort(DNSPort)}, Target: &stack.RedirectTarget{Port: dnsPort, NetworkProtocol: ipv4.ProtocolNumber}},
			accept,
			accept,
			accept,
			accept,
			{Filter: stack.EmptyFilter4(), Target: &stack.ErrorTarget{NetworkProtocol: ipv4.ProtocolNumber}},
		},
		BuiltinChains: [stack.NumHooks]int{stack.Prerouting: 0, stack.Input: 3, stack.Forward: stack.HookUnset, stack.Output: 4, stack.Postrouting: 5},
		Underflows:    [stack.NumHooks]int{stack.Prerouting: 2, stack.Input: 3, stack.Forward: stack.HookUnset, stack.Output: 4, stack.Postrouting: 5},
	}
	s.stack.IPTables().ReplaceTable(stack.NATID, table, false)
}

// udpDestinationPort matches UDP datagrams to one port.
type udpDestinationPort uint16

func (p udpDestinationPort) Match(_ stack.Hook, pkt *stack.PacketBuffer, _, _ string) (bool, bool) {
	hdr := header.UDP(pkt.TransportHeader().Slice())
	if len(hdr) < header.UDPMinimumSize {
		return false, false
	}
	return hdr.DestinationPort() == uint16(p), false
}

// SetDefaultGateway routes everything off-link through gateway. The anchor's
// stack does not need it; a stack standing in for a sandbox does, because a
// real sandbox routes through the anchor's address.
func (s *Stack) SetDefaultGateway(gateway string) error {
	gw := net.ParseIP(gateway).To4()
	if gw == nil {
		return fmt.Errorf("anchornet: %q is not an IPv4 address", gateway)
	}
	onLink := tcpip.AddressWithPrefix{Address: s.addr, PrefixLen: s.prefixLen}.Subnet()
	s.stack.SetRouteTable([]tcpip.Route{
		{Destination: onLink, NIC: nicID},
		{Destination: header.IPv4EmptySubnet, Gateway: tcpip.AddrFromSlice(gw), NIC: nicID},
	})
	return nil
}

// RedirectedConn is a connection the sandbox opened to somewhere else that
// RedirectEgress delivered to this stack.
type RedirectedConn struct {
	net.Conn
	OriginalDestination *net.TCPAddr
}

// OriginalDestination reports where a RedirectedConn was going. It has the
// shape atunnel's egress proxy expects.
func OriginalDestination(conn net.Conn) (string, error) {
	rc, ok := conn.(*RedirectedConn)
	if !ok {
		return "", errors.New("anchornet: not a redirected connection")
	}
	return rc.OriginalDestination.String(), nil
}

// ListenRedirectedTCP accepts the TCP connections RedirectEgress sends to
// port, each wrapped in a RedirectedConn that carries its original
// destination.
func (s *Stack) ListenRedirectedTCP(port uint16) (net.Listener, error) {
	var wq waiter.Queue
	ep, terr := s.stack.NewEndpoint(tcp.ProtocolNumber, ipv4.ProtocolNumber, &wq)
	if terr != nil {
		return nil, fmt.Errorf("anchornet: redirect endpoint: %s", terr)
	}
	if terr := ep.Bind(tcpip.FullAddress{NIC: nicID, Addr: s.addr, Port: port}); terr != nil {
		ep.Close()
		return nil, fmt.Errorf("anchornet: binding redirect listener: %s", terr)
	}
	if terr := ep.Listen(128); terr != nil {
		ep.Close()
		return nil, fmt.Errorf("anchornet: redirect listen: %s", terr)
	}
	l := &redirectListener{addr: &net.TCPAddr{IP: net.IP(s.addr.AsSlice()), Port: int(port)}, wq: &wq, ep: ep, closed: make(chan struct{})}
	l.entry, l.notify = waiter.NewChannelEntry(waiter.ReadableEvents)
	wq.EventRegister(&l.entry)
	return l, nil
}

type redirectListener struct {
	addr   *net.TCPAddr
	wq     *waiter.Queue
	ep     tcpip.Endpoint
	entry  waiter.Entry
	notify chan struct{}
	closed chan struct{}
	once   sync.Once
}

func (l *redirectListener) Accept() (net.Conn, error) {
	for {
		ep, wq, terr := l.ep.Accept(nil)
		if _, wouldBlock := terr.(*tcpip.ErrWouldBlock); wouldBlock {
			select {
			case <-l.notify:
				continue
			case <-l.closed:
				return nil, net.ErrClosed
			}
		}
		if terr != nil {
			return nil, &net.OpError{Op: "accept", Net: "tcp", Addr: l.addr, Err: errors.New(terr.String())}
		}
		var original tcpip.OriginalDestinationOption
		if terr := ep.GetSockOpt(&original); terr != nil {
			// Not redirected: the sandbox dialed this port itself. Nothing
			// legitimate does that, so refuse it rather than guess a target.
			ep.Abort()
			continue
		}
		return &RedirectedConn{
			Conn:                gonet.NewTCPConn(wq, ep),
			OriginalDestination: &net.TCPAddr{IP: net.IP(original.Addr.AsSlice()), Port: int(original.Port)},
		}, nil
	}
}

func (l *redirectListener) Close() error {
	l.once.Do(func() {
		close(l.closed)
		l.wq.EventUnregister(&l.entry)
		l.ep.Close()
	})
	return nil
}

func (l *redirectListener) Addr() net.Addr { return l.addr }

// ServeDNS answers the DNS queries RedirectEgress sends to port by forwarding
// each one to upstream (host:port) from this process's own network and
// relaying the reply. observe, if not nil, is told whether each query got an
// answer. It returns when ctx ends. UDP is not held: a query that arrives
// while no worker is attached is lost, and resolvers retry.
func (s *Stack) ServeDNS(ctx context.Context, port uint16, upstream string, observe func(answered bool)) error {
	conn, err := gonet.DialUDP(s.stack, &tcpip.FullAddress{NIC: nicID, Addr: s.addr, Port: port}, nil, ipv4.ProtocolNumber)
	if err != nil {
		return fmt.Errorf("anchornet: binding DNS forwarder: %w", err)
	}
	stop := context.AfterFunc(ctx, func() { conn.Close() })
	defer stop()
	buf := make([]byte, maxDNSMessage)
	for {
		n, from, err := conn.ReadFrom(buf)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("anchornet: reading DNS query: %w", err)
		}
		query := append([]byte(nil), buf[:n]...)
		go func() {
			answered := forwardDNS(conn, from, query, upstream)
			if observe != nil {
				observe(answered)
			}
		}()
	}
}

func forwardDNS(conn *gonet.UDPConn, from net.Addr, query []byte, upstream string) bool {
	up, err := net.DialTimeout("udp", upstream, dnsTimeout)
	if err != nil {
		return false
	}
	defer up.Close()
	_ = up.SetDeadline(time.Now().Add(dnsTimeout))
	if _, err := up.Write(query); err != nil {
		return false
	}
	reply := make([]byte, maxDNSMessage)
	n, err := up.Read(reply)
	if err != nil {
		return false
	}
	_, err = conn.WriteTo(reply[:n], from)
	return err == nil
}
