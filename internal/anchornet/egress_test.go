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
	"io"
	"net"
	"testing"
	"time"

	"gvisor.dev/gvisor/pkg/tcpip"
	"gvisor.dev/gvisor/pkg/tcpip/adapters/gonet"
	"gvisor.dev/gvisor/pkg/tcpip/network/ipv4"
)

const (
	egressTCPPort = 15001
	egressDNSPort = 15053
)

// egressPair is an anchor stack with egress redirected and a sandbox stack
// routing everything through it, joined by an in-process relay.
func egressPair(t *testing.T) (anchor, sandbox *Stack) {
	t.Helper()
	anchor, err := NewEthernetStack("169.254.17.1", 30, "02:a8:1e:00:00:01")
	if err != nil {
		t.Fatal(err)
	}
	sandbox, err = NewEthernetStack("169.254.17.2", 30, "02:a8:1e:00:00:02")
	if err != nil {
		t.Fatal(err)
	}
	if err := sandbox.SetDefaultGateway("169.254.17.1"); err != nil {
		t.Fatal(err)
	}
	anchor.RedirectEgress(egressTCPPort, egressDNSPort)
	ctx, cancel := context.WithCancel(context.Background())
	go Relay(ctx, anchor.Link(), sandbox.Link())
	t.Cleanup(func() {
		cancel()
		anchor.Close()
		sandbox.Close()
	})
	return anchor, sandbox
}

// TestRedirectEgress_TCP: a connection the sandbox opens to a far address is
// accepted inside the anchor stack with its original destination intact, and
// data flows both ways with the sandbox seeing the far address as its peer.
func TestRedirectEgress_TCP(t *testing.T) {
	anchor, sandbox := egressPair(t)
	ln, err := anchor.ListenRedirectedTCP(egressTCPPort)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	accepted := make(chan *RedirectedConn, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		rc := c.(*RedirectedConn)
		accepted <- rc
		_, _ = io.Copy(rc, rc)
		rc.Close()
	}()

	far := tcpip.FullAddress{Addr: tcpip.AddrFrom4([4]byte{203, 0, 113, 9}), Port: 8080}
	dctx, dcancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer dcancel()
	conn, err := gonet.DialContextTCP(dctx, sandbox.Stack(), far, ipv4.ProtocolNumber)
	if err != nil {
		t.Fatalf("sandbox dial through the anchor: %v", err)
	}
	defer conn.Close()

	var rc *RedirectedConn
	select {
	case rc = <-accepted:
	case <-time.After(5 * time.Second):
		t.Fatal("redirected connection was not accepted")
	}
	if got := rc.OriginalDestination.String(); got != "203.0.113.9:8080" {
		t.Errorf("original destination = %s, want 203.0.113.9:8080", got)
	}
	if dst, err := OriginalDestination(rc); err != nil || dst != "203.0.113.9:8080" {
		t.Errorf("OriginalDestination = %q, %v", dst, err)
	}
	if got := conn.RemoteAddr().String(); got != "203.0.113.9:8080" {
		t.Errorf("sandbox peer = %s, want the far address", got)
	}
	if _, err := conn.Write([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 4)
	if _, err := io.ReadFull(conn, buf); err != nil || string(buf) != "ping" {
		t.Fatalf("echo = %q, %v", buf, err)
	}
	if _, err := OriginalDestination(conn); err == nil {
		t.Error("OriginalDestination accepted a plain connection")
	}
}

// TestServeDNS_ForwardsAndAnswersFromTheQueriedAddress: a query the sandbox
// sends to a far resolver is answered by the upstream, and the reply comes
// back from the far resolver's address, which a connected UDP socket
// requires.
func TestServeDNS_ForwardsAndAnswersFromTheQueriedAddress(t *testing.T) {
	anchor, sandbox := egressPair(t)

	upstream, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer upstream.Close()
	go func() {
		buf := make([]byte, 512)
		for {
			n, from, err := upstream.ReadFromUDP(buf)
			if err != nil {
				return
			}
			_, _ = upstream.WriteToUDP(append([]byte("answer:"), buf[:n]...), from)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = anchor.ServeDNS(ctx, egressDNSPort, upstream.LocalAddr().String(), nil) }()

	resolver := tcpip.FullAddress{Addr: tcpip.AddrFrom4([4]byte{10, 96, 0, 10}), Port: DNSPort}
	conn, err := gonet.DialUDP(sandbox.Stack(), nil, &resolver, ipv4.ProtocolNumber)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	// The forwarder binds in the background; a real resolver retries too.
	buf := make([]byte, 64)
	var n int
	for attempt := 0; attempt < 10; attempt++ {
		if _, err = conn.Write([]byte("query")); err != nil {
			time.Sleep(20 * time.Millisecond)
			continue
		}
		_ = conn.SetReadDeadline(time.Now().Add(time.Second))
		if n, err = conn.Read(buf); err == nil {
			break
		}
	}
	if err != nil {
		t.Fatalf("no reply from the far resolver address: %v", err)
	}
	if string(buf[:n]) != "answer:query" {
		t.Errorf("reply = %q", buf[:n])
	}
}
