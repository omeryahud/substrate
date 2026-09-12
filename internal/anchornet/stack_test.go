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

const testPort = 9000

func mustWrite(t *testing.T, c net.Conn, s string) {
	t.Helper()
	if err := c.SetWriteDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatalf("SetWriteDeadline: %v", err)
	}
	if _, err := c.Write([]byte(s)); err != nil {
		t.Fatalf("Write %q: %v", s, err)
	}
}

func mustRead(t *testing.T, c net.Conn, want string) {
	t.Helper()
	if err := c.SetReadDeadline(time.Now().Add(20 * time.Second)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	buf := make([]byte, len(want))
	if _, err := io.ReadFull(c, buf); err != nil {
		t.Fatalf("ReadFull(want %q): %v", want, err)
	}
	if string(buf) != want {
		t.Fatalf("read %q, want %q", buf, want)
	}
}

// TestConnectionSurvivesTunnelSwap is the core claim of connection
// preservation, at the anchor: a TCP connection terminated in the anchor stack
// keeps working when the frame tunnel to the sandbox is torn down and a fresh
// one is attached, which is what happens when the actor is suspended and
// resumed on another worker. The two stacks stand for the anchor and the
// restored sandbox; only the relay between them is replaced. Data sent while
// the tunnel is down is delivered after it returns, with no loss or reorder.
func TestConnectionSurvivesTunnelSwap(t *testing.T) {
	anchorStack, err := NewStack("169.254.17.1", 30)
	if err != nil {
		t.Fatalf("anchor NewStack: %v", err)
	}
	defer anchorStack.Close()
	sandboxStack, err := NewStack("169.254.17.2", 30)
	if err != nil {
		t.Fatalf("sandbox NewStack: %v", err)
	}
	defer sandboxStack.Close()

	// The first tunnel joins the two stacks.
	ctx1, detach1 := context.WithCancel(context.Background())
	go Relay(ctx1, anchorStack.Link(), sandboxStack.Link())

	addr := tcpip.FullAddress{Addr: anchorStack.Addr(), Port: testPort}
	ln, err := gonet.ListenTCP(anchorStack.Stack(), addr, ipv4.ProtocolNumber)
	if err != nil {
		t.Fatalf("ListenTCP: %v", err)
	}
	defer ln.Close()

	accepted := make(chan net.Conn, 1)
	acceptErr := make(chan error, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			acceptErr <- err
			return
		}
		accepted <- c
	}()

	dialCtx, cancelDial := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancelDial()
	client, err := gonet.DialContextTCP(dialCtx, sandboxStack.Stack(), addr, ipv4.ProtocolNumber)
	if err != nil {
		t.Fatalf("DialContextTCP: %v", err)
	}
	defer client.Close()

	var server net.Conn
	select {
	case server = <-accepted:
	case err := <-acceptErr:
		t.Fatalf("Accept: %v", err)
	case <-time.After(20 * time.Second):
		t.Fatal("Accept timed out over the first tunnel")
	}
	defer server.Close()

	// Baseline exchange over the first tunnel.
	mustWrite(t, client, "hello-1")
	mustRead(t, server, "hello-1")
	mustWrite(t, server, "reply-1")
	mustRead(t, client, "reply-1")

	// Suspend: tear the tunnel down, then send while it is down. The bytes sit
	// in the sender's link queue and TCP retransmits until a tunnel returns.
	detach1()
	mustWrite(t, client, "sent-while-suspended")

	// Resume, possibly on another worker: a fresh tunnel between the same
	// persistent stacks.
	ctx2, detach2 := context.WithCancel(context.Background())
	defer detach2()
	go Relay(ctx2, anchorStack.Link(), sandboxStack.Link())

	// The data sent while suspended arrives, and the connection keeps working
	// in both directions.
	mustRead(t, server, "sent-while-suspended")
	mustWrite(t, server, "reply-after-resume")
	mustRead(t, client, "reply-after-resume")
	mustWrite(t, client, "hello-after-resume")
	mustRead(t, server, "hello-after-resume")
}

// TestConnectionSurvivesFramedTunnelReconnect is the same claim carried over
// the real anchortun frame codec rather than a raw packet relay: the tunnel is
// a byte stream (net.Pipe) that the shuttle and the anchor each bridge to their
// stack. Tearing the stream down and dialing a fresh one, as a reconnect on a
// new worker does, preserves the TCP connection.
func TestConnectionSurvivesFramedTunnelReconnect(t *testing.T) {
	anchorStack, err := NewStack("169.254.17.1", 30)
	if err != nil {
		t.Fatalf("anchor NewStack: %v", err)
	}
	defer anchorStack.Close()
	sandboxStack, err := NewStack("169.254.17.2", 30)
	if err != nil {
		t.Fatalf("sandbox NewStack: %v", err)
	}
	defer sandboxStack.Close()

	// connectTunnel wires a fresh net.Pipe between the two links via Bridge,
	// standing for the frame tunnel. It returns a cancel that tears it down.
	connectTunnel := func() context.CancelFunc {
		anchorEnd, sandboxEnd := net.Pipe()
		ctx, cancel := context.WithCancel(context.Background())
		go func() { _ = Bridge(ctx, anchorStack.Link(), anchorEnd) }()
		go func() { _ = Bridge(ctx, sandboxStack.Link(), sandboxEnd) }()
		return func() {
			cancel()
			anchorEnd.Close()
			sandboxEnd.Close()
		}
	}

	detach1 := connectTunnel()

	addr := tcpip.FullAddress{Addr: anchorStack.Addr(), Port: testPort}
	ln, err := gonet.ListenTCP(anchorStack.Stack(), addr, ipv4.ProtocolNumber)
	if err != nil {
		t.Fatalf("ListenTCP: %v", err)
	}
	defer ln.Close()

	accepted := make(chan net.Conn, 1)
	acceptErr := make(chan error, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			acceptErr <- err
			return
		}
		accepted <- c
	}()

	dialCtx, cancelDial := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancelDial()
	client, err := gonet.DialContextTCP(dialCtx, sandboxStack.Stack(), addr, ipv4.ProtocolNumber)
	if err != nil {
		t.Fatalf("DialContextTCP: %v", err)
	}
	defer client.Close()

	var server net.Conn
	select {
	case server = <-accepted:
	case err := <-acceptErr:
		t.Fatalf("Accept: %v", err)
	case <-time.After(20 * time.Second):
		t.Fatal("Accept timed out over the framed tunnel")
	}
	defer server.Close()

	mustWrite(t, client, "framed-1")
	mustRead(t, server, "framed-1")

	// Reconnect the tunnel: tear the stream down and dial a fresh one.
	detach1()
	detach2 := connectTunnel()
	defer detach2()

	mustWrite(t, client, "framed-after-reconnect")
	mustRead(t, server, "framed-after-reconnect")
	mustWrite(t, server, "framed-reply")
	mustRead(t, client, "framed-reply")
}
