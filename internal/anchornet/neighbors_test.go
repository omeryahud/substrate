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
	"sync/atomic"
	"testing"
	"time"

	"gvisor.dev/gvisor/pkg/tcpip"
	"gvisor.dev/gvisor/pkg/tcpip/adapters/gonet"
	"gvisor.dev/gvisor/pkg/tcpip/network/ipv4"

	"github.com/agent-substrate/substrate/internal/anchortun"
)

// TestForgetNeighbors_ResolvesThePeerAgain: after the ARP cache is dropped, a
// new connection resolves the peer's current MAC and completes, which is what
// a reattached sandbox with a new veth MAC needs.
func TestForgetNeighbors_ResolvesThePeerAgain(t *testing.T) {
	anchor, err := NewEthernetStack("169.254.17.1", 30, "02:a8:1e:00:00:01")
	if err != nil {
		t.Fatal(err)
	}
	defer anchor.Close()
	sandbox, err := NewEthernetStack("169.254.17.2", 30, "02:a8:1e:00:00:02")
	if err != nil {
		t.Fatal(err)
	}
	defer sandbox.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go Relay(ctx, anchor.Link(), sandbox.Link())

	ln, err := gonet.ListenTCP(sandbox.Stack(), tcpip.FullAddress{Addr: sandbox.Addr(), Port: 80}, ipv4.ProtocolNumber)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			c.Close()
		}
	}()

	dial := func() {
		t.Helper()
		dctx, dcancel := context.WithTimeout(ctx, 5*time.Second)
		defer dcancel()
		c, err := gonet.DialContextTCP(dctx, anchor.Stack(), tcpip.FullAddress{Addr: sandbox.Addr(), Port: 80}, ipv4.ProtocolNumber)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		c.Close()
	}
	dial()
	if len(anchor.Neighbors()) == 0 {
		t.Fatal("no neighbor learned by the first connection")
	}
	anchor.ForgetNeighbors()
	if got := anchor.Neighbors(); len(got) != 0 {
		t.Fatalf("neighbors after ForgetNeighbors = %v, want none", got)
	}
	dial()
	if len(anchor.Neighbors()) == 0 {
		t.Fatal("peer was not resolved again after ForgetNeighbors")
	}
}

// TestBridge_FrameHookSeesBothDirections: the hook observes every frame the
// bridge moves, tagged with its direction.
func TestBridge_FrameHookSeesBothDirections(t *testing.T) {
	a, err := NewStack("10.0.0.1", 24)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := NewStack("10.0.0.2", 24)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	aEnd, bEnd := net.Pipe()
	var outbound, inbound atomic.Int64
	hook := func(out bool, frame []byte) {
		if len(frame) == 0 {
			t.Error("hook saw an empty frame")
		}
		if out {
			outbound.Add(1)
		} else {
			inbound.Add(1)
		}
	}
	go func() { _ = BridgeFrameConn(ctx, a.Link(), anchortun.NewFrameConn(aEnd), hook) }()
	go func() { _ = Bridge(ctx, b.Link(), bEnd) }()

	ln, err := gonet.ListenTCP(b.Stack(), tcpip.FullAddress{Addr: b.Addr(), Port: 9}, ipv4.ProtocolNumber)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		_, _ = io.Copy(c, c)
		c.Close()
	}()
	dctx, dcancel := context.WithTimeout(ctx, 5*time.Second)
	defer dcancel()
	c, err := gonet.DialContextTCP(dctx, a.Stack(), tcpip.FullAddress{Addr: b.Addr(), Port: 9}, ipv4.ProtocolNumber)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Write([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 4)
	if _, err := io.ReadFull(c, buf); err != nil || string(buf) != "ping" {
		t.Fatalf("echo = %q, %v", buf, err)
	}
	c.Close()
	if outbound.Load() == 0 || inbound.Load() == 0 {
		t.Errorf("hook counts: outbound %d inbound %d, want both > 0", outbound.Load(), inbound.Load())
	}
}
