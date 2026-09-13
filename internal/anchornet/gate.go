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
	"net"
	"sync"
	"time"
)

// writeGate holds writes toward the actor while the stack is quiesced. The
// far end's bytes then stay in the anchor's kernel socket buffers instead of
// becoming segments that retransmit toward a frozen sandbox.
type writeGate struct {
	mu       sync.Mutex
	open     chan struct{}
	quiesced bool
	// onBlocked runs once per quiesce when a write is first held, so the
	// anchor can wake a held actor that has data waiting.
	onBlocked func()
	notified  bool
}

func newWriteGate() *writeGate {
	g := &writeGate{open: make(chan struct{})}
	close(g.open)
	return g
}

func (g *writeGate) quiesce() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.quiesced {
		return
	}
	g.quiesced = true
	g.notified = false
	g.open = make(chan struct{})
}

func (g *writeGate) unquiesce() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.quiesced {
		return
	}
	g.quiesced = false
	close(g.open)
}

func (g *writeGate) isQuiesced() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.quiesced
}

// wait blocks until the gate is open or closed reports the connection ended.
func (g *writeGate) wait(closed <-chan struct{}) bool {
	g.mu.Lock()
	open := g.open
	blocked := g.quiesced
	notify := blocked && !g.notified && g.onBlocked != nil
	if notify {
		g.notified = true
	}
	g.mu.Unlock()
	if notify {
		g.onBlocked()
	}
	select {
	case <-open:
		return true
	case <-closed:
		return false
	}
}

// Quiesce holds every write toward the actor until Unquiesce.
func (s *Stack) Quiesce() { s.gate.quiesce() }

// Unquiesce lets held writes through again.
func (s *Stack) Unquiesce() { s.gate.unquiesce() }

// Quiesced reports whether writes toward the actor are held.
func (s *Stack) Quiesced() bool { return s.gate.isQuiesced() }

// OnWriteBlocked sets the function run when a write is first held after a
// Quiesce. The anchor uses it to wake an actor that has data waiting.
func (s *Stack) OnWriteBlocked(fn func()) {
	s.gate.mu.Lock()
	defer s.gate.mu.Unlock()
	s.gate.onBlocked = fn
}

// Drained reports whether no outbound frame is waiting for the tunnel, which
// after a Quiesce means the stack has nothing more to send toward the actor
// until the actor acknowledges or a retransmit timer fires.
func (s *Stack) Drained() bool { return s.link.NumQueued() == 0 }

// WaitDrained waits until the stack is drained and stays drained for a short
// settle time, or until ctx ends. It reports whether it drained.
func (s *Stack) WaitDrained(ctx context.Context) bool {
	const settle = 3
	quiet := 0
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		if s.Drained() {
			quiet++
			if quiet >= settle {
				return true
			}
		} else {
			quiet = 0
		}
		select {
		case <-ctx.Done():
			return s.Drained()
		case <-ticker.C:
		}
	}
}

// GateWrites returns conn with its writes toward the actor held while the
// stack is quiesced. Reads are unaffected.
func (s *Stack) GateWrites(conn net.Conn) net.Conn {
	return &gatedConn{Conn: conn, gate: s.gate, closed: make(chan struct{})}
}

type gatedConn struct {
	net.Conn
	gate   *writeGate
	closed chan struct{}
	once   sync.Once
}

func (c *gatedConn) Write(p []byte) (int, error) {
	if !c.gate.wait(c.closed) {
		return 0, net.ErrClosed
	}
	return c.Conn.Write(p)
}

func (c *gatedConn) Close() error {
	c.once.Do(func() { close(c.closed) })
	return c.Conn.Close()
}

// CloseWrite keeps the half-close the proxies rely on.
func (c *gatedConn) CloseWrite() error {
	if cw, ok := c.Conn.(interface{ CloseWrite() error }); ok {
		return cw.CloseWrite()
	}
	return nil
}
