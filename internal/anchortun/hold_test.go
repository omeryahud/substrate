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

package anchortun

import (
	"errors"
	"testing"
	"time"
)

// fakeClock is a controllable clock for the hold TTL.
type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time      { return c.t }
func (c *fakeClock) add(d time.Duration) { c.t = c.t.Add(d) }

func newClock() *fakeClock {
	return &fakeClock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

// TestConn_SuspendResumeCycle walks the happy path: attach, quiesce on
// suspend, detach to held, data wakes it, attach on resume.
func TestConn_SuspendResumeCycle(t *testing.T) {
	clk := newClock()
	c := NewConn(clk.now, true)

	if c.State() != StateAttached {
		t.Fatalf("new conn = %s, want attached", c.State())
	}
	if err := c.Quiesce(); err != nil {
		t.Fatalf("Quiesce: %v", err)
	}
	if err := c.Detach(); err != nil {
		t.Fatalf("Detach: %v", err)
	}
	if c.State() != StateHeld {
		t.Fatalf("after detach = %s, want held", c.State())
	}
	wake, err := c.DataArrived()
	if err != nil {
		t.Fatalf("DataArrived: %v", err)
	}
	if !wake {
		t.Error("DataArrived on held conn with wakeOnData did not wake")
	}
	if c.State() != StateWaking {
		t.Fatalf("after data = %s, want waking", c.State())
	}
	if err := c.Attach(); err != nil {
		t.Fatalf("Attach: %v", err)
	}
	if c.State() != StateAttached {
		t.Fatalf("after attach = %s, want attached", c.State())
	}
	if c.HeldFor() != 0 {
		t.Errorf("HeldFor after reattach = %s, want 0", c.HeldFor())
	}
}

// TestConn_WorkerDeathHolds covers a worker dying while attached: detach
// straight from attached to held, no quiesce.
func TestConn_WorkerDeathHolds(t *testing.T) {
	c := NewConn(newClock().now, true)
	if err := c.Detach(); err != nil {
		t.Fatalf("Detach from attached: %v", err)
	}
	if c.State() != StateHeld {
		t.Errorf("after detach = %s, want held", c.State())
	}
}

// TestConn_WakeOnDataDisabled: data on a held conn without wake keeps it held.
func TestConn_WakeOnDataDisabled(t *testing.T) {
	c := NewConn(newClock().now, false)
	_ = c.Detach()
	wake, err := c.DataArrived()
	if err != nil {
		t.Fatalf("DataArrived: %v", err)
	}
	if wake {
		t.Error("DataArrived woke a conn with wakeOnData disabled")
	}
	if c.State() != StateHeld {
		t.Errorf("state = %s, want held", c.State())
	}
}

// TestConn_DataArrivedTwiceWakesOnce: a second data event while waking does
// not trigger another wake.
func TestConn_DataArrivedTwiceWakesOnce(t *testing.T) {
	c := NewConn(newClock().now, true)
	_ = c.Detach()
	if wake, _ := c.DataArrived(); !wake {
		t.Fatal("first DataArrived did not wake")
	}
	wake, err := c.DataArrived()
	if err != nil {
		t.Fatalf("second DataArrived: %v", err)
	}
	if wake {
		t.Error("second DataArrived woke again")
	}
}

// TestConn_NormalClose: close is allowed while attached.
func TestConn_NormalClose(t *testing.T) {
	c := NewConn(newClock().now, true)
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !c.Terminal() || c.State() != StateClosed {
		t.Errorf("state = %s, terminal %v; want closed/terminal", c.State(), c.Terminal())
	}
}

// TestConn_ResetFromAnyState: release resets a connection from any live state.
func TestConn_ResetFromAnyState(t *testing.T) {
	setup := map[string]func() *Conn{
		"attached":  func() *Conn { return NewConn(newClock().now, true) },
		"quiescing": func() *Conn { c := NewConn(newClock().now, true); _ = c.Quiesce(); return c },
		"held":      func() *Conn { c := NewConn(newClock().now, true); _ = c.Detach(); return c },
		"waking":    func() *Conn { c := NewConn(newClock().now, true); _ = c.Detach(); c.DataArrived(); return c },
	}
	for name, mk := range setup {
		t.Run(name, func(t *testing.T) {
			c := mk()
			if err := c.Reset(); err != nil {
				t.Fatalf("Reset from %s: %v", name, err)
			}
			if c.State() != StateReset {
				t.Errorf("state = %s, want reset", c.State())
			}
		})
	}
}

// TestConn_InvalidTransitions checks that events not allowed in a state fail
// with ErrInvalidTransition rather than silently corrupting state.
func TestConn_InvalidTransitions(t *testing.T) {
	var invalid ErrInvalidTransition
	for _, tc := range []struct {
		name string
		call func(*Conn) error
		make func() *Conn
	}{
		{"quiesceWhileHeld", (*Conn).Quiesce, func() *Conn { c := NewConn(newClock().now, true); _ = c.Detach(); return c }},
		{"detachWhileHeld", (*Conn).Detach, func() *Conn { c := NewConn(newClock().now, true); _ = c.Detach(); return c }},
		{"attachWhileAttached", (*Conn).Attach, func() *Conn { return NewConn(newClock().now, true) }},
		{"closeWhileHeld", (*Conn).Close, func() *Conn { c := NewConn(newClock().now, true); _ = c.Detach(); return c }},
		{"resetAfterClose", (*Conn).Reset, func() *Conn { c := NewConn(newClock().now, true); _ = c.Close(); return c }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.call(tc.make())
			if !errors.As(err, &invalid) {
				t.Errorf("got %v, want ErrInvalidTransition", err)
			}
		})
	}
}

func TestConn_DataArrivedWhileAttachedIsInvalid(t *testing.T) {
	c := NewConn(newClock().now, true)
	if _, err := c.DataArrived(); err == nil {
		t.Error("DataArrived while attached succeeded, want error")
	}
}

// TestConn_TTL: a held connection expires only after ttl elapses; zero ttl
// never expires; an attached connection never expires.
func TestConn_TTL(t *testing.T) {
	clk := newClock()
	c := NewConn(clk.now, true)
	_ = c.Detach()

	if c.ExpiredTTL(0) {
		t.Error("zero TTL should never expire")
	}
	if c.ExpiredTTL(time.Hour) {
		t.Error("expired before any time passed")
	}
	clk.add(30 * time.Minute)
	if c.ExpiredTTL(time.Hour) {
		t.Error("expired at 30m under a 1h TTL")
	}
	if c.HeldFor() != 30*time.Minute {
		t.Errorf("HeldFor = %s, want 30m", c.HeldFor())
	}
	clk.add(30 * time.Minute)
	if !c.ExpiredTTL(time.Hour) {
		t.Error("did not expire at 1h under a 1h TTL")
	}

	attached := NewConn(clk.now, true)
	if attached.ExpiredTTL(time.Nanosecond) {
		t.Error("attached connection reported TTL expiry")
	}
}

// TestConn_TTLExpiresWhileWaking: a waking connection is TTL-eligible too, so a
// resume that never lands still lets the hold be reclaimed.
func TestConn_TTLExpiresWhileWaking(t *testing.T) {
	clk := newClock()
	c := NewConn(clk.now, true)
	_ = c.Detach()
	if wake, _ := c.DataArrived(); !wake {
		t.Fatal("expected wake to reach waking state")
	}
	if c.State() != StateWaking {
		t.Fatalf("state = %s, want waking", c.State())
	}
	clk.add(2 * time.Hour)
	if !c.ExpiredTTL(time.Hour) {
		t.Error("waking connection did not expire under a 1h TTL after 2h")
	}
}

// TestConn_Unquiesce: a suspend that quiesced but did not detach can abort
// back to attached; unquiesce is invalid from any other state.
func TestConn_Unquiesce(t *testing.T) {
	c := NewConn(newClock().now, true)
	_ = c.Quiesce()
	if err := c.Unquiesce(); err != nil {
		t.Fatalf("Unquiesce from quiescing: %v", err)
	}
	if c.State() != StateAttached {
		t.Errorf("state = %s, want attached", c.State())
	}
	var invalid ErrInvalidTransition
	if err := c.Unquiesce(); !errors.As(err, &invalid) {
		t.Errorf("Unquiesce from attached = %v, want ErrInvalidTransition", err)
	}
}

// TestConn_ProactiveAttachFromHeld: a resume triggered by something other than
// data attaches a held connection directly, without a wake.
func TestConn_ProactiveAttachFromHeld(t *testing.T) {
	c := NewConn(newClock().now, true)
	_ = c.Detach()
	if err := c.Attach(); err != nil {
		t.Fatalf("Attach from held: %v", err)
	}
	if c.State() != StateAttached {
		t.Errorf("state = %s, want attached", c.State())
	}
}

// TestConn_CloseFromQuiescing: a close during a quiesce ends the connection.
func TestConn_CloseFromQuiescing(t *testing.T) {
	c := NewConn(newClock().now, true)
	_ = c.Quiesce()
	if err := c.Close(); err != nil {
		t.Fatalf("Close from quiescing: %v", err)
	}
	if c.State() != StateClosed {
		t.Errorf("state = %s, want closed", c.State())
	}
}

// TestConn_ResetClearsHeldFor: after a reset, HeldFor honors its contract of
// zero, so a caller never reads a growing hold time from a dead connection.
func TestConn_ResetClearsHeldFor(t *testing.T) {
	clk := newClock()
	c := NewConn(clk.now, true)
	_ = c.Detach()
	clk.add(time.Hour)
	if err := c.Reset(); err != nil {
		t.Fatalf("Reset: %v", err)
	}
	clk.add(time.Hour)
	if c.HeldFor() != 0 {
		t.Errorf("HeldFor after reset = %s, want 0", c.HeldFor())
	}
}

func TestState_String(t *testing.T) {
	for s, want := range map[State]string{
		StateAttached:  "attached",
		StateQuiescing: "quiescing",
		StateHeld:      "held",
		StateWaking:    "waking",
		StateClosed:    "closed",
		StateReset:     "reset",
		State(99):      "state(99)",
	} {
		if got := s.String(); got != want {
			t.Errorf("State(%d).String() = %q, want %q", int(s), got, want)
		}
	}
}

func TestErrInvalidTransition_Error(t *testing.T) {
	err := ErrInvalidTransition{From: StateHeld, Event: "quiesce"}
	if got, want := err.Error(), "anchortun: cannot quiesce from state held"; got != want {
		t.Errorf("Error() = %q, want %q", got, want)
	}
}

func TestNewConn_NilClockDefaults(t *testing.T) {
	c := NewConn(nil, true)
	if c.HeldFor() != 0 {
		t.Errorf("HeldFor while attached = %s, want 0", c.HeldFor())
	}
	_ = c.Detach()
	held := c.HeldFor()
	if held < 0 || held > time.Minute {
		t.Errorf("HeldFor just after detach = %s, want a small non-negative duration", held)
	}
}
