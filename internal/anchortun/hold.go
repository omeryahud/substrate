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
	"fmt"
	"time"
)

// State is the lifecycle of one actor-facing connection in the anchor.
type State int

const (
	// StateAttached: the actor is running and data flows on the connection.
	StateAttached State = iota
	// StateQuiescing: a suspend or pause has started; the anchor has stopped
	// feeding new data toward the actor and is draining what is in flight.
	StateQuiescing
	// StateHeld: the actor is detached (suspended or its worker gone). The
	// far end stays connected while data waits under back-pressure.
	StateHeld
	// StateWaking: data arrived for a held connection and a resume was asked
	// for; the connection waits for a worker to attach.
	StateWaking
	// StateClosed: one side closed the connection normally.
	StateClosed
	// StateReset: the connection was reset, on release, hold TTL, or a cap.
	StateReset
)

func (s State) String() string {
	switch s {
	case StateAttached:
		return "attached"
	case StateQuiescing:
		return "quiescing"
	case StateHeld:
		return "held"
	case StateWaking:
		return "waking"
	case StateClosed:
		return "closed"
	case StateReset:
		return "reset"
	default:
		return fmt.Sprintf("state(%d)", int(s))
	}
}

// ErrInvalidTransition reports an event that is not allowed in the current
// state.
type ErrInvalidTransition struct {
	From  State
	Event string
}

func (e ErrInvalidTransition) Error() string {
	return fmt.Sprintf("anchortun: cannot %s from state %s", e.Event, e.From)
}

// Conn is the state machine for one actor-facing connection. It is the
// reference logic the anchor embeds next to the real socket and netstack
// endpoint; it holds no I/O and is safe to test in isolation. It is not
// safe for concurrent use; the anchor serializes events per connection.
type Conn struct {
	state      State
	wakeOnData bool
	now        func() time.Time
	heldSince  time.Time
}

// NewConn returns a connection in the attached state. now supplies the clock,
// so tests can drive the hold TTL without waiting; a nil now defaults to
// time.Now. wakeOnData sets whether data arriving while held should trigger a
// wake.
func NewConn(now func() time.Time, wakeOnData bool) *Conn {
	if now == nil {
		now = time.Now
	}
	return &Conn{state: StateAttached, wakeOnData: wakeOnData, now: now}
}

// State returns the current state.
func (c *Conn) State() State { return c.state }

// Terminal reports whether the connection has ended and accepts no more events.
func (c *Conn) Terminal() bool {
	return c.state == StateClosed || c.state == StateReset
}

func (c *Conn) require(event string, allowed ...State) error {
	for _, s := range allowed {
		if c.state == s {
			return nil
		}
	}
	return ErrInvalidTransition{From: c.state, Event: event}
}

// Quiesce begins draining before a checkpoint. Valid only while attached.
func (c *Conn) Quiesce() error {
	if err := c.require("quiesce", StateAttached); err != nil {
		return err
	}
	c.state = StateQuiescing
	return nil
}

// Unquiesce aborts a suspend that quiesced but did not detach, returning the
// connection to attached so it can serve again. It matches a checkpoint that
// failed before the worker was freed.
func (c *Conn) Unquiesce() error {
	if err := c.require("unquiesce", StateQuiescing); err != nil {
		return err
	}
	c.state = StateAttached
	return nil
}

// Detach moves the connection to held when the tunnel goes away: after a
// clean quiesce, or when a worker dies while attached. It records when the
// hold began so the TTL can be measured.
func (c *Conn) Detach() error {
	if err := c.require("detach", StateAttached, StateQuiescing); err != nil {
		return err
	}
	c.state = StateHeld
	c.heldSince = c.now()
	return nil
}

// DataArrived reports payload bytes from the far end of a held connection. It
// returns wake=true when this should trigger a resume, which happens on the
// first data for a held connection whose policy allows waking. A connection
// already waking does not wake again. When wakeOnData is off the data waits
// under back-pressure and the connection stays held.
func (c *Conn) DataArrived() (wake bool, err error) {
	switch c.state {
	case StateHeld:
		if c.wakeOnData {
			c.state = StateWaking
			return true, nil
		}
		return false, nil
	case StateWaking:
		return false, nil
	default:
		return false, ErrInvalidTransition{From: c.state, Event: "receive data"}
	}
}

// Attach reconnects the actor after a resume, from either a held or a waking
// connection. Data flows again.
func (c *Conn) Attach() error {
	if err := c.require("attach", StateHeld, StateWaking); err != nil {
		return err
	}
	c.state = StateAttached
	c.heldSince = time.Time{}
	return nil
}

// Close ends the connection normally, when either side closes while the actor
// is attached or quiescing. A far-end close that arrives while the connection
// is held is deliberately not applied here: the anchor keeps the held endpoint
// and delivers the end of stream to the actor when it reattaches, so a held
// connection never becomes Closed or Reset merely because the peer went away.
func (c *Conn) Close() error {
	if err := c.require("close", StateAttached, StateQuiescing); err != nil {
		return err
	}
	c.state = StateClosed
	return nil
}

// Reset ends the connection by force: on release (delete, crash, fresh boot),
// on hold TTL, or when a cap is exceeded. It is allowed from any non-terminal
// state so release can clear a connection whatever it was doing.
func (c *Conn) Reset() error {
	if c.Terminal() {
		return ErrInvalidTransition{From: c.state, Event: "reset"}
	}
	c.state = StateReset
	c.heldSince = time.Time{}
	return nil
}

// HeldFor returns how long the connection has been held. It is zero unless the
// connection is held or waking.
func (c *Conn) HeldFor() time.Duration {
	if c.heldSince.IsZero() {
		return 0
	}
	return c.now().Sub(c.heldSince)
}

// ExpiredTTL reports whether a held or waking connection has been held at least
// ttl. A ttl of zero or less means connections are never reset for age.
func (c *Conn) ExpiredTTL(ttl time.Duration) bool {
	if ttl <= 0 {
		return false
	}
	if c.state != StateHeld && c.state != StateWaking {
		return false
	}
	return c.HeldFor() >= ttl
}
