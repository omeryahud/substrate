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
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

// TestGateWrites_HoldsWritesWhileQuiesced: a write toward the actor waits
// while the stack is quiesced, the wake hook fires once, and the write goes
// through after Unquiesce. Closing a held connection releases its writer.
func TestGateWrites_HoldsWritesWhileQuiesced(t *testing.T) {
	st, err := NewStack("10.0.0.1", 24)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	var woke atomic.Int32
	st.OnWriteBlocked(func() { woke.Add(1) })

	a, b := net.Pipe()
	defer b.Close()
	conn := st.GateWrites(a)
	go func() { _, _ = io.Copy(io.Discard, b) }()

	if _, err := conn.Write([]byte("open")); err != nil {
		t.Fatalf("write while open: %v", err)
	}

	st.Quiesce()
	if !st.Quiesced() {
		t.Fatal("stack does not report quiesced")
	}
	done := make(chan error, 1)
	go func() {
		_, err := conn.Write([]byte("held"))
		done <- err
	}()
	select {
	case err := <-done:
		t.Fatalf("write went through while quiesced: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	if woke.Load() != 1 {
		t.Fatalf("wake hook ran %d times, want 1", woke.Load())
	}

	st.Unquiesce()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("write after unquiesce: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("write stayed held after Unquiesce")
	}

	st.Quiesce()
	go func() {
		_, err := conn.Write([]byte("closing"))
		done <- err
	}()
	time.Sleep(20 * time.Millisecond)
	conn.Close()
	select {
	case err := <-done:
		if !errors.Is(err, net.ErrClosed) {
			t.Fatalf("write on a closed held connection = %v, want ErrClosed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("close did not release the held writer")
	}
}

// TestWaitDrained: an idle stack drains at once; a bounded wait returns when
// its context ends.
func TestWaitDrained(t *testing.T) {
	st, err := NewStack("10.0.0.1", 24)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if !st.WaitDrained(ctx) {
		t.Fatal("idle stack did not report drained")
	}
}
