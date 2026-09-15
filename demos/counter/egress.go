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

package main

import (
	"bufio"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

const egressIOTimeout = 15 * time.Second

// egressConnection is one outbound TCP connection to an echo service, kept
// open across requests so its survival across a suspend can be checked.
// Replies are read in the background, so a request can be answered long
// after it was sent, including after the Actor was suspended and resumed.
type egressConnection struct {
	mu       sync.Mutex
	conn     net.Conn
	since    time.Time
	remote   string
	received []string
	changed  chan struct{}
}

func (e *egressConnection) open(w http.ResponseWriter, r *http.Request) {
	addr := r.URL.Query().Get("addr")
	if _, _, err := net.SplitHostPort(addr); err != nil {
		http.Error(w, "addr must be host:port", http.StatusBadRequest)
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.conn != nil {
		http.Error(w, "already open to "+e.remote, http.StatusConflict)
		return
	}
	conn, err := net.DialTimeout("tcp", addr, egressIOTimeout)
	if err != nil {
		http.Error(w, fmt.Sprintf("dial %s: %v", addr, err), http.StatusBadGateway)
		return
	}
	e.conn, e.since, e.remote = conn, time.Now(), addr
	e.received, e.changed = nil, make(chan struct{})
	go e.readReplies(conn)
	fmt.Fprintf(w, "open %s -> %s\n", conn.LocalAddr(), conn.RemoteAddr())
}

// send writes one line and answers with the next reply.
func (e *egressConnection) send(w http.ResponseWriter, r *http.Request) {
	before, ok := e.write(w, r)
	if !ok {
		return
	}
	lines, open := e.waitReplies(before, egressIOTimeout)
	if len(lines) <= before {
		if !open {
			http.Error(w, "connection closed before the reply", http.StatusBadGateway)
			return
		}
		http.Error(w, "no reply within "+egressIOTimeout.String(), http.StatusGatewayTimeout)
		return
	}
	fmt.Fprint(w, lines[before])
}

// request writes one line and returns at once; the reply is collected in the
// background and served by replies.
func (e *egressConnection) request(w http.ResponseWriter, r *http.Request) {
	if before, ok := e.write(w, r); ok {
		fmt.Fprintf(w, "sent, %d replies so far\n", before)
	}
}

// replies lists the reply lines received so far. With wait=<duration> it
// blocks until more than after=<n> replies are in, or the wait runs out.
func (e *egressConnection) replies(w http.ResponseWriter, r *http.Request) {
	after, _ := strconv.Atoi(r.URL.Query().Get("after"))
	wait, _ := time.ParseDuration(r.URL.Query().Get("wait"))
	lines, _ := e.waitReplies(after, wait)
	for _, line := range lines {
		fmt.Fprint(w, line)
	}
}

func (e *egressConnection) status(w http.ResponseWriter, _ *http.Request) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.conn == nil {
		fmt.Fprintln(w, "closed")
		return
	}
	fmt.Fprintf(w, "open to %s since %s\n", e.remote, e.since.UTC().Format(time.RFC3339))
}

func (e *egressConnection) close(w http.ResponseWriter, _ *http.Request) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.dropLocked()
	fmt.Fprintln(w, "closed")
}

// write sends the msg query parameter as one line and returns how many
// replies had arrived before it, so the caller can wait for the next one.
func (e *egressConnection) write(w http.ResponseWriter, r *http.Request) (int, bool) {
	msg := strings.ReplaceAll(r.URL.Query().Get("msg"), "\n", " ")
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.conn == nil {
		http.Error(w, "not open", http.StatusConflict)
		return 0, false
	}
	_ = e.conn.SetWriteDeadline(time.Now().Add(egressIOTimeout))
	if _, err := fmt.Fprintf(e.conn, "%s\n", msg); err != nil {
		e.dropLocked()
		http.Error(w, "write: "+err.Error(), http.StatusBadGateway)
		return 0, false
	}
	return len(e.received), true
}

func (e *egressConnection) readReplies(conn net.Conn) {
	reader := bufio.NewReader(conn)
	for {
		line, err := reader.ReadString('\n')
		e.mu.Lock()
		if e.conn != conn {
			e.mu.Unlock()
			return
		}
		if len(line) > 0 {
			e.received = append(e.received, line)
			e.notifyLocked()
		}
		if err != nil {
			e.dropLocked()
			e.mu.Unlock()
			return
		}
		e.mu.Unlock()
	}
}

// waitReplies returns the replies once more than after of them are in, the
// connection is gone, or timeout passes. The bool reports the connection
// still being open.
func (e *egressConnection) waitReplies(after int, timeout time.Duration) ([]string, bool) {
	deadline := time.Now().Add(timeout)
	for {
		e.mu.Lock()
		lines, open, changed := append([]string(nil), e.received...), e.conn != nil, e.changed
		e.mu.Unlock()
		remaining := time.Until(deadline)
		if len(lines) > after || !open || remaining <= 0 {
			return lines, open
		}
		select {
		case <-changed:
		case <-time.After(remaining):
		}
	}
}

func (e *egressConnection) notifyLocked() {
	if e.changed != nil {
		close(e.changed)
	}
	e.changed = make(chan struct{})
}

func (e *egressConnection) dropLocked() {
	if e.conn != nil {
		e.conn.Close()
	}
	e.conn, e.remote = nil, ""
	e.notifyLocked()
}
