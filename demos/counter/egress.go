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
	"strings"
	"sync"
	"time"
)

const egressIOTimeout = 15 * time.Second

// egressConnection is one outbound TCP connection to an echo service, kept
// open across requests so its survival across a suspend can be checked.
type egressConnection struct {
	mu     sync.Mutex
	conn   net.Conn
	reader *bufio.Reader
	since  time.Time
	remote string
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
	e.conn, e.reader, e.since, e.remote = conn, bufio.NewReader(conn), time.Now(), addr
	fmt.Fprintf(w, "open %s -> %s\n", conn.LocalAddr(), conn.RemoteAddr())
}

// send writes one line and returns the line the echo service sends back.
func (e *egressConnection) send(w http.ResponseWriter, r *http.Request) {
	msg := strings.ReplaceAll(r.URL.Query().Get("msg"), "\n", " ")
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.conn == nil {
		http.Error(w, "not open", http.StatusConflict)
		return
	}
	_ = e.conn.SetDeadline(time.Now().Add(egressIOTimeout))
	if _, err := fmt.Fprintf(e.conn, "%s\n", msg); err != nil {
		e.dropLocked()
		http.Error(w, "write: "+err.Error(), http.StatusBadGateway)
		return
	}
	line, err := e.reader.ReadString('\n')
	if err != nil {
		e.dropLocked()
		http.Error(w, "read: "+err.Error(), http.StatusBadGateway)
		return
	}
	fmt.Fprint(w, line)
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

func (e *egressConnection) dropLocked() {
	if e.conn != nil {
		e.conn.Close()
	}
	e.conn, e.reader, e.remote = nil, nil, ""
}
