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
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestParseDelay(t *testing.T) {
	for _, tc := range []struct {
		line    string
		delay   time.Duration
		payload string
	}{
		{"hello\n", 0, "hello\n"},
		{"delay=200ms late\n", 200 * time.Millisecond, "late\n"},
		{"delay=2s a b\n", 2 * time.Second, "a b\n"},
		{"delay=soon x\n", 0, "delay=soon x\n"},
		{"delay=1s\n", 0, "delay=1s\n"},
		{"delay=-1s x\n", 0, "delay=-1s x\n"},
	} {
		delay, payload := parseDelay(tc.line)
		if delay != tc.delay || payload != tc.payload {
			t.Errorf("parseDelay(%q) = %v %q, want %v %q", tc.line, delay, payload, tc.delay, tc.payload)
		}
	}
}

// startEcho serves echoLines on a loopback listener and returns its address.
func startEcho(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { listener.Close() })
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go echoLines(conn)
		}
	}()
	return listener.Addr().String()
}

func TestEchoLinesDelaysPrefixedLines(t *testing.T) {
	conn, err := net.Dial("tcp", startEcho(t))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	reader := bufio.NewReader(conn)

	start := time.Now()
	if _, err := conn.Write([]byte("delay=150ms late\n")); err != nil {
		t.Fatal(err)
	}
	line, err := reader.ReadString('\n')
	if err != nil || line != "late\n" {
		t.Fatalf("delayed echo = %q, %v", line, err)
	}
	if elapsed := time.Since(start); elapsed < 150*time.Millisecond {
		t.Fatalf("reply came after %v, want at least the 150ms delay", elapsed)
	}

	if _, err := conn.Write([]byte("now\n")); err != nil {
		t.Fatal(err)
	}
	if line, err := reader.ReadString('\n'); err != nil || line != "now\n" {
		t.Fatalf("plain echo = %q, %v", line, err)
	}
}

func call(t *testing.T, handler http.HandlerFunc, query url.Values) (int, string) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/?"+query.Encode(), nil)
	rec := httptest.NewRecorder()
	handler(rec, req)
	return rec.Code, rec.Body.String()
}

// TestEgressRequestAndReplies: a request returns before its reply exists,
// replies waits for the reply, and send still answers with the next line.
func TestEgressRequestAndReplies(t *testing.T) {
	e := &egressConnection{}
	if code, body := call(t, e.open, url.Values{"addr": {startEcho(t)}}); code != http.StatusOK {
		t.Fatalf("open = %d %q", code, body)
	}
	defer call(t, e.close, nil)

	code, body := call(t, e.request, url.Values{"msg": {"delay=200ms late"}})
	if code != http.StatusOK || !strings.HasPrefix(body, "sent") {
		t.Fatalf("request = %d %q", code, body)
	}
	if code, body := call(t, e.replies, nil); code != http.StatusOK || body != "" {
		t.Fatalf("replies right after the request = %d %q, want none yet", code, body)
	}
	if code, body := call(t, e.replies, url.Values{"wait": {"5s"}}); code != http.StatusOK || body != "late\n" {
		t.Fatalf("replies after waiting = %d %q, want \"late\"", code, body)
	}

	if code, body := call(t, e.send, url.Values{"msg": {"now"}}); code != http.StatusOK || body != "now\n" {
		t.Fatalf("send = %d %q, want \"now\"", code, body)
	}
	if code, body := call(t, e.replies, url.Values{"after": {"1"}}); code != http.StatusOK || body != "late\nnow\n" {
		t.Fatalf("replies = %d %q, want both lines", code, body)
	}
	if code, body := call(t, e.status, nil); code != http.StatusOK || !strings.HasPrefix(body, "open to ") {
		t.Fatalf("status = %d %q", code, body)
	}
}

func TestEgressRequestNeedsAnOpenConnection(t *testing.T) {
	e := &egressConnection{}
	if code, _ := call(t, e.request, url.Values{"msg": {"x"}}); code != http.StatusConflict {
		t.Fatalf("request while closed = %d, want 409", code)
	}
	if code, body := call(t, e.replies, url.Values{"wait": {"50ms"}}); code != http.StatusOK || body != "" {
		t.Fatalf("replies while closed = %d %q, want an empty list", code, body)
	}
}
