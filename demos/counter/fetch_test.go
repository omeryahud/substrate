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
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestServeDelay(t *testing.T) {
	start := time.Now()
	code, body := call(t, serveDelay, url.Values{"d": {"60ms"}})
	if code != http.StatusOK || body != "delayed 60ms" {
		t.Fatalf("delay = %d %q", code, body)
	}
	if elapsed := time.Since(start); elapsed < 60*time.Millisecond {
		t.Fatalf("answered after %v, want at least 60ms", elapsed)
	}
	for _, d := range []string{"", "soon", "-1s", "11m"} {
		if code, _ := call(t, serveDelay, url.Values{"d": {d}}); code != http.StatusBadRequest {
			t.Errorf("delay d=%q = %d, want 400", d, code)
		}
	}
}

// TestFetcherStartAndResult: a started GET blocks in the background, result
// says pending until it returns, then reports status, body and elapsed time.
func TestFetcherStartAndResult(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(serveDelay))
	defer server.Close()
	f := &fetcher{}
	target := server.URL + "/delay?d=200ms"

	if code, body := call(t, f.result, nil); code != http.StatusNotFound {
		t.Fatalf("result before any fetch = %d %q, want 404", code, body)
	}
	if code, body := call(t, f.start, url.Values{"url": {target}}); code != http.StatusOK || !strings.HasSuffix(strings.TrimSpace(body), ", request sent") {
		t.Fatalf("start = %d %q, want it to return once the request is sent", code, body)
	}
	if code, body := call(t, f.result, nil); code != http.StatusOK || !strings.HasPrefix(body, "pending for ") {
		t.Fatalf("result right after start = %d %q, want pending", code, body)
	}
	if code, body := call(t, f.start, url.Values{"url": {target}}); code != http.StatusConflict {
		t.Fatalf("second start while pending = %d %q, want 409", code, body)
	}
	code, body := call(t, f.result, url.Values{"wait": {"5s"}})
	if code != http.StatusOK || !strings.HasPrefix(body, "200 delayed 200ms after ") {
		t.Fatalf("result after waiting = %d %q", code, body)
	}
	if code, body := call(t, f.start, url.Values{"url": {target}}); code != http.StatusOK {
		t.Fatalf("start after the previous fetch finished = %d %q", code, body)
	}
}

// TestFetcherStartReportsAFailedCall: a GET that cannot even connect ends at
// once, and start says so instead of claiming the request was sent.
func TestFetcherStartReportsAFailedCall(t *testing.T) {
	f := &fetcher{}
	code, body := call(t, f.start, url.Values{"url": {"http://127.0.0.1:1/"}})
	if code != http.StatusOK || !strings.HasPrefix(body, "GET http://127.0.0.1:1/ ended at once: error ") {
		t.Fatalf("start against a closed port = %d %q", code, body)
	}
}

func TestFetcherFetchBlocksUntilTheResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(serveDelay))
	defer server.Close()
	f := &fetcher{}
	start := time.Now()
	code, body := call(t, f.fetch, url.Values{"url": {server.URL + "/delay?d=80ms"}})
	if code != http.StatusOK || !strings.HasPrefix(body, "200 delayed 80ms after ") {
		t.Fatalf("fetch = %d %q", code, body)
	}
	if elapsed := time.Since(start); elapsed < 80*time.Millisecond {
		t.Fatalf("fetch returned after %v, want at least the 80ms delay", elapsed)
	}
	if code, _ := call(t, f.fetch, nil); code != http.StatusBadRequest {
		t.Fatalf("fetch without url = %d, want 400", code)
	}
}
