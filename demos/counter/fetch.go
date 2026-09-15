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
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

const maxDelay = 10 * time.Minute

// serveDelay answers after d=<duration>. The echo target serves it, so an
// Actor can make a plain HTTP request whose response takes a while.
func serveDelay(w http.ResponseWriter, r *http.Request) {
	d, err := time.ParseDuration(r.URL.Query().Get("d"))
	if err != nil || d < 0 || d > maxDelay {
		http.Error(w, "d must be a duration up to "+maxDelay.String(), http.StatusBadRequest)
		return
	}
	select {
	case <-time.After(d):
		fmt.Fprintf(w, "delayed %s", d)
	case <-r.Context().Done():
	}
}

// fetchRun is one plain blocking HTTP GET made from inside the Actor.
type fetchRun struct {
	url     string
	started time.Time
	done    chan struct{}
	status  int
	body    string
	elapsed time.Duration
	err     error
}

func (run *fetchRun) String() string {
	if run.err != nil {
		return fmt.Sprintf("error %v after %s", run.err, run.elapsed.Round(time.Millisecond))
	}
	return fmt.Sprintf("%d %s after %s", run.status, run.body, run.elapsed.Round(time.Millisecond))
}

// fetcher runs GETs with an ordinary HTTP client. Nothing in it knows about
// suspend or resume: the goroutine simply blocks in client.Get until the
// response is there.
type fetcher struct {
	mu     sync.Mutex
	latest *fetchRun
	client http.Client
}

func (f *fetcher) run(url string) *fetchRun {
	run := &fetchRun{url: url, started: time.Now(), done: make(chan struct{})}
	go func() {
		defer close(run.done)
		resp, err := f.client.Get(url)
		run.elapsed = time.Since(run.started)
		if err != nil {
			run.err = err
			return
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
		run.status, run.body = resp.StatusCode, string(body)
	}()
	return run
}

// fetch runs the GET and answers when it returns.
func (f *fetcher) fetch(w http.ResponseWriter, r *http.Request) {
	url := r.URL.Query().Get("url")
	if url == "" {
		http.Error(w, "url is required", http.StatusBadRequest)
		return
	}
	run := f.run(url)
	select {
	case <-run.done:
		fmt.Fprintln(w, run)
	case <-r.Context().Done():
	}
}

// start runs the GET in the background and answers at once.
func (f *fetcher) start(w http.ResponseWriter, r *http.Request) {
	url := r.URL.Query().Get("url")
	if url == "" {
		http.Error(w, "url is required", http.StatusBadRequest)
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.latest != nil {
		select {
		case <-f.latest.done:
		default:
			http.Error(w, "a fetch is still pending", http.StatusConflict)
			return
		}
	}
	f.latest = f.run(url)
	fmt.Fprintf(w, "started GET %s\n", url)
}

// result answers with the outcome of the latest GET, waiting up to
// wait=<duration> for it, or says how long it has been pending.
func (f *fetcher) result(w http.ResponseWriter, r *http.Request) {
	wait, _ := time.ParseDuration(r.URL.Query().Get("wait"))
	wait = min(wait, maxDelay)
	f.mu.Lock()
	run := f.latest
	f.mu.Unlock()
	if run == nil {
		http.Error(w, "no fetch started", http.StatusNotFound)
		return
	}
	select {
	case <-run.done:
	case <-time.After(wait):
	case <-r.Context().Done():
		return
	}
	select {
	case <-run.done:
		fmt.Fprintln(w, run)
	default:
		fmt.Fprintf(w, "pending for %s\n", time.Since(run.started).Round(time.Second))
	}
}
