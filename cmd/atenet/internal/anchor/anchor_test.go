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

package anchor

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"google.golang.org/grpc"
	"gvisor.dev/gvisor/pkg/tcpip"
	"gvisor.dev/gvisor/pkg/tcpip/adapters/gonet"
	"gvisor.dev/gvisor/pkg/tcpip/header"
	"gvisor.dev/gvisor/pkg/tcpip/network/ipv4"

	"github.com/agent-substrate/substrate/internal/anchornet"
	"github.com/agent-substrate/substrate/internal/anchortun"
	"github.com/agent-substrate/substrate/internal/ateomnet/actoraddr"
	"github.com/agent-substrate/substrate/internal/atunnel"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

func newTestAnchor() *Anchor {
	a := &Anchor{actors: map[resources.ActorRef]*actorEntry{}}
	a.cfg.Hold.UnquiesceAfter = 20 * time.Millisecond
	return a
}

func TestActorRefFromRequest(t *testing.T) {
	ref := resources.ActorRef{Atespace: "team-a", Name: "chatbot-1"}
	dns := resources.ActorDNSName(ref)
	for _, tc := range []struct {
		name   string
		host   string
		header string
		want   resources.ActorRef
		ok     bool
	}{
		{"hostOnly", dns, "", ref, true},
		{"hostWithPort", dns + ":80", "", ref, true},
		{"originalHostHeaderWins", "ignored.example", dns, ref, true},
		{"upperCase", "CHATBOT-1.TEAM-A.actors.resources.substrate.ate.dev", "", ref, true},
		{"notActor", "example.com", "", resources.ActorRef{}, false},
		{"badHostPort", "a:b:c", "", resources.ActorRef{}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, "/", nil)
			r.Host = tc.host
			if tc.header != "" {
				r.Header.Set(atunnel.OriginalHostHeader, tc.header)
			}
			got, ok := actorRefFromRequest(r)
			if ok != tc.ok || got != tc.want {
				t.Errorf("actorRefFromRequest = (%v, %v), want (%v, %v)", got, ok, tc.want, tc.ok)
			}
		})
	}
}

// TestGetOrCreate covers the hold contract: a restore of the same actor reuses
// the held stack; a fresh boot or a new actor UID replaces it; unrelated
// actors do not share a stack.
func TestGetOrCreate(t *testing.T) {
	ctx := context.Background()
	a := newTestAnchor()
	ref := resources.ActorRef{Atespace: "team-a", Name: "chatbot-1"}

	first, err := a.getOrCreate(ctx, ref, "uid-1", false)
	if err != nil {
		t.Fatalf("first getOrCreate: %v", err)
	}
	again, err := a.getOrCreate(ctx, ref, "uid-1", false)
	if err != nil {
		t.Fatalf("restore getOrCreate: %v", err)
	}
	if again != first {
		t.Error("restore attach did not reuse the held stack")
	}

	renamed, err := a.getOrCreate(ctx, ref, "uid-2", false)
	if err != nil {
		t.Fatalf("new-UID getOrCreate: %v", err)
	}
	if renamed == first {
		t.Error("a new actor UID reused the old actor's held stack")
	}

	fresh, err := a.getOrCreate(ctx, ref, "uid-2", true)
	if err != nil {
		t.Fatalf("fresh getOrCreate: %v", err)
	}
	defer fresh.stack.Close()
	if fresh == renamed {
		t.Error("fresh boot reused the held stack instead of replacing it")
	}

	other, err := a.getOrCreate(ctx, resources.ActorRef{Atespace: "team-a", Name: "other"}, "uid-3", false)
	if err != nil {
		t.Fatalf("other getOrCreate: %v", err)
	}
	defer other.stack.Close()
	if other == fresh {
		t.Error("two actors share one stack")
	}
	if a.lookup(ref) != fresh {
		t.Error("lookup does not return the current stack")
	}
}

// TestServeHTTP_UnknownActorIs421: ingress for an actor no worker attached is
// rejected as a stale assignment, the signal the router re-resolves on.
func TestServeHTTP_UnknownActorIs421(t *testing.T) {
	a := newTestAnchor()
	r := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	r.Host = resources.ActorDNSName(resources.ActorRef{Atespace: "team-a", Name: "nobody"})
	w := httptest.NewRecorder()
	a.ServeHTTP(w, r)
	if w.Code != http.StatusMisdirectedRequest {
		t.Errorf("status = %d, want 421", w.Code)
	}
	if w.Header().Get(atunnel.StaleAssignmentHeader) == "" {
		t.Error("missing stale-assignment header")
	}
}

// fakeSandbox is an HTTP server inside a stack with the actor's address, wired
// to an anchor entry's link the way the tunnel wires a real sandbox.
type fakeSandbox struct {
	stack *anchornet.Stack
}

func startFakeSandbox(t *testing.T, ctx context.Context, entry *actorEntry, body string) *fakeSandbox {
	t.Helper()
	st, err := anchornet.NewEthernetStack(actoraddr.ActorVethIP, actoraddr.PrefixLen, "02:a8:1e:00:00:02")
	if err != nil {
		t.Fatalf("sandbox stack: %v", err)
	}
	ip := net.ParseIP(actoraddr.ActorVethIP).To4()
	ln, err := gonet.ListenTCP(st.Stack(), tcpip.FullAddress{Addr: tcpip.AddrFromSlice(ip), Port: 80}, ipv4.ProtocolNumber)
	if err != nil {
		t.Fatalf("sandbox listen: %v", err)
	}
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/host" {
			fmt.Fprint(w, r.Host)
			return
		}
		fmt.Fprint(w, body)
	})}
	go func() { _ = srv.Serve(ln) }()
	go anchornet.Relay(ctx, entry.stack.Link(), st.Link())
	t.Cleanup(func() {
		_ = srv.Close()
		st.Close()
	})
	return &fakeSandbox{stack: st}
}

// TestServeHTTP_ProxiesIntoTheActorsOwnStack: two actors have the same address
// inside their own stacks, and a request for each must reach its own sandbox,
// also on the second request when the first left an idle pooled connection.
func TestServeHTTP_ProxiesIntoTheActorsOwnStack(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	a := newTestAnchor()
	refA := resources.ActorRef{Atespace: "team-a", Name: "alpha"}
	refB := resources.ActorRef{Atespace: "team-a", Name: "beta"}
	entryA, err := a.getOrCreate(ctx, refA, "uid-a", false)
	if err != nil {
		t.Fatal(err)
	}
	entryB, err := a.getOrCreate(ctx, refB, "uid-b", false)
	if err != nil {
		t.Fatal(err)
	}
	defer entryA.stack.Close()
	defer entryB.stack.Close()
	startFakeSandbox(t, ctx, entryA, "alpha")
	startFakeSandbox(t, ctx, entryB, "beta")

	get := func(ref resources.ActorRef, path string) (int, string) {
		r := httptest.NewRequest(http.MethodGet, path, nil)
		r.Host = resources.ActorDNSName(ref)
		w := httptest.NewRecorder()
		a.ServeHTTP(w, r)
		return w.Code, w.Body.String()
	}
	for i := 0; i < 3; i++ {
		if code, body := get(refA, "/"); code != http.StatusOK || body != "alpha" {
			t.Fatalf("request %d for alpha: %d %q", i, code, body)
		}
		if code, body := get(refB, "/"); code != http.StatusOK || body != "beta" {
			t.Fatalf("request %d for beta: %d %q", i, code, body)
		}
	}
	if _, host := get(refA, "/host"); host != resources.ActorDNSName(refA) {
		t.Errorf("actor saw Host %q, want its own DNS name", host)
	}
}

// TestServeHTTP_UpstreamFailureIs502: an actor whose sandbox is not there
// (nothing relays its frames) yields a bad gateway, not a hang, once the
// request's own deadline passes.
func TestServeHTTP_UpstreamFailureIs502(t *testing.T) {
	a := newTestAnchor()
	ref := resources.ActorRef{Atespace: "team-a", Name: "lonely"}
	entry, err := a.getOrCreate(context.Background(), ref, "uid", false)
	if err != nil {
		t.Fatal(err)
	}
	defer entry.stack.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	r := httptest.NewRequest(http.MethodGet, "/", nil).WithContext(ctx)
	r.Host = resources.ActorDNSName(ref)
	w := httptest.NewRecorder()
	a.ServeHTTP(w, r)
	if w.Code != http.StatusBadGateway {
		t.Errorf("status = %d, want 502", w.Code)
	}
}

func TestServeProbe(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	a := newTestAnchor()
	ref := resources.ActorRef{Atespace: "team-a", Name: "alpha"}
	entry, err := a.getOrCreate(ctx, ref, "uid-a", false)
	if err != nil {
		t.Fatal(err)
	}
	defer entry.stack.Close()
	startFakeSandbox(t, ctx, entry, "ok")

	probe := func(query string) int {
		r := httptest.NewRequest(http.MethodGet, "/probe?"+query, nil)
		w := httptest.NewRecorder()
		a.serveProbe(w, r)
		return w.Code
	}
	if code := probe("atespace=team-a&actor=alpha&port=80&path=/readyz"); code != http.StatusOK {
		t.Errorf("probe of attached actor = %d, want 200", code)
	}
	if code := probe("atespace=team-a&actor=nobody&port=80&path=/readyz"); code != http.StatusNotFound {
		t.Errorf("probe of unknown actor = %d, want 404", code)
	}
	if code := probe("atespace=team-a&actor=alpha&port=nope"); code != http.StatusBadRequest {
		t.Errorf("probe with bad port = %d, want 400", code)
	}
}

// attachTunnel plays a worker's shuttle over an in-memory pipe: it sends the
// attach header, then bridges the fake sandbox's link over the pipe. It
// returns a function that drops the tunnel the way a stopped shuttle does.
func attachTunnel(t *testing.T, a *Anchor, hdr anchortun.AttachHeader, sandbox *anchornet.Stack) (drop func()) {
	t.Helper()
	anchorEnd, workerEnd := net.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		a.handleAttach(ctx, anchorEnd)
	}()
	raw, err := anchortun.MarshalAttachHeader(hdr)
	if err != nil {
		t.Fatal(err)
	}
	if err := anchortun.WriteFrame(workerEnd, raw); err != nil {
		t.Fatal(err)
	}
	go func() { _ = anchornet.Bridge(ctx, sandbox.Link(), workerEnd) }()
	return func() {
		workerEnd.Close()
		<-done
		cancel()
	}
}

func newSandboxStack(t *testing.T, body string) *anchornet.Stack {
	t.Helper()
	st, err := anchornet.NewEthernetStack(actoraddr.ActorVethIP, actoraddr.PrefixLen, "02:a8:1e:00:00:02")
	if err != nil {
		t.Fatal(err)
	}
	ip := net.ParseIP(actoraddr.ActorVethIP).To4()
	ln, err := gonet.ListenTCP(st.Stack(), tcpip.FullAddress{Addr: tcpip.AddrFromSlice(ip), Port: 80}, ipv4.ProtocolNumber)
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, body) })}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close(); st.Close() })
	return st
}

// TestHandleAttach_HoldsAcrossReattach drives the anchor's attach path end to
// end: a worker attaches, ingress reaches the sandbox, the tunnel drops and
// the stack is held, a second worker reattaches with the same actor UID and
// ingress works again; a fresh boot then replaces the stack.
func TestHandleAttach_HoldsAcrossReattach(t *testing.T) {
	a := newTestAnchor()
	ref := resources.ActorRef{Atespace: "team-a", Name: "alpha"}
	hdr := anchortun.AttachHeader{
		Atespace: ref.Atespace, ActorName: ref.Name, ActorUID: "uid-a",
		WorkerPodUID: "3fa9c1e2-0000-4444-8888-abcdefabcdef", ActivationID: "act-1", Boot: anchortun.BootRestore,
	}
	sandbox := newSandboxStack(t, "hello")
	get := func() (int, string) {
		r := httptest.NewRequest(http.MethodGet, "/", nil)
		r.Host = resources.ActorDNSName(ref)
		w := httptest.NewRecorder()
		a.ServeHTTP(w, r)
		return w.Code, w.Body.String()
	}
	waitAttached := func() *actorEntry {
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			if e := a.lookup(ref); e != nil {
				e.mu.Lock()
				attached := e.detach != nil
				e.mu.Unlock()
				if attached {
					return e
				}
			}
			time.Sleep(5 * time.Millisecond)
		}
		t.Fatal("actor never attached")
		return nil
	}

	drop := attachTunnel(t, a, hdr, sandbox)
	first := waitAttached()
	if code, body := get(); code != http.StatusOK || body != "hello" {
		t.Fatalf("ingress while attached: %d %q", code, body)
	}
	if first.framesToActor.Load() == 0 || first.framesFromActor.Load() == 0 {
		t.Error("frame counters did not move")
	}

	drop()
	first.mu.Lock()
	held := first.detach == nil && !first.heldSince.IsZero()
	first.mu.Unlock()
	if !held {
		t.Fatal("stack was not marked held after the tunnel dropped")
	}
	if a.lookup(ref) != first {
		t.Fatal("held stack was discarded")
	}

	hdr.ActivationID = "act-2"
	hdr.WorkerPodUID = "3fa9c1e2-0000-4444-8888-000000000002"
	drop2 := attachTunnel(t, a, hdr, sandbox)
	defer drop2()
	if waitAttached() != first {
		t.Fatal("reattach did not reuse the held stack")
	}
	if code, body := get(); code != http.StatusOK || body != "hello" {
		t.Fatalf("ingress after reattach: %d %q", code, body)
	}

	hdr.ActivationID = "act-3"
	hdr.Boot = anchortun.BootFresh
	drop3 := attachTunnel(t, a, hdr, sandbox)
	defer drop3()
	deadline := time.Now().Add(5 * time.Second)
	for a.lookup(ref) == first && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if a.lookup(ref) == first {
		t.Fatal("fresh boot did not replace the held stack")
	}
}

// TestHandleAttach_RejectsBadHeader: a tunnel that does not start with a valid
// header is closed without creating a stack.
func TestHandleAttach_RejectsBadHeader(t *testing.T) {
	a := newTestAnchor()
	anchorEnd, workerEnd := net.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		a.handleAttach(context.Background(), anchorEnd)
	}()
	if err := anchortun.WriteFrame(workerEnd, []byte(`{"atespace":"team-a"}`)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("handleAttach did not return on a bad header")
	}
	a.mu.Lock()
	n := len(a.actors)
	a.mu.Unlock()
	if n != 0 {
		t.Errorf("bad header created %d stacks", n)
	}
}

func TestVerifyPeers(t *testing.T) {
	a := newTestAnchor()
	a.cfg.RouterClientID = "spiffe://cluster.local/ns/ate-system/sa/atenet-router"
	uri := func(s string) *url.URL {
		u, err := url.Parse(s)
		if err != nil {
			t.Fatal(err)
		}
		return u
	}
	router := []*url.URL{uri(a.cfg.RouterClientID)}
	worker := []*url.URL{uri("spiffe://cluster.local/ns/ate-demo-counter/sa/default")}
	stranger := []*url.URL{uri("https://example.com/not-spiffe")}

	if err := a.verifyRouter(router); err != nil {
		t.Errorf("verifyRouter(router) = %v", err)
	}
	if err := a.verifyRouter(worker); err == nil {
		t.Error("verifyRouter accepted a worker identity")
	}
	if err := verifyWorkload(worker); err != nil {
		t.Errorf("verifyWorkload(worker) = %v", err)
	}
	if err := verifyWorkload(stranger); err == nil {
		t.Error("verifyWorkload accepted a non-SPIFFE identity")
	}
	if err := verifyWorkload(nil); err == nil {
		t.Error("verifyWorkload accepted a certificate without URIs")
	}
}

// TestServeQuiesceAndRelease: quiesce holds writes toward the actor and
// reports a drain; release drops the stack only for the named incarnation.
func TestServeQuiesceAndRelease(t *testing.T) {
	ctx := context.Background()
	a := newTestAnchor()
	ref := resources.ActorRef{Atespace: "team-a", Name: "alpha"}
	entry, err := a.getOrCreate(ctx, ref, "uid-a", false)
	if err != nil {
		t.Fatal(err)
	}
	entry.mu.Lock()
	entry.activationID = "act-1"
	entry.mu.Unlock()

	post := func(path, query string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodPost, path+"?"+query, nil)
		w := httptest.NewRecorder()
		if path == "/quiesce" {
			a.serveQuiesce(w, r)
		} else {
			a.serveRelease(w, r)
		}
		return w
	}
	if w := post("/quiesce", "atespace=team-a&actor=alpha&activationID=act-9"); w.Code != http.StatusConflict {
		t.Errorf("quiesce with a stale activation = %d, want 409", w.Code)
	}
	w := post("/quiesce", "atespace=team-a&actor=alpha&activationID=act-1&timeout=100ms")
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"drained":true`) {
		t.Fatalf("quiesce = %d %s, want 200 drained", w.Code, w.Body.String())
	}
	if !entry.stack.Quiesced() {
		t.Error("stack is not quiesced after /quiesce")
	}
	if w := post("/quiesce", "atespace=team-a&actor=nobody"); w.Code != http.StatusNotFound {
		t.Errorf("quiesce of an unknown actor = %d, want 404", w.Code)
	}

	if w := post("/release", "atespace=team-a&actor=alpha&actorUID=uid-other"); w.Code != http.StatusOK {
		t.Errorf("release of another incarnation = %d, want 200", w.Code)
	}
	if a.lookup(ref) != entry {
		t.Fatal("release of another incarnation dropped the stack")
	}
	if w := post("/release", "atespace=team-a&actor=alpha&actorUID=uid-a&reason=delete"); w.Code != http.StatusOK {
		t.Errorf("release = %d, want 200", w.Code)
	}
	if a.lookup(ref) != nil {
		t.Fatal("release did not drop the stack")
	}
	if w := post("/release", "atespace=team-a&actor=alpha"); w.Code != http.StatusOK {
		t.Errorf("release of an unknown actor = %d, want 200", w.Code)
	}
	if w := post("/release", "atespace=Not+Valid&actor=alpha"); w.Code != http.StatusBadRequest {
		t.Errorf("release with a bad reference = %d, want 400", w.Code)
	}
}

type fakeAssignments struct {
	actor *ateapipb.Actor
	err   error
}

func (f *fakeAssignments) GetActor(context.Context, *ateapipb.GetActorRequest, ...grpc.CallOption) (*ateapipb.Actor, error) {
	return f.actor, f.err
}

func (f *fakeAssignments) ResumeActor(context.Context, *ateapipb.ResumeActorRequest, ...grpc.CallOption) (*ateapipb.ResumeActorResponse, error) {
	return &ateapipb.ResumeActorResponse{}, nil
}

// TestVerifyAttach: an attach is accepted only from the worker the control
// plane placed this actor incarnation on, for the current activation.
func TestVerifyAttach(t *testing.T) {
	hdr := anchortun.AttachHeader{
		Atespace: "team-a", ActorName: "alpha", ActorUID: "uid-a",
		WorkerPodUID: "3fa9c1e2-0000-4444-8888-abcdefabcdef", ActivationID: "act-1", Boot: anchortun.BootRestore,
	}
	placed := func() *ateapipb.Actor {
		return &ateapipb.Actor{
			Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "alpha", Uid: "uid-a"},
			Status: &ateapipb.ActorStatus{WorkerAssignment: &ateapipb.WorkerAssignment{
				WorkerPodUid: hdr.WorkerPodUID, ActivationId: "act-1",
			}},
		}
	}
	for name, tc := range map[string]struct {
		peer    string
		actor   func() *ateapipb.Actor
		err     error
		noCheck bool
		want    error
	}{
		"placed worker, current activation": {peer: hdr.WorkerPodUID, actor: placed},
		"no control plane check configured": {peer: hdr.WorkerPodUID, noCheck: true},
		"peer certificate is another pod":   {peer: "3fa9c1e2-0000-4444-8888-000000000009", actor: placed, want: errIdentityMismatch},
		"actor not found":                   {peer: hdr.WorkerPodUID, err: errors.New("not found"), want: errAssignmentMismatch},
		"other incarnation": {peer: hdr.WorkerPodUID, actor: func() *ateapipb.Actor {
			a := placed()
			a.Metadata.Uid = "uid-b"
			return a
		}, want: errAssignmentMismatch},
		"placed on another worker": {peer: hdr.WorkerPodUID, actor: func() *ateapipb.Actor {
			a := placed()
			a.Status.WorkerAssignment.WorkerPodUid = "3fa9c1e2-0000-4444-8888-000000000009"
			return a
		}, want: errAssignmentMismatch},
		"stale activation": {peer: hdr.WorkerPodUID, actor: func() *ateapipb.Actor {
			a := placed()
			a.Status.WorkerAssignment.ActivationId = "act-2"
			return a
		}, want: errAssignmentMismatch},
		"suspended, no assignment": {peer: hdr.WorkerPodUID, actor: func() *ateapipb.Actor {
			a := placed()
			a.Status.WorkerAssignment = nil
			return a
		}, want: errAssignmentMismatch},
	} {
		t.Run(name, func(t *testing.T) {
			a := newTestAnchor()
			if !tc.noCheck {
				fake := &fakeAssignments{err: tc.err}
				if tc.actor != nil {
					fake.actor = tc.actor()
				}
				a.controlPlane = fake
			}
			err := a.verifyAttach(context.Background(), hdr, tc.peer)
			if tc.want == nil && err != nil {
				t.Fatalf("verifyAttach = %v, want accepted", err)
			}
			if tc.want != nil && !errors.Is(err, tc.want) {
				t.Fatalf("verifyAttach = %v, want %v", err, tc.want)
			}
		})
	}
}

type countingControlPlane struct {
	fakeAssignments
	resumes atomic.Int32
}

func (c *countingControlPlane) ResumeActor(context.Context, *ateapipb.ResumeActorRequest, ...grpc.CallOption) (*ateapipb.ResumeActorResponse, error) {
	c.resumes.Add(1)
	return &ateapipb.ResumeActorResponse{}, nil
}

// TestWake: a held actor whose template asked for it is resumed when data
// arrives, at most once per wake interval; an attached actor or one that did
// not ask is left alone.
func TestWake(t *testing.T) {
	now := time.Date(2026, 9, 13, 8, 0, 0, 0, time.UTC)
	a := newTestAnchor()
	a.now = func() time.Time { return now }
	a.cfg.Hold.WakeInterval = time.Second
	control := &countingControlPlane{}
	a.controlPlane = control
	entry, err := a.getOrCreate(context.Background(), resources.ActorRef{Atespace: "team-a", Name: "alpha"}, "uid-a", false)
	if err != nil {
		t.Fatal(err)
	}
	defer entry.stack.Close()
	waitResumes := func(want int32) {
		t.Helper()
		deadline := time.Now().Add(2 * time.Second)
		for control.resumes.Load() != want && time.Now().Before(deadline) {
			time.Sleep(5 * time.Millisecond)
		}
		if got := control.resumes.Load(); got != want {
			t.Fatalf("resume attempts = %d, want %d", got, want)
		}
	}

	a.wake(entry)
	waitResumes(0)

	entry.wakeOnData = true
	entry.detach = func() {}
	a.wake(entry)
	waitResumes(0)

	entry.detach = nil
	a.wake(entry)
	waitResumes(1)
	a.wake(entry)
	waitResumes(1)

	now = now.Add(2 * time.Second)
	a.wake(entry)
	waitResumes(2)

	a.controlPlane = nil
	now = now.Add(2 * time.Second)
	a.wake(entry)
	waitResumes(2)
}

// TestHeldWriteWakesTheActor: after a detach, the first write toward the
// actor is held and triggers a wake.
func TestHeldWriteWakesTheActor(t *testing.T) {
	a := newTestAnchor()
	a.cfg.Hold.WakeInterval = time.Second
	control := &countingControlPlane{}
	a.controlPlane = control
	entry, err := a.getOrCreate(context.Background(), resources.ActorRef{Atespace: "team-a", Name: "alpha"}, "uid-a", false)
	if err != nil {
		t.Fatal(err)
	}
	defer entry.stack.Close()
	entry.wakeOnData = true
	a.markHeld(entry)

	pipeA, pipeB := net.Pipe()
	defer pipeB.Close()
	conn := entry.stack.GateWrites(pipeA)
	defer conn.Close()
	done := make(chan struct{})
	go func() {
		_, _ = conn.Write([]byte("data for a suspended actor"))
		close(done)
	}()
	deadline := time.Now().Add(2 * time.Second)
	for control.resumes.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if control.resumes.Load() != 1 {
		t.Fatalf("resume attempts = %d, want 1", control.resumes.Load())
	}
	select {
	case <-done:
		t.Fatal("write went through while the actor is held")
	default:
	}
	go func() { _, _ = io.Copy(io.Discard, pipeB) }()
	entry.stack.Unquiesce()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("write stayed held after the actor reattached")
	}
}

// TestCaps: stacks beyond the actor cap are refused and counted.
func TestCaps(t *testing.T) {
	ctx := context.Background()
	a := newTestAnchor()
	a.cfg.Hold.MaxActors = 1
	first, err := a.getOrCreate(ctx, resources.ActorRef{Atespace: "team-a", Name: "alpha"}, "uid-a", false)
	if err != nil {
		t.Fatal(err)
	}
	defer first.stack.Close()
	if _, err := a.getOrCreate(ctx, resources.ActorRef{Atespace: "team-a", Name: "beta"}, "uid-b", false); !errors.Is(err, errTooManyActors) {
		t.Fatalf("second actor over the cap: err = %v, want %v", err, errTooManyActors)
	}
	if again, err := a.getOrCreate(ctx, resources.ActorRef{Atespace: "team-a", Name: "alpha"}, "uid-a", false); err != nil || again != first {
		t.Fatalf("reattach of a held actor under the cap failed: %v", err)
	}
	a.cfg.Hold.MaxConnections = 0
	if !a.connectionAllowed(ctx) {
		t.Error("no connection cap refused a connection")
	}
}

func TestServeStatusz(t *testing.T) {
	a := newTestAnchor()
	a.cfg.HoldTTL = time.Hour
	entry, err := a.getOrCreate(context.Background(), resources.ActorRef{Atespace: "team-a", Name: "alpha"}, "uid-a", false)
	if err != nil {
		t.Fatal(err)
	}
	defer entry.stack.Close()
	entry.heldSince = time.Now()
	w := httptest.NewRecorder()
	a.serveStatusz(w, httptest.NewRequest(http.MethodGet, "/statusz", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("statusz = %d", w.Code)
	}
	body := w.Body.String()
	for _, want := range []string{`"name": "alpha"`, `"held": 1`, `"attached": 0`, `"holdTTL": "1h0m0s"`} {
		if !strings.Contains(body, want) {
			t.Errorf("statusz body lacks %s:\n%s", want, body)
		}
	}
}

// TestMarkHeldWakesAWriterHeldDuringTheCheckpoint: ateapi quiesces the stack
// while the actor is still attached, so a write that arrives then cannot
// wake it yet. The detach that follows must.
func TestMarkHeldWakesAWriterHeldDuringTheCheckpoint(t *testing.T) {
	a := newTestAnchor()
	a.cfg.Hold.WakeInterval = time.Second
	control := &countingControlPlane{}
	a.controlPlane = control
	entry, err := a.getOrCreate(context.Background(), resources.ActorRef{Atespace: "team-a", Name: "alpha"}, "uid-a", false)
	if err != nil {
		t.Fatal(err)
	}
	defer entry.stack.Close()
	entry.wakeOnData = true
	entry.detach = func() {}
	entry.stack.Quiesce()

	pipeA, pipeB := net.Pipe()
	defer pipeB.Close()
	go func() { _, _ = io.Copy(io.Discard, pipeB) }()
	conn := entry.stack.GateWrites(pipeA)
	defer conn.Close()
	done := make(chan struct{})
	go func() {
		_, _ = conn.Write([]byte("reply that lands during the checkpoint"))
		close(done)
	}()
	deadline := time.Now().Add(time.Second)
	for entry.stack.HeldWriters() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if entry.stack.HeldWriters() != 1 {
		t.Fatal("the write was not held")
	}
	if control.resumes.Load() != 0 {
		t.Fatalf("resume attempts while still attached = %d, want 0", control.resumes.Load())
	}

	entry.mu.Lock()
	entry.detach = nil
	entry.mu.Unlock()
	a.markHeld(entry)
	deadline = time.Now().Add(2 * time.Second)
	for control.resumes.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if control.resumes.Load() != 1 {
		t.Fatalf("resume attempts after the detach = %d, want 1", control.resumes.Load())
	}
	select {
	case <-done:
		t.Fatal("write went through while the actor is held")
	default:
	}
	entry.stack.Unquiesce()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("write stayed held after the actor reattached")
	}
}

type flakyControlPlane struct {
	fakeAssignments
	attempts  atomic.Int32
	failFirst int32
}

func (c *flakyControlPlane) ResumeActor(context.Context, *ateapipb.ResumeActorRequest, ...grpc.CallOption) (*ateapipb.ResumeActorResponse, error) {
	if c.attempts.Add(1) <= c.failFirst {
		return nil, errors.New("control plane unavailable")
	}
	return &ateapipb.ResumeActorResponse{}, nil
}

// TestWakeRetriesAfterAFailure: while data stays held, a failed resume is
// tried again after the wake interval.
func TestWakeRetriesAfterAFailure(t *testing.T) {
	a := newTestAnchor()
	a.cfg.Hold.WakeInterval = 30 * time.Millisecond
	control := &flakyControlPlane{failFirst: 1}
	a.controlPlane = control
	entry, err := a.getOrCreate(context.Background(), resources.ActorRef{Atespace: "team-a", Name: "alpha"}, "uid-a", false)
	if err != nil {
		t.Fatal(err)
	}
	defer entry.stack.Close()
	entry.wakeOnData = true
	a.markHeld(entry)

	pipeA, pipeB := net.Pipe()
	defer pipeB.Close()
	go func() { _, _ = io.Copy(io.Discard, pipeB) }()
	conn := entry.stack.GateWrites(pipeA)
	defer conn.Close()
	done := make(chan struct{})
	go func() {
		_, _ = conn.Write([]byte("data for a suspended actor"))
		close(done)
	}()
	deadline := time.Now().Add(3 * time.Second)
	for control.attempts.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := control.attempts.Load(); got < 2 {
		t.Fatalf("resume attempts = %d, want a retry after the failure", got)
	}
	entry.stack.Unquiesce()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("write stayed held after Unquiesce")
	}
}

// TestSweepHeld: a stack held past the TTL is dropped; attached stacks and
// recent holds stay.
func TestSweepHeld(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 12, 12, 0, 0, 0, time.UTC)
	a := newTestAnchor()
	a.cfg.HoldTTL = time.Hour
	a.now = func() time.Time { return now }

	mk := func(name string) *actorEntry {
		e, err := a.getOrCreate(ctx, resources.ActorRef{Atespace: "team-a", Name: name}, "uid-"+name, false)
		if err != nil {
			t.Fatal(err)
		}
		return e
	}
	expired := mk("expired")
	expired.heldSince = now.Add(-2 * time.Hour)
	recent := mk("recent")
	recent.heldSince = now.Add(-time.Minute)
	attached := mk("attached")
	attached.heldSince = now.Add(-2 * time.Hour)
	attached.detach = func() {}
	defer recent.stack.Close()
	defer attached.stack.Close()

	if got := a.sweepHeld(ctx); got != 1 {
		t.Errorf("sweepHeld dropped %d stacks, want 1", got)
	}
	if a.lookup(expired.ref) != nil {
		t.Error("expired hold was not dropped")
	}
	if a.lookup(recent.ref) == nil || a.lookup(attached.ref) == nil {
		t.Error("a live stack was dropped")
	}
	if got := a.sweepHeld(ctx); got != 0 {
		t.Errorf("second sweep dropped %d stacks, want 0", got)
	}
}

func TestSummarizeFrame(t *testing.T) {
	src := tcpip.LinkAddress("\x02\xa8\x1e\x00\x00\x02")
	dst := tcpip.LinkAddress("\x02\xa8\x1e\x00\x00\x01")

	arpFrame := make([]byte, header.EthernetMinimumSize+header.ARPSize)
	header.Ethernet(arpFrame).Encode(&header.EthernetFields{SrcAddr: src, DstAddr: dst, Type: header.ARPProtocolNumber})
	arp := header.ARP(arpFrame[header.EthernetMinimumSize:])
	arp.SetIPv4OverEthernet()
	arp.SetOp(header.ARPRequest)
	copy(arp.HardwareAddressSender(), src)
	copy(arp.ProtocolAddressSender(), []byte{169, 254, 17, 2})
	copy(arp.ProtocolAddressTarget(), []byte{169, 254, 17, 1})
	got := summarizeFrame(arpFrame)
	for _, want := range []string{"arp request", "who-has 169.254.17.1", "tell 169.254.17.2"} {
		if !strings.Contains(got, want) {
			t.Errorf("ARP summary %q lacks %q", got, want)
		}
	}

	tcpFrame := make([]byte, header.EthernetMinimumSize+header.IPv4MinimumSize+header.TCPMinimumSize+3)
	header.Ethernet(tcpFrame).Encode(&header.EthernetFields{SrcAddr: dst, DstAddr: src, Type: header.IPv4ProtocolNumber})
	ip := header.IPv4(tcpFrame[header.EthernetMinimumSize:])
	ip.Encode(&header.IPv4Fields{
		TotalLength: uint16(len(tcpFrame) - header.EthernetMinimumSize),
		TTL:         64,
		Protocol:    uint8(header.TCPProtocolNumber),
		SrcAddr:     tcpip.AddrFrom4([4]byte{169, 254, 17, 1}),
		DstAddr:     tcpip.AddrFrom4([4]byte{169, 254, 17, 2}),
	})
	ip.SetChecksum(^ip.CalculateChecksum())
	tcp := header.TCP(ip.Payload())
	tcp.Encode(&header.TCPFields{SrcPort: 40000, DstPort: 80, SeqNum: 7, AckNum: 0, DataOffset: header.TCPMinimumSize, Flags: header.TCPFlagSyn, WindowSize: 1024})
	got = summarizeFrame(tcpFrame)
	for _, want := range []string{"169.254.17.1 > 169.254.17.2", "tcp 40000 > 80", "S", "seq 7", "len 3"} {
		if !strings.Contains(got, want) {
			t.Errorf("TCP summary %q lacks %q", got, want)
		}
	}

	if got := summarizeFrame([]byte{1, 2, 3}); !strings.Contains(got, "short frame") {
		t.Errorf("short frame summary = %q", got)
	}
}

// TestMetrics_RecordsInstruments: the instruments are created on the global
// provider and each record lands under its metric name.
func TestMetrics_RecordsInstruments(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	prev := otel.GetMeterProvider()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	t.Cleanup(func() { otel.SetMeterProvider(prev) })

	m, err := NewMetrics()
	if err != nil {
		t.Fatalf("NewMetrics: %v", err)
	}
	ctx := context.Background()
	m.recordAttach(ctx, outcomeAttached, "restore")
	m.addTunnels(ctx, 1)
	m.addActors(ctx, 1)
	m.recordHoldExpired(ctx)
	m.recordFrame(ctx, directionToActor, 60)
	m.recordIngress(ctx, 5*time.Millisecond, outcomeProxied)
	m.recordProbe(ctx, time.Millisecond, outcomeOK)

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	seen := map[string]bool{}
	for _, sm := range rm.ScopeMetrics {
		for _, mtr := range sm.Metrics {
			seen[mtr.Name] = true
		}
	}
	for _, name := range []string{attachMetricName, tunnelsActiveMetricName, actorsMetricName, holdsExpiredMetricName,
		framesMetricName, frameBytesMetricName, ingressRequestsMetricName, ingressDurationMetricName, probesMetricName, probeDurationMetricName} {
		if !seen[name] {
			t.Errorf("metric %s was not recorded", name)
		}
	}

	var nilMetrics *Metrics
	nilMetrics.recordAttach(ctx, outcomeAttached, "fresh")
	nilMetrics.recordIngress(ctx, 0, outcomeProxied)
}
