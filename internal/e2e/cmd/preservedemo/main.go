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

// Command preservedemo shows connection preservation live. It creates an
// Actor from a connection-preserving template, opens one WebSocket to it
// through the router, and serves a small page with Send, Suspend, and Resume
// buttons, so the same connection can be watched across a suspend and a
// resume. It deletes the Actor when it exits.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/websocket"

	"github.com/agent-substrate/substrate/internal/ateclient"
	"github.com/agent-substrate/substrate/internal/e2e"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

func main() {
	listen := flag.String("listen", "127.0.0.1:8090", "address the demo page listens on")
	kubeContext := flag.String("kube-context", "kind-kind", "kubernetes context")
	atespace := flag.String("atespace", "preserve-demo", "atespace for the demo Actor")
	templateNS := flag.String("template-namespace", "ate-demo-counter", "ActorTemplate namespace")
	template := flag.String("template", "counter-preserve", "ActorTemplate with ate.dev/connection-policy=Preserve")
	slowDelay := flag.Duration("slow-reply-delay", 30*time.Second, "how long the echo target takes to answer the slow request")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	e2e.KubeContext = *kubeContext
	d, err := newDemo(ctx, *atespace, *templateNS, *template)
	if err != nil {
		log.Fatal(err)
	}
	d.slowDelay = *slowDelay
	defer d.cleanup()

	mux := http.NewServeMux()
	mux.HandleFunc("/", d.servePage)
	mux.HandleFunc("/events", d.serveEvents)
	mux.HandleFunc("/api/status", d.serveStatus)
	mux.HandleFunc("/api/connect", d.action(d.connect))
	mux.HandleFunc("/api/send", d.action(d.send))
	mux.HandleFunc("/api/suspend", d.action(d.suspend))
	mux.HandleFunc("/api/resume", d.action(d.resume))
	mux.HandleFunc("/api/egress/open", d.action(d.egressOpen))
	mux.HandleFunc("/api/egress/send", d.action(d.egressSend))
	mux.HandleFunc("/api/sequence", d.action(d.sequence))
	mux.HandleFunc("/api/slow", d.action(d.slowRequest))
	mux.HandleFunc("/api/quit", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK); stop() })
	srv := &http.Server{Addr: *listen, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		_ = srv.Close()
	}()
	fmt.Printf("demo page: http://%s/\n", *listen)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}

type event struct {
	At   string `json:"at"`
	Kind string `json:"kind"`
	Text string `json:"text"`
}

type pendingSend struct {
	n     int
	at    time.Time
	state string
}

type demo struct {
	ctx    context.Context
	ref    resources.ActorRef
	apiRef *ateapipb.ObjectRef

	clientsMu sync.Mutex
	clients   *e2e.Clients
	clientsAt time.Time
	routerMu  sync.Mutex
	router    *e2e.RouterClient
	routerAt  time.Time

	mu          sync.Mutex
	ws          *websocket.Conn
	wsSince     time.Time
	wsCount     int
	sent        int
	received    int
	pending     []pendingSend
	state       string
	worker      string
	busy        bool
	egressSent  int
	egressState string
	slowDelay   time.Duration
	slowCount   int
	log         []event
	subscribers map[chan event]struct{}
}

// echoTarget is the plain TCP echo service the counter demo deploys outside
// any sandbox; the Actor reaches it through the egress gateway.
const echoTarget = "echo-target.ate-demo-counter.svc.cluster.local:7777"

// clientRefreshAfter is how long the demo keeps one set of cluster clients.
// The ateapi bearer token they carry lives one hour.
const clientRefreshAfter = 50 * time.Minute

func newDemo(ctx context.Context, atespace, templateNS, template string) (*demo, error) {
	clients, err := e2e.NewClients(ctx)
	if err != nil {
		return nil, fmt.Errorf("connecting to the cluster: %w", err)
	}
	router, err := e2e.NewRouterClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("port-forwarding the router: %w", err)
	}
	name := fmt.Sprintf("demo-%d", time.Now().Unix()%100000)
	d := &demo{
		ctx:         ctx,
		clients:     clients,
		clientsAt:   time.Now(),
		router:      router,
		routerAt:    time.Now(),
		ref:         resources.ActorRef{Atespace: atespace, Name: name},
		apiRef:      &ateapipb.ObjectRef{Atespace: atespace, Name: name},
		state:       "creating",
		subscribers: map[chan event]struct{}{},
	}
	_, _ = clients.SubstrateAPI.CreateAtespace(ctx, &ateapipb.CreateAtespaceRequest{
		Atespace: &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: atespace}},
	})
	d.logf("info", "creating Actor %s/%s from template %s/%s", atespace, name, templateNS, template)
	if _, err := clients.SubstrateAPI.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:               &ateapipb.ResourceMetadata{Atespace: atespace, Name: name},
		ActorTemplateNamespace: templateNS,
		ActorTemplateName:      template,
	}}); err != nil {
		return nil, fmt.Errorf("CreateActor: %w", err)
	}
	go d.pollState()
	go d.keepRouterAlive()
	go func() {
		if err := d.resume(); err != nil {
			d.logf("error", "first resume: %v", err)
		}
	}()
	return d, nil
}

// api returns an ateapi client whose bearer token is still valid.
func (d *demo) api(ctx context.Context) (*ateclient.Client, error) {
	d.clientsMu.Lock()
	defer d.clientsMu.Unlock()
	if time.Since(d.clientsAt) < clientRefreshAfter {
		return d.clients.SubstrateAPI, nil
	}
	fresh, err := e2e.NewClients(ctx)
	if err != nil {
		return nil, fmt.Errorf("refreshing the cluster clients: %w", err)
	}
	d.clients.Close()
	d.clients, d.clientsAt = fresh, time.Now()
	d.logf("info", "refreshed the ateapi credentials")
	return fresh.SubstrateAPI, nil
}

func (d *demo) currentRouter() *e2e.RouterClient {
	d.routerMu.Lock()
	defer d.routerMu.Unlock()
	return d.router
}

// routerDead reports whether the router port-forward stopped answering.
// The kubelet drops streaming connections that idle for hours, and a dead
// port-forward answers every request with an empty reply.
func (d *demo) routerDead() bool {
	ctx, cancel := context.WithTimeout(d.ctx, 5*time.Second)
	defer cancel()
	return d.currentRouter().Ping(ctx) != nil
}

func (d *demo) reconnectRouter() error {
	d.routerMu.Lock()
	defer d.routerMu.Unlock()
	if time.Since(d.routerAt) < 10*time.Second {
		return nil
	}
	fresh, err := e2e.NewRouterClient(d.ctx)
	if err != nil {
		return fmt.Errorf("rebuilding the router port-forward: %w", err)
	}
	d.router.Close()
	d.router, d.routerAt = fresh, time.Now()
	d.logf("info", "rebuilt the router port-forward")
	return nil
}

// routerGet sends one GET to the Actor through the router. When the
// request failed because the port-forward is dead, it is retried once on a
// fresh one.
func (d *demo) routerGet(path string) (*http.Response, error) {
	resp, err := d.currentRouter().Get(d.ctx, d.ref, path)
	if err == nil || !d.routerDead() {
		return resp, err
	}
	if rerr := d.reconnectRouter(); rerr != nil {
		return nil, err
	}
	return d.currentRouter().Get(d.ctx, d.ref, path)
}

func (d *demo) dialWebSocket() (*websocket.Conn, error) {
	conn, _, err := d.currentRouter().DialWebSocket(d.ctx, d.ref, "/ws")
	if err == nil || !d.routerDead() {
		return conn, err
	}
	if rerr := d.reconnectRouter(); rerr != nil {
		return nil, err
	}
	conn, _, err = d.currentRouter().DialWebSocket(d.ctx, d.ref, "/ws")
	return conn, err
}

// keepRouterAlive pings the router once a minute so the port-forward never
// idles out, and rebuilds it when it died anyway.
func (d *demo) keepRouterAlive() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-d.ctx.Done():
			return
		case <-ticker.C:
		}
		if d.routerDead() {
			if err := d.reconnectRouter(); err != nil {
				d.logf("error", "%v", err)
			}
		}
	}
}

func (d *demo) cleanup() {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	d.mu.Lock()
	if d.ws != nil {
		d.ws.Close()
	}
	d.mu.Unlock()
	fmt.Println("deleting the demo Actor")
	if api, err := d.api(ctx); err != nil {
		fmt.Printf("cannot reach ateapi, Actor %s/%s is left behind: %v\n", d.ref.Atespace, d.ref.Name, err)
	} else {
		d.suspendAndDelete(ctx, api)
	}
	d.currentRouter().Close()
	d.clients.Close()
}

// suspendAndDelete waits for the suspend to settle first: DeleteActor refuses
// an Actor that is still RUNNING or SUSPENDING.
func (d *demo) suspendAndDelete(ctx context.Context, api *ateclient.Client) {
	_, _ = api.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: d.apiRef})
	for ctx.Err() == nil {
		resp, err := api.GetActor(ctx, &ateapipb.GetActorRequest{Actor: d.apiRef})
		if err != nil {
			break
		}
		state := resp.GetStatus().GetState()
		if state != ateapipb.ActorState_ACTOR_STATE_RUNNING && state != ateapipb.ActorState_ACTOR_STATE_SUSPENDING {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if _, err := api.DeleteActor(ctx, &ateapipb.DeleteActorRequest{Actor: d.apiRef}); err != nil {
		fmt.Printf("DeleteActor: %v\n", err)
	}
}

func (d *demo) logf(kind, format string, args ...any) {
	ev := event{At: time.Now().Format("15:04:05.000"), Kind: kind, Text: fmt.Sprintf(format, args...)}
	d.mu.Lock()
	d.log = append(d.log, ev)
	for ch := range d.subscribers {
		select {
		case ch <- ev:
		default:
		}
	}
	d.mu.Unlock()
	log.Printf("%-6s %s", kind, ev.Text)
}

// pollState watches the Actor and logs every state or worker change.
func (d *demo) pollState() {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-d.ctx.Done():
			return
		case <-ticker.C:
		}
		api, err := d.api(d.ctx)
		if err != nil {
			continue
		}
		resp, err := api.GetActor(d.ctx, &ateapipb.GetActorRequest{Actor: d.apiRef})
		if err != nil {
			continue
		}
		state := strings.TrimPrefix(resp.GetStatus().GetState().String(), "ACTOR_STATE_")
		worker := resp.GetStatus().GetWorkerAssignment().GetWorkerPod()
		d.mu.Lock()
		changed := state != d.state || worker != d.worker
		d.state, d.worker = state, worker
		d.mu.Unlock()
		if changed {
			if worker != "" {
				d.logf("state", "Actor is %s on worker %s", state, worker)
			} else {
				d.logf("state", "Actor is %s", state)
			}
		}
	}
}

func (d *demo) action(fn func() error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST only", http.StatusMethodNotAllowed)
			return
		}
		go func() {
			if err := fn(); err != nil {
				d.logf("error", "%v", err)
			}
		}()
		w.WriteHeader(http.StatusAccepted)
	}
}

func (d *demo) connect() error {
	d.mu.Lock()
	if d.ws != nil {
		d.mu.Unlock()
		return errors.New("already connected; the point is to keep this one connection")
	}
	d.mu.Unlock()
	if err := d.waitRouteReady(); err != nil {
		return err
	}
	conn, err := d.dialWebSocket()
	if err != nil {
		return fmt.Errorf("WebSocket dial through the router: %w", err)
	}
	d.mu.Lock()
	d.ws = conn
	d.wsCount++
	d.wsSince = time.Now()
	d.pending = nil
	n := d.wsCount
	d.mu.Unlock()
	d.logf("ws", "WebSocket #%d open through the router (local %s)", n, conn.LocalAddr())
	go d.readLoop(conn, n)
	return nil
}

func (d *demo) readLoop(conn *websocket.Conn, n int) {
	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			d.mu.Lock()
			if d.ws == conn {
				d.ws = nil
			}
			d.mu.Unlock()
			d.logf("error", "WebSocket #%d closed: %v", n, err)
			return
		}
		d.mu.Lock()
		d.received++
		var p pendingSend
		if len(d.pending) > 0 {
			p, d.pending = d.pending[0], d.pending[1:]
		}
		d.mu.Unlock()
		note := ""
		if p.state != "" && p.state != "RUNNING" {
			note = fmt.Sprintf(" (sent while the Actor was %s)", p.state)
		}
		d.logf("recv", "echo for message %d after %s: %q%s", p.n, time.Since(p.at).Round(time.Millisecond), msg, note)
	}
}

func (d *demo) send() error {
	d.mu.Lock()
	conn := d.ws
	if conn == nil {
		d.mu.Unlock()
		return errors.New("not connected")
	}
	d.sent++
	n := d.sent
	p := pendingSend{n: n, at: time.Now(), state: d.state}
	d.pending = append(d.pending, p)
	d.mu.Unlock()
	text := fmt.Sprintf("hello %d", n)
	if err := conn.WriteMessage(websocket.TextMessage, []byte(text)); err != nil {
		return fmt.Errorf("send: %w", err)
	}
	d.logf("sent", "message %d %q while the Actor is %s", n, text, p.state)
	return nil
}

func (d *demo) suspend() error {
	d.logf("info", "SuspendActor requested")
	api, err := d.api(d.ctx)
	if err != nil {
		return err
	}
	if _, err := api.SuspendActor(d.ctx, &ateapipb.SuspendActorRequest{Actor: d.apiRef}); err != nil {
		return fmt.Errorf("SuspendActor: %w", err)
	}
	return d.waitState("SUSPENDED")
}

func (d *demo) resume() error {
	d.logf("info", "ResumeActor requested")
	api, err := d.api(d.ctx)
	if err != nil {
		return err
	}
	if _, err := api.ResumeActor(d.ctx, &ateapipb.ResumeActorRequest{Actor: d.apiRef}); err != nil {
		return fmt.Errorf("ResumeActor: %w", err)
	}
	return d.waitState("RUNNING")
}

func (d *demo) waitState(want string) error {
	return d.waitStateFor(want, 2*time.Minute)
}

func (d *demo) waitStateFor(want string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		d.mu.Lock()
		state := d.state
		d.mu.Unlock()
		if state == want {
			return nil
		}
		select {
		case <-d.ctx.Done():
			return d.ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
	return fmt.Errorf("Actor did not reach %s within %s", want, timeout)
}

func (d *demo) waitRouteReady() error {
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := d.routerGet("/readyz")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		select {
		case <-d.ctx.Done():
			return d.ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
	return errors.New("the router never returned 200 for the Actor's /readyz")
}

// actorGet sends one HTTP request to the Actor through the router.
func (d *demo) actorGet(path string) (int, string, error) {
	resp, err := d.routerGet(path)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, strings.TrimSpace(string(body)), nil
}

// egressOpen asks the Actor to open one outbound connection to the echo
// target and keep it.
func (d *demo) egressOpen() error {
	code, body, err := d.actorGet("/egress/open?addr=" + echoTarget)
	if err != nil {
		return fmt.Errorf("egress open: %w", err)
	}
	if code != http.StatusOK {
		return fmt.Errorf("egress open: HTTP %d: %s", code, body)
	}
	d.mu.Lock()
	d.egressState = body
	d.mu.Unlock()
	d.logf("egress", "Actor opened an outbound connection through the egress gateway: %s", body)
	return nil
}

// egressSend asks the Actor to send one line over its outbound connection
// and reports what the echo target sent back.
func (d *demo) egressSend() error {
	d.mu.Lock()
	d.egressSent++
	n := d.egressSent
	state := d.state
	d.mu.Unlock()
	start := time.Now()
	code, body, err := d.actorGet(fmt.Sprintf("/egress/send?msg=egress+%d", n))
	if err != nil {
		return fmt.Errorf("egress send: %w", err)
	}
	if code != http.StatusOK {
		return fmt.Errorf("egress send %d: HTTP %d: %s", n, code, body)
	}
	d.logf("egress", "echo target answered egress message %d after %s: %q (Actor was %s)", n, time.Since(start).Round(time.Millisecond), body, state)
	return nil
}

// sequence runs the whole story: connect, echo, open egress, suspend, send
// while suspended, resume, echo again on both connections.
func (d *demo) sequence() error {
	if !d.setBusy() {
		return errors.New("a sequence is already running")
	}
	defer d.clearBusy()

	d.mu.Lock()
	connected := d.ws != nil
	d.mu.Unlock()
	if !connected {
		if err := d.connect(); err != nil {
			return err
		}
	}
	d.mu.Lock()
	egressOpen := d.egressState != ""
	d.mu.Unlock()
	steps := []func() error{
		d.send,
		func() error {
			if egressOpen {
				return nil
			}
			return d.egressOpen()
		},
		d.egressSend,
		func() error { time.Sleep(time.Second); return d.suspend() },
		func() error {
			d.logf("info", "sending on the WebSocket while suspended: the anchor holds the message and, because the template asks for it, wakes the Actor")
			return d.send()
		},
		func() error {
			if err := d.waitStateFor("RUNNING", 30*time.Second); err == nil {
				d.logf("state", "the Actor was woken by data; no explicit Resume was needed")
				return nil
			}
			return d.resume()
		},
		func() error { time.Sleep(time.Second); return d.send() },
		d.egressSend,
		func() error {
			time.Sleep(2 * time.Second)
			d.mu.Lock()
			ok := len(d.pending) == 0 && d.ws != nil
			since := d.wsSince
			egress := d.egressState
			d.mu.Unlock()
			if !ok {
				return errors.New("some echoes are still missing")
			}
			d.logf("done", "same WebSocket (open since %s) and same egress connection (%s) carried data before and after the suspend", since.Format("15:04:05"), egress)
			return nil
		},
	}
	for _, step := range steps {
		if err := step(); err != nil {
			return err
		}
	}
	return nil
}

func (d *demo) setBusy() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.busy {
		return false
	}
	d.busy = true
	return true
}

func (d *demo) clearBusy() {
	d.mu.Lock()
	d.busy = false
	d.mu.Unlock()
}

// slowRequest is the story the feature exists for: the Actor asks for a
// reply that takes a while, is suspended while it waits, and the reply
// itself wakes it, on whichever worker has room.
func (d *demo) slowRequest() error {
	if !d.setBusy() {
		return errors.New("a sequence is already running")
	}
	defer d.clearBusy()
	d.mu.Lock()
	egressOpen := d.egressState != ""
	workerBefore := d.worker
	d.slowCount++
	n := d.slowCount
	d.mu.Unlock()
	if !egressOpen {
		if err := d.egressOpen(); err != nil {
			return err
		}
	}
	before, err := d.egressReplies(0, 0)
	if err != nil {
		return err
	}
	msg := fmt.Sprintf("delay=%s slow reply %d", d.slowDelay, n)
	code, body, err := d.actorGet("/egress/request?msg=" + url.QueryEscape(msg))
	if err != nil {
		return fmt.Errorf("egress request: %w", err)
	}
	if code != http.StatusOK {
		return fmt.Errorf("egress request: HTTP %d: %s", code, body)
	}
	askedAt := time.Now()
	d.logf("egress", "Actor asked the echo target for a reply in %s (slow request %d) and went on; nothing inside it is blocked on the answer", d.slowDelay, n)
	time.Sleep(time.Second)
	if err := d.suspend(); err != nil {
		return err
	}
	d.logf("info", "suspended with the request outstanding: the anchor keeps the connection to the echo target open and will wake the Actor when the reply arrives")
	if err := d.waitStateFor("RUNNING", d.slowDelay+90*time.Second); err != nil {
		return fmt.Errorf("the reply did not wake the Actor: %w", err)
	}
	d.mu.Lock()
	workerAfter, egress := d.worker, d.egressState
	d.mu.Unlock()
	d.logf("state", "the reply woke the Actor %s after the request; worker before %s, after %s", time.Since(askedAt).Round(time.Second), workerBefore, workerAfter)
	if err := d.waitRouteReady(); err != nil {
		return err
	}
	lines, err := d.egressReplies(len(before), 30*time.Second)
	if err != nil {
		return err
	}
	if len(lines) <= len(before) {
		return errors.New("the reply never reached the Actor")
	}
	d.logf("egress", "Actor read %q on the same egress connection (%s), %s after asking", lines[len(lines)-1], egress, time.Since(askedAt).Round(time.Second))
	d.logf("done", "asked on worker %s, suspended, woken by the reply and answered on worker %s, one egress connection throughout", workerBefore, workerAfter)
	return nil
}

// egressReplies lists the reply lines the Actor has collected, waiting up to
// wait for more than after of them.
func (d *demo) egressReplies(after int, wait time.Duration) ([]string, error) {
	code, body, err := d.actorGet(fmt.Sprintf("/egress/replies?after=%d&wait=%s", after, wait))
	if err != nil {
		return nil, fmt.Errorf("egress replies: %w", err)
	}
	if code != http.StatusOK {
		return nil, fmt.Errorf("egress replies: HTTP %d: %s", code, body)
	}
	if body == "" {
		return nil, nil
	}
	return strings.Split(body, "\n"), nil
}

func (d *demo) serveStatus(w http.ResponseWriter, _ *http.Request) {
	d.mu.Lock()
	defer d.mu.Unlock()
	status := map[string]any{
		"actor":     d.ref.Atespace + "/" + d.ref.Name,
		"state":     d.state,
		"worker":    d.worker,
		"connected": d.ws != nil,
		"wsCount":   d.wsCount,
		"wsSince":   "",
		"sent":      d.sent,
		"received":  d.received,
		"pending":   len(d.pending),
		"busy":      d.busy,
		"egress":    d.egressState,
	}
	if d.ws != nil {
		status["wsSince"] = d.wsSince.Format("15:04:05")
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(status)
}

func (d *demo) serveEvents(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-store")
	ch := make(chan event, 64)
	d.mu.Lock()
	backlog := append([]event(nil), d.log...)
	d.subscribers[ch] = struct{}{}
	d.mu.Unlock()
	defer func() {
		d.mu.Lock()
		delete(d.subscribers, ch)
		d.mu.Unlock()
	}()
	write := func(ev event) bool {
		b, _ := json.Marshal(ev)
		if _, err := fmt.Fprintf(w, "data: %s\n\n", b); err != nil {
			return false
		}
		flusher.Flush()
		return true
	}
	for _, ev := range backlog {
		if !write(ev) {
			return
		}
	}
	for {
		select {
		case <-r.Context().Done():
			return
		case ev := <-ch:
			if !write(ev) {
				return
			}
		}
	}
}

func (d *demo) servePage(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(page))
}

const page = `<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8"><title>Connection preservation demo</title>
<style>
 body{margin:0;font:15px/1.5 -apple-system,BlinkMacSystemFont,"Segoe UI",Helvetica,Arial,sans-serif;background:#0d1117;color:#e6edf3}
 header{padding:16px 24px;border-bottom:1px solid #30363d;display:flex;gap:28px;align-items:center;flex-wrap:wrap}
 .k{color:#8b949e;font-size:12px;text-transform:uppercase;letter-spacing:.06em}
 .v{font-size:18px;font-weight:600}
 .badge{display:inline-block;padding:2px 10px;border-radius:999px;font-size:13px;font-weight:600}
 .RUNNING{background:#1a7f37;color:#fff}.SUSPENDED{background:#6e40c9;color:#fff}.RESUMING,.SUSPENDING{background:#9e6a03;color:#fff}.CRASHED{background:#cf222e;color:#fff}
 .bar{padding:12px 24px;display:flex;gap:10px;flex-wrap:wrap;border-bottom:1px solid #30363d}
 button{background:#21262d;color:#e6edf3;border:1px solid #30363d;border-radius:6px;padding:8px 14px;font-size:14px;cursor:pointer}
 button:hover{background:#30363d} button.primary{background:#1f6feb;border-color:#1f6feb} button.danger{border-color:#cf222e;color:#ff7b72}
 #log{padding:12px 24px;font:13px/1.6 ui-monospace,SFMono-Regular,Menlo,monospace;white-space:pre-wrap}
 .row{display:flex;gap:12px}.t{color:#8b949e}.kind{width:52px;color:#8b949e}
 .sent .kind{color:#58a6ff}.recv .kind{color:#3fb950}.state .kind{color:#d2a8ff}.error .kind{color:#ff7b72}.ws .kind{color:#f0883e}.done .kind{color:#3fb950}.egress .kind{color:#79c0ff}
 .done{background:#12261e;border-radius:4px}
</style></head><body>
<header>
 <div><div class="k">Actor</div><div class="v" id="actor">...</div></div>
 <div><div class="k">State</div><div class="v"><span class="badge" id="state">...</span></div></div>
 <div><div class="k">Worker</div><div class="v" id="worker">-</div></div>
 <div><div class="k">WebSocket</div><div class="v" id="ws">not connected</div></div>
 <div><div class="k">Messages</div><div class="v" id="msgs">0 sent, 0 echoed</div></div>
 <div><div class="k">Egress</div><div class="v" id="egress">not open</div></div>
</header>
<div class="bar">
 <button class="primary" onclick="post('sequence')">Run the whole sequence</button>
 <button class="primary" onclick="post('slow')">Slow request: ask, suspend, wake on the reply</button>
 <button onclick="post('connect')">Connect WebSocket</button>
 <button onclick="post('send')">Send a message</button>
 <button onclick="post('egress/open')">Open egress</button>
 <button onclick="post('egress/send')">Send over egress</button>
 <button onclick="post('suspend')">Suspend</button>
 <button onclick="post('resume')">Resume</button>
 <button class="danger" onclick="if(confirm('Delete the demo Actor and quit?'))post('quit')">Quit and delete</button>
</div>
<div id="log"></div>
<script>
function post(a){fetch('/api/'+a,{method:'POST'})}
function status(){fetch('/api/status').then(r=>r.json()).then(s=>{
 document.getElementById('actor').textContent=s.actor;
 var st=document.getElementById('state');st.textContent=s.state;st.className='badge '+s.state;
 document.getElementById('worker').textContent=s.worker||'-';
 document.getElementById('ws').textContent=s.connected?('#'+s.wsCount+' open since '+s.wsSince):'not connected';
 document.getElementById('msgs').textContent=s.sent+' sent, '+s.received+' echoed'+(s.pending?(', '+s.pending+' waiting'):'');
 document.getElementById('egress').textContent=s.egress||'not open';
}).catch(()=>{})}
setInterval(status,700);status();
var log=document.getElementById('log');
new EventSource('/events').onmessage=function(e){var ev=JSON.parse(e.data);
 var row=document.createElement('div');row.className='row '+ev.kind;
 row.innerHTML='<span class="t">'+ev.at+'</span><span class="kind">'+ev.kind+'</span><span>'+ev.text.replace(/&/g,'&amp;').replace(/</g,'&lt;')+'</span>';
 log.appendChild(row);window.scrollTo(0,document.body.scrollHeight)};
</script></body></html>`
