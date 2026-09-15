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

package networking

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/e2e"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/gorilla/websocket"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const networkingAtespace = "networking-e2e"

func TestActorDirectAccess(t *testing.T) {
	ctx := context.Background()
	actorName, actor := createAndResumeActor(t, ctx, "direct", e2e.CounterFixture())
	router := mustRouterClient(t, ctx)
	defer router.Close()

	t.Run("direct", func(t *testing.T) {
		assertDirectActorAccess(t, ctx, e2e.GetClients(), actor)
	})
	t.Run("via ingress", func(t *testing.T) {
		actorRef := resources.ActorRef{Atespace: networkingAtespace, Name: actorName}
		body := waitForRouteReady(t, "Actor access through ingress", func() (*http.Response, error) {
			return router.Get(ctx, actorRef, "/readyz")
		})
		t.Logf("Actor access through ingress succeeded; body: %s", body)
	})
}

// TestActorWebSocket exercises a WebSocket upgrade through the ingress router
// to the Actor. Envoy only proxies an upgrade its HCM declares, so this proves
// the router's websocket upgrade config reaches a real Actor: the client
// upgrades, sends a message, and the Actor echoes it back over the same
// connection.
func TestActorWebSocket(t *testing.T) {
	ctx := context.Background()
	actorName, _ := createAndResumeActor(t, ctx, "websocket", e2e.CounterFixture())
	router := mustRouterClient(t, ctx)
	defer router.Close()

	actorRef := resources.ActorRef{Atespace: networkingAtespace, Name: actorName}
	// Resume the Actor and wait for its route before upgrading, riding out the
	// xDS propagation race the same way the other ingress tests do.
	waitForRouteReady(t, "Actor readyz before WebSocket", func() (*http.Response, error) {
		return router.Get(ctx, actorRef, "/readyz")
	})

	var conn *websocket.Conn
	var lastErr error
	for attempt := 0; attempt < 30; attempt++ {
		c, resp, err := router.DialWebSocket(ctx, actorRef, "/ws")
		if err == nil {
			conn = c
			break
		}
		lastErr = err
		if resp != nil {
			lastErr = fmt.Errorf("%w (handshake status %s)", err, resp.Status)
		}
		time.Sleep(time.Second)
	}
	if conn == nil {
		t.Fatalf("WebSocket upgrade through the router never succeeded: %v", lastErr)
	}
	defer conn.Close()

	if err := conn.WriteMessage(websocket.TextMessage, []byte("hello")); err != nil {
		t.Fatalf("WriteMessage: %v", err)
	}
	if err := conn.SetReadDeadline(time.Now().Add(15 * time.Second)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	_, msg, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("ReadMessage: %v", err)
	}
	if got, want := string(msg), "echo: hello"; got != want {
		t.Errorf("WebSocket echo = %q, want %q", got, want)
	}
	t.Logf("WebSocket echo through ingress succeeded: %q", msg)
}

// counterPreserveFixture is the counter actor with connection preservation
// opted in through its ActorTemplate annotation.
func counterPreserveFixture() e2e.Fixture {
	f := e2e.CounterFixture()
	f.Name = "counter-preserve"
	return f
}

// TestActorWebSocketSurvivesSuspend is the connection-preservation claim end
// to end: a WebSocket opened through the router stays open while the Actor is
// suspended and resumed, and carries data again afterwards on the same
// connection. The Actor's TCP endpoint lives in the connection anchor, so the
// worker it is suspended from and the worker it resumes on do not matter.
func TestActorWebSocketSurvivesSuspend(t *testing.T) {
	ctx := context.Background()
	clients := e2e.GetClients()
	actorName, _ := createAndResumeActor(t, ctx, "ws-preserve", counterPreserveFixture())
	router := mustRouterClient(t, ctx)
	defer router.Close()

	actorRef := resources.ActorRef{Atespace: networkingAtespace, Name: actorName}
	waitForRouteReady(t, "Actor readyz before WebSocket", func() (*http.Response, error) {
		return router.Get(ctx, actorRef, "/readyz")
	})

	conn := dialWebSocketWithRetry(t, ctx, router, actorRef)
	defer conn.Close()

	echo := func(msg string) {
		t.Helper()
		if err := conn.WriteMessage(websocket.TextMessage, []byte(msg)); err != nil {
			t.Fatalf("WriteMessage(%q): %v", msg, err)
		}
		if err := conn.SetReadDeadline(time.Now().Add(30 * time.Second)); err != nil {
			t.Fatalf("SetReadDeadline: %v", err)
		}
		_, got, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("ReadMessage after %q: %v", msg, err)
		}
		if want := "echo: " + msg; string(got) != want {
			t.Fatalf("echo = %q, want %q", got, want)
		}
	}
	echo("before-suspend")

	suspendNetworkingActor(ctx, t, clients, actorName)
	waitForNetworkingActorState(ctx, t, clients, actorName, ateapipb.ActorState_ACTOR_STATE_SUSPENDED)
	t.Logf("actor suspended with the WebSocket still open")

	resumeNetworkingActor(ctx, t, clients, actorName)
	waitForNetworkingActorState(ctx, t, clients, actorName, ateapipb.ActorState_ACTOR_STATE_RUNNING)
	t.Logf("actor resumed")

	echo("after-resume")
	t.Logf("WebSocket survived suspend and resume: echo succeeded on the same connection")
}

// TestActorWakesOnData: the counter-preserve template asks to be woken by
// data. A message sent on a held WebSocket while the Actor is suspended
// resumes it, and the echo arrives, without anyone calling ResumeActor.
func TestActorWakesOnData(t *testing.T) {
	ctx := context.Background()
	actorName, _ := createAndResumeActor(t, ctx, "wake", counterPreserveFixture())
	router := mustRouterClient(t, ctx)
	defer router.Close()
	actorRef := resources.ActorRef{Atespace: networkingAtespace, Name: actorName}
	clients := e2e.GetClients()

	conn := dialWebSocketWithRetry(t, ctx, router, actorRef)
	defer conn.Close()
	if err := conn.WriteMessage(websocket.TextMessage, []byte("before")); err != nil {
		t.Fatal(err)
	}
	if _, msg, err := conn.ReadMessage(); err != nil || string(msg) != "echo: before" {
		t.Fatalf("echo before suspend = %q, %v", msg, err)
	}

	suspendNetworkingActor(ctx, t, clients, actorName)
	waitForNetworkingActorState(ctx, t, clients, actorName, ateapipb.ActorState_ACTOR_STATE_SUSPENDED)
	t.Log("actor suspended with the WebSocket still open")

	if err := conn.WriteMessage(websocket.TextMessage, []byte("wake up")); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Minute))
	_, msg, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("no echo after sending to the suspended actor: %v", err)
	}
	if string(msg) != "echo: wake up" {
		t.Fatalf("echo = %q, want \"echo: wake up\"", msg)
	}
	waitForNetworkingActorState(ctx, t, clients, actorName, ateapipb.ActorState_ACTOR_STATE_RUNNING)
	t.Log("data on the held connection woke the actor and was echoed on the same connection")
}

// echoTargetAddress is the plain TCP echo service the counter demo deploys
// outside any sandbox, reached from an Actor through the egress gateway.
const (
	echoTargetHost    = "echo-target.ate-demo-counter.svc.cluster.local"
	echoTargetAddress = echoTargetHost + ":7777"
)

// TestActorEgressSurvivesSuspend is the egress half of the connection
// preservation claim: an outbound connection the Actor opened before Suspend
// still carries data after Resume, because its Actor-facing end lives in the
// anchor and its far end in the egress gateway.
func TestActorEgressSurvivesSuspend(t *testing.T) {
	ctx := context.Background()
	actorName, _ := createAndResumeActor(t, ctx, "egress-preserve", counterPreserveFixture())
	router := mustRouterClient(t, ctx)
	defer router.Close()
	actorRef := resources.ActorRef{Atespace: networkingAtespace, Name: actorName}
	clients := e2e.GetClients()

	get := routerGet(t, ctx, router, actorRef)
	waitForRouteReady(t, "Actor readyz before egress", func() (*http.Response, error) {
		return router.Get(ctx, actorRef, "/readyz")
	})
	if code, body := get("/egress/open?addr=" + echoTargetAddress); code != http.StatusOK {
		t.Fatalf("egress open returned HTTP %d: %s", code, body)
	}
	if code, body := get("/egress/send?msg=before"); code != http.StatusOK || body != "before" {
		t.Fatalf("egress echo before suspend = %d %q, want 200 \"before\"", code, body)
	}
	_, statusBefore := get("/egress/status")
	t.Logf("egress connection %s", statusBefore)

	suspendNetworkingActor(ctx, t, clients, actorName)
	waitForNetworkingActorState(ctx, t, clients, actorName, ateapipb.ActorState_ACTOR_STATE_SUSPENDED)
	t.Log("actor suspended with the egress connection still open")
	resumeNetworkingActor(ctx, t, clients, actorName)
	waitForNetworkingActorState(ctx, t, clients, actorName, ateapipb.ActorState_ACTOR_STATE_RUNNING)
	t.Log("actor resumed")

	waitForRouteReady(t, "Actor readyz after resume", func() (*http.Response, error) {
		return router.Get(ctx, actorRef, "/readyz")
	})
	if code, body := get("/egress/send?msg=after"); code != http.StatusOK || body != "after" {
		t.Fatalf("egress echo after resume = %d %q, want 200 \"after\"", code, body)
	}
	if _, statusAfter := get("/egress/status"); statusAfter != statusBefore {
		t.Fatalf("egress connection changed across suspend: before %q, after %q", statusBefore, statusAfter)
	}
	t.Log("egress connection survived suspend and resume: echo succeeded on the same connection")
}

// TestActorEgressResponseWakesActor: the Actor asks the echo target for a
// reply that takes 20 seconds, is suspended while it waits, and is woken by
// that reply arriving at the anchor. Nothing calls Resume. The reply is then
// read on the connection the Actor opened before the suspend, whichever
// worker it came back on.
func TestActorEgressResponseWakesActor(t *testing.T) {
	ctx := context.Background()
	actorName, _ := createAndResumeActor(t, ctx, "egress-wake", counterPreserveFixture())
	router := mustRouterClient(t, ctx)
	defer router.Close()
	actorRef := resources.ActorRef{Atespace: networkingAtespace, Name: actorName}
	clients := e2e.GetClients()
	get := routerGet(t, ctx, router, actorRef)

	waitForRouteReady(t, "Actor readyz before egress", func() (*http.Response, error) {
		return router.Get(ctx, actorRef, "/readyz")
	})
	if code, body := get("/egress/open?addr=" + echoTargetAddress); code != http.StatusOK {
		t.Fatalf("egress open returned HTTP %d: %s", code, body)
	}
	_, statusBefore := get("/egress/status")
	_, workerBefore := networkingActorStatus(ctx, t, clients, actorName)

	const replyDelay = 20 * time.Second
	if code, body := get("/egress/request?msg=" + url.QueryEscape("delay=20s late")); code != http.StatusOK {
		t.Fatalf("egress request returned HTTP %d: %s", code, body)
	}
	askedAt := time.Now()
	t.Logf("asked the echo target for a reply in %s while on %s", replyDelay, workerBefore)

	workerAfter := suspendAndWaitForWake(ctx, t, clients, actorName, askedAt, replyDelay)
	t.Logf("the reply woke the actor %s after the request; worker before %q, after %q",
		time.Since(askedAt).Round(time.Second), workerBefore, workerAfter)

	waitForRouteReady(t, "Actor readyz after the wake", func() (*http.Response, error) {
		return router.Get(ctx, actorRef, "/readyz")
	})
	if code, body := get("/egress/replies?wait=30s"); code != http.StatusOK || body != "late" {
		t.Fatalf("egress replies after the wake = %d %q, want 200 \"late\"", code, body)
	}
	if _, statusAfter := get("/egress/status"); statusAfter != statusBefore {
		t.Fatalf("egress connection changed across the suspend: before %q, after %q", statusBefore, statusAfter)
	}
	t.Log("the reply was delivered on the connection opened before the suspend")
}

// TestActorHTTPResponseWakesActor: a goroutine in the Actor blocks in a
// plain http.Get whose response takes 20 seconds. The Actor is suspended
// while the call is blocked, the response wakes it, and the call returns as
// if nothing had happened.
func TestActorHTTPResponseWakesActor(t *testing.T) {
	ctx := context.Background()
	actorName, _ := createAndResumeActor(t, ctx, "http-wake", counterPreserveFixture())
	router := mustRouterClient(t, ctx)
	defer router.Close()
	actorRef := resources.ActorRef{Atespace: networkingAtespace, Name: actorName}
	clients := e2e.GetClients()
	get := routerGet(t, ctx, router, actorRef)

	waitForRouteReady(t, "Actor readyz before the fetch", func() (*http.Response, error) {
		return router.Get(ctx, actorRef, "/readyz")
	})
	_, workerBefore := networkingActorStatus(ctx, t, clients, actorName)
	const replyDelay = 20 * time.Second
	target := "http://" + echoTargetHost + "/delay?d=20s"
	if code, body := get("/fetch/start?url=" + url.QueryEscape(target)); code != http.StatusOK || !strings.HasSuffix(body, ", request sent") {
		t.Fatalf("fetch start = HTTP %d %q, want the request on the wire before the suspend", code, body)
	}
	askedAt := time.Now()
	t.Logf("actor on %s is blocked in http.Get(%s)", workerBefore, target)

	workerAfter := suspendAndWaitForWake(ctx, t, clients, actorName, askedAt, replyDelay)
	t.Logf("the response woke the actor %s after the call; worker before %q, after %q",
		time.Since(askedAt).Round(time.Second), workerBefore, workerAfter)

	waitForRouteReady(t, "Actor readyz after the wake", func() (*http.Response, error) {
		return router.Get(ctx, actorRef, "/readyz")
	})
	code, body := get("/fetch/result?wait=30s")
	if code != http.StatusOK || !strings.HasPrefix(body, "200 delayed 20s after ") {
		t.Fatalf("fetch result after the wake = %d %q, want \"200 delayed 20s after ...\"", code, body)
	}
	t.Logf("http.Get returned normally: %s", body)
}

// suspendAndWaitForWake suspends the Actor while a reply is outstanding,
// checks it stays suspended until that reply is due, waits for data to bring
// it back, and returns the worker it came back on.
func suspendAndWaitForWake(ctx context.Context, t *testing.T, clients *e2e.Clients, actorName string, askedAt time.Time, replyDelay time.Duration) string {
	t.Helper()
	suspendNetworkingActor(ctx, t, clients, actorName)
	waitForNetworkingActorState(ctx, t, clients, actorName, ateapipb.ActorState_ACTOR_STATE_SUSPENDED)
	t.Log("actor suspended while its request is outstanding")

	// Nothing may wake the Actor before the reply exists. The check stops a
	// little before the reply is due so it cannot race the wake.
	for time.Now().Before(askedAt.Add(replyDelay - 5*time.Second)) {
		if state, worker := networkingActorStatus(ctx, t, clients, actorName); state != ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
			t.Fatalf("actor left SUSPENDED (%v on %q) before its reply could exist", state, worker)
		}
		time.Sleep(time.Second)
	}
	waitForNetworkingActorState(ctx, t, clients, actorName, ateapipb.ActorState_ACTOR_STATE_RUNNING)
	_, worker := networkingActorStatus(ctx, t, clients, actorName)
	return worker
}

// routerGet returns a helper that sends one GET to the Actor through the
// router and returns the status code and the trimmed body.
func routerGet(t *testing.T, ctx context.Context, router *e2e.RouterClient, actorRef resources.ActorRef) func(path string) (int, string) {
	return func(path string) (int, string) {
		t.Helper()
		resp, err := router.Get(ctx, actorRef, path)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, strings.TrimSpace(string(body))
	}
}

// networkingActorStatus returns the Actor's state and its worker pod, which
// is empty while it has none.
func networkingActorStatus(ctx context.Context, t *testing.T, clients *e2e.Clients, name string) (ateapipb.ActorState, string) {
	t.Helper()
	resp, err := clients.SubstrateAPI.GetActor(ctx, &ateapipb.GetActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: networkingAtespace, Name: name},
	})
	if err != nil {
		t.Fatalf("GetActor %s: %v", name, err)
	}
	return resp.GetStatus().GetState(), resp.GetStatus().GetWorkerAssignment().GetWorkerPod()
}

func dialWebSocketWithRetry(t *testing.T, ctx context.Context, router *e2e.RouterClient, actorRef resources.ActorRef) *websocket.Conn {
	t.Helper()
	var lastErr error
	for attempt := 0; attempt < 30; attempt++ {
		c, resp, err := router.DialWebSocket(ctx, actorRef, "/ws")
		if err == nil {
			return c
		}
		lastErr = err
		if resp != nil {
			lastErr = fmt.Errorf("%w (handshake status %s)", err, resp.Status)
		}
		time.Sleep(time.Second)
	}
	t.Fatalf("WebSocket upgrade through the router never succeeded: %v", lastErr)
	return nil
}

func suspendNetworkingActor(ctx context.Context, t *testing.T, clients *e2e.Clients, name string) {
	t.Helper()
	if _, err := clients.SubstrateAPI.SuspendActor(ctx, &ateapipb.SuspendActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: networkingAtespace, Name: name},
	}); err != nil {
		t.Fatalf("failed to suspend actor %q: %v", name, err)
	}
}

func resumeNetworkingActor(ctx context.Context, t *testing.T, clients *e2e.Clients, name string) {
	t.Helper()
	if _, err := clients.SubstrateAPI.ResumeActor(ctx, &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: networkingAtespace, Name: name},
	}); err != nil {
		t.Fatalf("failed to resume actor %q: %v", name, err)
	}
}

func waitForNetworkingActorState(ctx context.Context, t *testing.T, clients *e2e.Clients, name string, want ateapipb.ActorState) {
	t.Helper()
	deadline := time.Now().Add(120 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := clients.SubstrateAPI.GetActor(ctx, &ateapipb.GetActorRequest{
			Actor: &ateapipb.ObjectRef{Atespace: networkingAtespace, Name: name},
		})
		if err == nil && resp.GetStatus().GetState() == want {
			return
		}
		time.Sleep(time.Second)
	}
	t.Fatalf("timed out waiting for actor %q to reach %v", name, want)
}

// TestActorEgress exercises the full egress path. The Actor's outbound TCP
// connection is transparently redirected by nftables into atunnel, wrapped in
// mTLS with the Actor's own actor-identity certificate plus an HTTP CONNECT to
// atenet-egress, authorized there against that certificate, and only then
// dialed out. A masqueraded (pre-gateway) egress would also return 200, so this
// asserts the gateway is deployed and that it did not reject the Actor.
func TestActorEgress(t *testing.T) {
	ctx := context.Background()
	actorName, _ := createAndResumeActor(t, ctx, "egress", e2e.EgressFixture())
	router := mustRouterClient(t, ctx)
	defer router.Close()

	actorRef := resources.ActorRef{Atespace: networkingAtespace, Name: actorName}
	status, body := fetchThroughEgressActor(t, ctx, router, actorRef, "http://example.com/")
	if status != http.StatusOK {
		t.Fatalf("Actor egress fetch returned HTTP %d, want 200; body: %s", status, body)
	}
	t.Logf("Actor egress fetch succeeded; body: %s", body)
}

// TestActorEgressHTTPS covers the same path as TestActorEgress with a TLS
// origin, where the gateway cannot see inside the request. atenet-egress
// authorizes the CONNECT against the Actor's actor-identity certificate and
// then relays raw TCP: it never decrypts, so the TLS session runs end to end
// between the Actor and the origin.
func TestActorEgressHTTPS(t *testing.T) {
	ctx := context.Background()
	actorName, _ := createAndResumeActor(t, ctx, "egress-https", e2e.EgressFixture())
	router := mustRouterClient(t, ctx)
	defer router.Close()

	// Bound the access-log scan below to lines this test could have produced.
	// The slack absorbs clock skew between here and the gateway's node.
	since := metav1.NewTime(time.Now().Add(-1 * time.Minute))

	actorRef := resources.ActorRef{Atespace: networkingAtespace, Name: actorName}
	status, body := fetchThroughEgressActor(t, ctx, router, actorRef, "https://example.com/")
	if status != http.StatusOK {
		t.Fatalf("Actor HTTPS egress fetch returned HTTP %d, want 200; body: %s", status, body)
	}
	t.Logf("Actor HTTPS egress fetch succeeded; body: %s", body)

	assertEgressGatewayConnect(t, ctx, since, actorName, "443")
}

// fetchThroughEgressActor asks the egress demo Actor to fetch url and returns
// the status and body it echoes back. Retries a non-200 response for up to
// 30s: ResumeActor can return before its route reaches atenet-router's xDS
// snapshot, and a request sent in that window sees a transient 503.
func fetchThroughEgressActor(t *testing.T, ctx context.Context, router *e2e.RouterClient, actorRef resources.ActorRef, url string) (int, []byte) {
	t.Helper()
	payload, err := json.Marshal(map[string]string{"url": url})
	if err != nil {
		t.Fatalf("marshaling the fetch request for %s: %v", url, err)
	}

	const timeout = 30 * time.Second
	deadline := time.Now().Add(timeout)
	for {
		response, err := router.PostJSON(ctx, actorRef, "/", payload)
		if err != nil {
			t.Fatalf("POST %s to egress Actor through ingress: %v", url, err)
		}
		body, err := io.ReadAll(response.Body)
		response.Body.Close()
		if err != nil {
			t.Fatalf("reading egress response body (HTTP %d): %v", response.StatusCode, err)
		}
		if response.StatusCode == http.StatusOK || time.Now().After(deadline) {
			return response.StatusCode, body
		}
		t.Logf("fetch through egress Actor returned HTTP %d; retrying...", response.StatusCode)
		time.Sleep(1 * time.Second)
	}
}

// assertEgressGatewayConnect waits for the atenet-egress access log to show a
// CONNECT to port opened by actorName.
func assertEgressGatewayConnect(t *testing.T, ctx context.Context, since metav1.Time, actorName, port string) {
	t.Helper()
	want := fmt.Sprintf("a CONNECT to port %s by actor %s", port, actorName)
	waitForAccessLog(t, ctx, since, want, func(lines []string) (bool, error) {
		for _, line := range lines {
			authority, ok := accessLogField(line, "authority")
			if !ok || !strings.HasSuffix(authority, ":"+port) {
				continue
			}
			if !strings.Contains(line, "/actor/"+actorName) {
				continue
			}
			t.Logf("egress gateway tunneled the request: %s", line)
			return true, nil
		}
		return false, nil
	})
}

// waitForAccessLog polls the atenet-egress access log, across every gateway
// replica, until predicate accepts the lines written since.
func waitForAccessLog(t *testing.T, ctx context.Context, since metav1.Time, want string, predicate func(lines []string) (bool, error)) {
	t.Helper()
	const (
		gatewayNamespace = "ate-system"
		gatewaySelector  = "app=atenet-egress"
		gatewayContainer = "envoy"
		// The access log's line prefix, from the HttpConnectionManager
		// text_format_source in manifests/ate-install/atenet-egress.yaml.
		accessLogPrefix = "[egress] "
	)

	clients := e2e.GetClients()
	pods, err := clients.K8s.CoreV1().Pods(gatewayNamespace).List(ctx, metav1.ListOptions{LabelSelector: gatewaySelector})
	if err != nil {
		t.Fatalf("listing %s pods in %s: %v", gatewaySelector, gatewayNamespace, err)
	}
	if len(pods.Items) == 0 {
		t.Fatalf("no %s pods in %s; the egress gateway is not deployed", gatewaySelector, gatewayNamespace)
	}

	// Poll for the access log line (it may show up asynchronously from the actual traffic).
	const timeout = 30 * time.Second
	deadline := time.Now().Add(timeout)
	for {
		var lines []string
		for _, pod := range pods.Items {
			logs, err := clients.K8s.CoreV1().Pods(gatewayNamespace).GetLogs(pod.Name, &corev1.PodLogOptions{
				Container: gatewayContainer,
				SinceTime: &since,
			}).DoRaw(ctx)
			if err != nil {
				t.Fatalf("reading logs of %s/%s: %v", gatewayNamespace, pod.Name, err)
			}
			for line := range strings.SplitSeq(string(logs), "\n") {
				if strings.Contains(line, accessLogPrefix) {
					lines = append(lines, line)
				}
			}
		}

		matched, err := predicate(lines)
		if err != nil {
			t.Fatalf("looking for %s in the atenet-egress access log: %v", want, err)
		}
		if matched {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("no atenet-egress access-log line for %s after %v; lines seen:\n%s",
				want, timeout, strings.Join(lines, "\n"))
		}
		time.Sleep(1 * time.Second)
	}
}

// accessLogField returns the value of the key=value field named key in an Envoy
// access log line whose fields are separated by spaces.
func accessLogField(line, key string) (string, bool) {
	_, rest, ok := strings.Cut(line, key+"=")
	if !ok {
		return "", false
	}
	value, _, _ := strings.Cut(rest, " ")
	return value, true
}

func createAndResumeActor(t *testing.T, ctx context.Context, prefix string, template e2e.Fixture) (string, *ateapipb.Actor) {
	t.Helper()
	clients := e2e.GetClients()
	actorName := fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
	actorRef := &ateapipb.ObjectRef{Atespace: networkingAtespace, Name: actorName}

	t.Logf("creating actor %s/%s", networkingAtespace, actorName)
	_, _ = clients.SubstrateAPI.CreateAtespace(ctx, &ateapipb.CreateAtespaceRequest{
		Atespace: &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: networkingAtespace}},
	})
	if _, err := clients.SubstrateAPI.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:               &ateapipb.ResourceMetadata{Atespace: networkingAtespace, Name: actorName},
		ActorTemplateNamespace: template.Namespace,
		ActorTemplateName:      template.Name,
	}}); err != nil {
		t.Fatalf("CreateActor from %s/%s: %v (deploy the fixture with %s)", template.Namespace, template.Name, err, template.DeployWith)
	}
	t.Cleanup(func() {
		_, _ = clients.SubstrateAPI.SuspendActor(context.Background(), &ateapipb.SuspendActorRequest{Actor: actorRef})
		_, _ = clients.SubstrateAPI.DeleteActor(context.Background(), &ateapipb.DeleteActorRequest{Actor: actorRef})
	})

	resumeResponse, err := clients.SubstrateAPI.ResumeActor(ctx, &ateapipb.ResumeActorRequest{Actor: actorRef})
	if err != nil {
		t.Fatalf("ResumeActor: %v", err)
	}
	t.Logf("resumed actor %s/%s", networkingAtespace, actorName)
	return actorName, resumeResponse.GetActor()
}

func mustRouterClient(t *testing.T, ctx context.Context) *e2e.RouterClient {
	t.Helper()
	router, err := e2e.NewRouterClient(ctx)
	if err != nil {
		t.Fatalf("NewRouterClient: %v", err)
	}
	return router
}

// waitForRouteReady retries request until it returns a 200 response or
// timeout elapses, and returns that response's body. This rides out the race
// between ResumeActor returning and its route reaching atenet-router's xDS
// snapshot: a request sent in that window sees a transient 503 connection
// timeout, not a real failure, and every caller through the router hits it.
// what names the request in log/failure output.
func waitForRouteReady(t *testing.T, what string, request func() (*http.Response, error)) string {
	t.Helper()
	const timeout = 30 * time.Second
	deadline := time.Now().Add(timeout)
	for {
		response, err := request()
		if err != nil {
			t.Fatalf("%s: %v", what, err)
		}
		body, err := io.ReadAll(response.Body)
		response.Body.Close()
		if err != nil {
			t.Fatalf("reading %s response body (HTTP %d): %v", what, response.StatusCode, err)
		}
		if response.StatusCode == http.StatusOK {
			return string(body)
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s returned HTTP %d after %v; body: %s", what, response.StatusCode, timeout, body)
		}
		t.Logf("%s returned HTTP %d; retrying...", what, response.StatusCode)
		time.Sleep(1 * time.Second)
	}
}

func assertDirectActorAccess(t *testing.T, ctx context.Context, clients *e2e.Clients, actor *ateapipb.Actor) {
	t.Helper()
	if actor.GetStatus().GetWorkerAssignment().GetWorkerNamespace() == "" || actor.GetStatus().GetWorkerAssignment().GetWorkerPod() == "" {
		t.Fatalf("resumed Actor has no worker pod assignment: %+v", actor)
	}

	// The Kubernetes pod proxy performs this request from inside the cluster to
	// the assigned worker's port 80. It bypasses atenet-router and therefore
	// verifies that the old direct path remains unavailable without relying on
	// the test runner having a route to the pod CIDR.
	result := clients.K8s.CoreV1().RESTClient().Get().
		Namespace(actor.GetStatus().GetWorkerAssignment().GetWorkerNamespace()).
		Resource("pods").
		Name(actor.GetStatus().GetWorkerAssignment().GetWorkerPod() + ":80").
		SubResource("proxy").
		Suffix("readyz").
		Do(ctx)
	body, err := result.Raw()

	if err == nil {
		t.Fatalf("direct Actor access through %s/%s:80 unexpectedly succeeded; body: %s", actor.GetStatus().GetWorkerAssignment().GetWorkerNamespace(), actor.GetStatus().GetWorkerAssignment().GetWorkerPod(), body)
	}
	t.Logf("direct Actor access through %s/%s:80 was blocked as expected: %v", actor.GetStatus().GetWorkerAssignment().GetWorkerNamespace(), actor.GetStatus().GetWorkerAssignment().GetWorkerPod(), err)
}
