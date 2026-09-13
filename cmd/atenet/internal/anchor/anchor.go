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

// Package anchor is the connection anchor: a stable place where an actor's TCP
// connections terminate so they survive the actor being suspended and resumed
// on any worker. Each actor gets its own userspace network stack here. The
// worker's frame shuttle attaches a tunnel that carries the sandbox's Ethernet
// frames to that stack; the router's ingress is proxied into the stack the way
// atunnel proxies it on a worker today. When the tunnel goes away the stack and
// its endpoints stay, holding every connection until a worker reattaches.
package anchor

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"gvisor.dev/gvisor/pkg/tcpip"
	"gvisor.dev/gvisor/pkg/tcpip/adapters/gonet"
	"gvisor.dev/gvisor/pkg/tcpip/network/ipv4"

	"github.com/agent-substrate/substrate/internal/anchornet"
	"github.com/agent-substrate/substrate/internal/anchortun"
	"github.com/agent-substrate/substrate/internal/ateomnet/actoraddr"
	"github.com/agent-substrate/substrate/internal/atunnel"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/internal/serverboot"
)

const (
	// spiffePrefix is the trust domain every workload identity lives under.
	spiffePrefix = "spiffe://"
	// probeTimeout bounds one readiness probe through the actor's stack.
	probeTimeout = 5 * time.Second
	// headerTimeout bounds how long a new tunnel may take to send its header.
	headerTimeout = 10 * time.Second
	// maxSweepInterval bounds how long an expired hold outlives its TTL.
	maxSweepInterval = time.Minute
)

// Config configures an Anchor.
type Config struct {
	IngressListen string
	AttachListen  string
	ControlListen string
	// MetricsAddr serves Prometheus metrics and the health endpoints.
	MetricsAddr string
	// IngressCredentialBundlePath is the pod-identity certificate the ingress
	// listener presents; the router validates it by SPIFFE prefix, as it does
	// a worker's atunnel.
	IngressCredentialBundlePath string
	// CredentialBundlePath is the service-DNS certificate the attach and
	// control listeners present, so workers can verify the anchor by its
	// Service name.
	CredentialBundlePath string
	// TrustBundlePath verifies router and worker client certificates.
	TrustBundlePath string
	// RouterClientID is the only identity allowed on the ingress listener.
	RouterClientID string
	// HoldTTL drops an actor's stack, resetting its held connections, when no
	// worker has reattached for this long. Zero holds forever.
	HoldTTL time.Duration
	// LogFrames logs one line per frame at debug level.
	LogFrames bool
	// Egress configures actors' outbound connections and DNS.
	Egress EgressConfig
	// Ateapi lets the anchor check attaches against the control plane and
	// wake held actors.
	Ateapi AteapiConfig
	// ConnectListen serves the router's raw CONNECT tunnels into actors.
	ConnectListen string
	// StatusListen serves /statusz over plain HTTP. Empty disables it.
	StatusListen string
	// Hold bounds held state and wake attempts.
	Hold HoldConfig
}

// Anchor holds one network stack per actor and the listeners that reach them.
type Anchor struct {
	cfg         Config
	clientCAs   *x509.CertPool
	metrics     *Metrics
	now         func() time.Time
	dnsUpstream string
	// controlPlane is nil when the anchor runs without ateapi: attaches are
	// then not checked and held actors are not woken.
	controlPlane controlPlane

	mu     sync.Mutex
	actors map[resources.ActorRef]*actorEntry
}

// actorEntry is one actor's stack, the proxy into it, and its current tunnel.
type actorEntry struct {
	ref      resources.ActorRef
	actorUID string
	stack    *anchornet.Stack
	// transport pools connections into this stack only. Every actor has the
	// same address inside its own stack, so a pool shared across actors would
	// hand one actor's connection to another.
	transport *http.Transport
	proxy     *httputil.ReverseProxy
	// egress is nil when the anchor runs without a certificate broker.
	egress *actorEgress

	framesToActor   atomic.Int64
	framesFromActor atomic.Int64

	mu           sync.Mutex
	detach       context.CancelFunc
	activationID string
	// heldSince is when the last tunnel went away; zero while attached.
	heldSince time.Time
	// wakeOnData is the template's choice, carried in the attach header.
	wakeOnData bool
	lastWake   time.Time
}

// ingressCall is the per-request state the proxy callbacks need.
type ingressCall struct {
	entry  *actorEntry
	failed atomic.Bool
}

type ingressCallKey struct{}

// New validates the TLS material and creates the instruments.
func New(cfg Config) (*Anchor, error) {
	if cfg.CredentialBundlePath == "" || cfg.IngressCredentialBundlePath == "" || cfg.TrustBundlePath == "" {
		return nil, errors.New("anchor: credential and trust bundle paths are required")
	}
	if cfg.RouterClientID == "" {
		return nil, errors.New("anchor: router client identity is required")
	}
	for _, p := range []string{cfg.CredentialBundlePath, cfg.IngressCredentialBundlePath} {
		if _, err := loadCredentialBundle(p); err != nil {
			return nil, err
		}
	}
	trustPEM, err := os.ReadFile(cfg.TrustBundlePath)
	if err != nil {
		return nil, fmt.Errorf("anchor: reading trust bundle: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(trustPEM) {
		return nil, fmt.Errorf("anchor: trust bundle %q contains no certificates", cfg.TrustBundlePath)
	}
	metrics, err := NewMetrics()
	if err != nil {
		return nil, err
	}
	dnsUpstream := cfg.Egress.DNSUpstream
	if cfg.Egress.enabled() && dnsUpstream == "" {
		if dnsUpstream, err = defaultDNSUpstream("/etc/resolv.conf"); err != nil {
			return nil, err
		}
	}
	var control controlPlane
	if cfg.Ateapi.enabled() {
		if control, err = newControlPlane(cfg.Ateapi); err != nil {
			return nil, err
		}
	}
	return &Anchor{cfg: cfg, clientCAs: pool, metrics: metrics, now: time.Now, dnsUpstream: dnsUpstream, controlPlane: control, actors: map[resources.ActorRef]*actorEntry{}}, nil
}

func loadCredentialBundle(path string) (*tls.Certificate, error) {
	pemBytes, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("anchor: reading credential bundle: %w", err)
	}
	cert, err := tls.X509KeyPair(pemBytes, pemBytes)
	if err != nil {
		return nil, fmt.Errorf("anchor: parsing credential bundle: %w", err)
	}
	return &cert, nil
}

// tlsConfig builds a server config that presents the anchor's identity and
// requires a client certificate from the pod-identity CA. verify further
// checks the peer's SPIFFE identity.
func (a *Anchor) tlsConfig(credentialPath string, verify func(uris []*url.URL) error) *tls.Config {
	return &tls.Config{
		MinVersion: tls.VersionTLS12,
		GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
			return loadCredentialBundle(credentialPath)
		},
		ClientAuth: tls.RequireAndVerifyClientCert,
		ClientCAs:  a.clientCAs,
		VerifyConnection: func(cs tls.ConnectionState) error {
			if len(cs.PeerCertificates) == 0 {
				return errors.New("anchor: client certificate is required")
			}
			return verify(cs.PeerCertificates[0].URIs)
		},
	}
}

func (a *Anchor) verifyRouter(uris []*url.URL) error {
	for _, u := range uris {
		if u.String() == a.cfg.RouterClientID {
			return nil
		}
	}
	return fmt.Errorf("anchor: client is not %q", a.cfg.RouterClientID)
}

// verifyWorkload accepts any pod identity in the trust domain. The anchor
// still only serves an actor that a worker has attached; checking the
// attaching worker against the control plane's assignment is a follow-up.
func verifyWorkload(uris []*url.URL) error {
	for _, u := range uris {
		if strings.HasPrefix(u.String(), spiffePrefix) {
			return nil
		}
	}
	return errors.New("anchor: client has no workload identity")
}

// Run serves the listeners and sweeps expired holds until ctx is canceled.
func (a *Anchor) Run(ctx context.Context) error {
	ingressLis, err := net.Listen("tcp", a.cfg.IngressListen)
	if err != nil {
		return fmt.Errorf("anchor: ingress listen: %w", err)
	}
	attachLis, err := net.Listen("tcp", a.cfg.AttachListen)
	if err != nil {
		return fmt.Errorf("anchor: attach listen: %w", err)
	}
	controlLis, err := net.Listen("tcp", a.cfg.ControlListen)
	if err != nil {
		return fmt.Errorf("anchor: control listen: %w", err)
	}

	if a.cfg.MetricsAddr != "" {
		mp, err := serverboot.InitMetrics(ctx, ServiceName)
		if err != nil {
			return fmt.Errorf("anchor: initializing metrics: %w", err)
		}
		defer serverboot.ShutdownProvider("MeterProvider", mp.Shutdown)
		go serverboot.StartMetricsServer(ctx, serverboot.MetricsServerOptions{
			Addr:          a.cfg.MetricsAddr,
			Readiness:     &serverboot.Readiness{},
			EnableHealthz: true,
		})
	}

	ingressSrv := &http.Server{Handler: a, TLSConfig: a.tlsConfig(a.cfg.IngressCredentialBundlePath, a.verifyRouter), ReadHeaderTimeout: headerTimeout}
	controlMux := http.NewServeMux()
	controlMux.HandleFunc("/probe", a.serveProbe)
	controlMux.HandleFunc("/quiesce", a.serveQuiesce)
	controlMux.HandleFunc("/release", a.serveRelease)
	controlSrv := &http.Server{Handler: controlMux, TLSConfig: a.tlsConfig(a.cfg.CredentialBundlePath, verifyWorkload), ReadHeaderTimeout: headerTimeout}
	attachTLS := tls.NewListener(attachLis, a.tlsConfig(a.cfg.CredentialBundlePath, verifyWorkload))

	errc := make(chan error, 5)
	go func() { errc <- ingressSrv.ServeTLS(ingressLis, "", "") }()
	go func() { errc <- controlSrv.ServeTLS(controlLis, "", "") }()
	go func() { errc <- a.serveAttach(ctx, attachTLS) }()
	connectSrv, err := a.startConnectListener(errc)
	if err != nil {
		return err
	}
	statusSrv := a.startStatusListener(errc)
	go a.sweepLoop(ctx)
	slog.InfoContext(ctx, "anchor serving",
		slog.String("ingress", a.cfg.IngressListen),
		slog.String("connect", a.cfg.ConnectListen),
		slog.String("attach", a.cfg.AttachListen),
		slog.String("control", a.cfg.ControlListen),
		slog.Duration("holdTTL", a.cfg.HoldTTL))

	select {
	case <-ctx.Done():
	case err = <-errc:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.ErrorContext(ctx, "anchor listener failed", slog.Any("err", err))
		}
	}
	_ = ingressSrv.Close()
	_ = controlSrv.Close()
	_ = attachTLS.Close()
	if connectSrv != nil {
		_ = connectSrv.Close()
	}
	if statusSrv != nil {
		_ = statusSrv.Close()
	}
	return err
}

// startStatusListener serves /statusz over plain HTTP when configured.
func (a *Anchor) startStatusListener(errc chan<- error) *http.Server {
	if a.cfg.StatusListen == "" {
		return nil
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/statusz", a.serveStatusz)
	srv := &http.Server{Addr: a.cfg.StatusListen, Handler: mux, ReadHeaderTimeout: headerTimeout}
	go func() { errc <- srv.ListenAndServe() }()
	return srv
}

// startConnectListener serves the router's raw CONNECT tunnels, over HTTP/1.1
// or HTTP/2 like a worker's atunnel, when a listen address is configured.
func (a *Anchor) startConnectListener(errc chan<- error) (*http.Server, error) {
	if a.cfg.ConnectListen == "" {
		return nil, nil
	}
	lis, err := net.Listen("tcp", a.cfg.ConnectListen)
	if err != nil {
		return nil, fmt.Errorf("anchor: connect listen: %w", err)
	}
	tlsCfg := a.tlsConfig(a.cfg.IngressCredentialBundlePath, a.verifyRouter)
	tlsCfg.NextProtos = []string{"h2", "http/1.1"}
	srv := &http.Server{Handler: http.HandlerFunc(a.serveConnect), TLSConfig: tlsCfg, ReadHeaderTimeout: headerTimeout}
	go func() { errc <- srv.ServeTLS(lis, "", "") }()
	return srv, nil
}

// serveAttach accepts worker tunnels. Each stream starts with one attach
// header frame and then carries the actor's Ethernet frames.
func (a *Anchor) serveAttach(ctx context.Context, lis net.Listener) error {
	for {
		conn, err := lis.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return fmt.Errorf("anchor: accepting attach: %w", err)
		}
		go a.handleAttach(ctx, conn)
	}
}

func (a *Anchor) handleAttach(ctx context.Context, conn net.Conn) {
	defer conn.Close()
	fc := anchortun.NewFrameConn(conn)

	_ = conn.SetReadDeadline(time.Now().Add(headerTimeout))
	first, err := fc.ReadFrame()
	if err != nil {
		slog.WarnContext(ctx, "anchor attach: reading header", slog.Any("err", err))
		a.metrics.recordAttach(ctx, outcomeBadHeader, "")
		return
	}
	hdr, err := anchortun.UnmarshalAttachHeader(first)
	if err != nil {
		slog.WarnContext(ctx, "anchor attach: bad header", slog.Any("err", err))
		a.metrics.recordAttach(ctx, outcomeBadHeader, "")
		return
	}
	_ = conn.SetReadDeadline(time.Time{})

	if err := a.verifyAttach(ctx, hdr, peerPodUID(conn)); err != nil {
		outcome := outcomeRejectedAssignment
		if errors.Is(err, errIdentityMismatch) {
			outcome = outcomeRejectedIdentity
		}
		slog.WarnContext(ctx, "anchor attach: rejected", slog.Any("actor", hdr.Ref()), slog.String("worker", hdr.WorkerPodUID), slog.Any("err", err))
		a.metrics.recordAttach(ctx, outcome, string(hdr.Boot))
		return
	}

	entry, err := a.getOrCreate(ctx, hdr.Ref(), hdr.ActorUID, hdr.Boot == anchortun.BootFresh)
	if err != nil {
		slog.ErrorContext(ctx, "anchor attach: creating stack", slog.Any("actor", hdr.Ref()), slog.Any("err", err))
		a.metrics.recordAttach(ctx, outcomeStackError, string(hdr.Boot))
		return
	}
	a.metrics.recordAttach(ctx, outcomeAttached, string(hdr.Boot))

	tunnelCtx, cancel := context.WithCancel(ctx)
	entry.mu.Lock()
	if entry.detach != nil {
		entry.detach()
	}
	entry.detach = cancel
	entry.activationID = hdr.ActivationID
	entry.heldSince = time.Time{}
	entry.wakeOnData = hdr.WakeOnData
	entry.mu.Unlock()
	entry.stack.Unquiesce()
	a.metrics.addTunnels(ctx, 1)

	slog.InfoContext(ctx, "anchor: actor attached",
		slog.Any("actor", hdr.Ref()),
		slog.String("actorUID", hdr.ActorUID),
		slog.String("worker", hdr.WorkerPodUID),
		slog.String("activation", hdr.ActivationID),
		slog.String("boot", string(hdr.Boot)),
		slog.String("egressGateway", hdr.EgressGateway),
		slog.Any("neighbors", summarizeNeighbors(entry.stack.Neighbors())))
	entry.stack.ForgetNeighbors()

	if hdr.EgressGateway != "" {
		if entry.egress == nil {
			slog.WarnContext(ctx, "anchor: actor asked for egress but this anchor has no certificate broker; the actor has no egress",
				slog.Any("actor", hdr.Ref()))
		} else if err := a.activateEgress(ctx, entry, hdr.EgressGateway); err != nil {
			// Fail closed, as a worker does: no sandbox traffic without the
			// actor's egress identity. The shuttle redials with backoff.
			slog.ErrorContext(ctx, "anchor: egress activation failed, closing the tunnel", slog.Any("actor", hdr.Ref()), slog.Any("err", err))
			cancel()
		}
	}

	err = anchornet.BridgeFrameConn(tunnelCtx, entry.stack.Link(), fc, a.frameHook(ctx, entry))

	entry.mu.Lock()
	// Only clear the tunnel we own; a newer attach may have replaced it.
	held := entry.activationID == hdr.ActivationID
	if held {
		entry.detach = nil
		entry.heldSince = a.clock()
	}
	entry.mu.Unlock()
	if held {
		a.markHeld(entry)
	}
	cancel()
	a.metrics.addTunnels(ctx, -1)
	slog.InfoContext(ctx, "anchor: actor detached, connections held",
		slog.Any("actor", hdr.Ref()),
		slog.String("activation", hdr.ActivationID),
		slog.Int64("framesToActor", entry.framesToActor.Load()),
		slog.Int64("framesFromActor", entry.framesFromActor.Load()),
		slog.Any("neighbors", summarizeNeighbors(entry.stack.Neighbors())),
		slog.Any("err", err))
}

// frameHook counts the frames a tunnel moves and logs them when asked.
func (a *Anchor) frameHook(ctx context.Context, entry *actorEntry) anchornet.FrameHook {
	return func(outbound bool, frame []byte) {
		direction := directionFromActor
		if outbound {
			direction = directionToActor
			entry.framesToActor.Add(1)
		} else {
			entry.framesFromActor.Add(1)
		}
		a.metrics.recordFrame(ctx, direction, len(frame))
		if a.cfg.LogFrames {
			slog.DebugContext(ctx, "anchor frame",
				slog.Any("actor", entry.ref),
				slog.String("direction", direction),
				slog.String("frame", summarizeFrame(frame)))
		}
	}
}

// getOrCreate returns the actor's stack, creating it on first attach. A held
// stack is reused only for a restore of the same actor UID: a fresh boot is a
// new process, and a new UID is a new actor that reused the name, so in both
// cases the held connections cannot belong to it and the stack is replaced.
func (a *Anchor) getOrCreate(ctx context.Context, ref resources.ActorRef, actorUID string, fresh bool) (*actorEntry, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if e, ok := a.actors[ref]; ok {
		if !fresh && e.actorUID == actorUID {
			return e, nil
		}
		a.dropLocked(ctx, e)
	}
	if a.cfg.Hold.MaxActors > 0 && len(a.actors) >= a.cfg.Hold.MaxActors {
		a.metrics.recordCapRejection(ctx, "actors")
		return nil, errTooManyActors
	}
	e, err := a.newEntry(ref, actorUID)
	if err != nil {
		return nil, err
	}
	a.actors[ref] = e
	a.metrics.addActors(ctx, 1)
	return e, nil
}

// dropLocked detaches, closes, and forgets an actor's stack. Closing the stack
// resets its held connections toward the router side. Caller holds a.mu.
func (a *Anchor) dropLocked(ctx context.Context, e *actorEntry) {
	e.mu.Lock()
	if e.detach != nil {
		e.detach()
	}
	e.mu.Unlock()
	e.transport.CloseIdleConnections()
	e.egress.stop(ctx)
	// Release writers held by the gate before the stack goes away.
	e.stack.Unquiesce()
	e.stack.Close()
	delete(a.actors, e.ref)
	a.metrics.addActors(ctx, -1)
}

func (a *Anchor) newEntry(ref resources.ActorRef, actorUID string) (*actorEntry, error) {
	st, err := anchornet.NewEthernetStack(actoraddr.ActorVethGateway, actoraddr.PrefixLen, actoraddr.GatewayMAC)
	if err != nil {
		return nil, err
	}
	if err := st.SetTCPMaxRetries(tcpRetriesFor(a.cfg.HoldTTL)); err != nil {
		st.Close()
		return nil, err
	}
	e := &actorEntry{ref: ref, actorUID: actorUID, stack: st}
	st.OnWriteBlocked(func() { a.wake(e) })
	e.transport = &http.Transport{
		DialContext: func(ctx context.Context, _ string, addr string) (net.Conn, error) {
			if !a.connectionAllowed(ctx) {
				return nil, errTooManyConnections
			}
			return dialInStack(ctx, st, addr)
		},
		MaxIdleConnsPerHost: 8,
		IdleConnTimeout:     90 * time.Second,
	}
	e.proxy = &httputil.ReverseProxy{
		Rewrite:      rewriteToActor,
		Transport:    e.transport,
		ErrorHandler: upstreamError,
	}
	if a.cfg.Egress.enabled() {
		if err := a.startEgress(e); err != nil {
			st.Close()
			return nil, err
		}
	}
	return e, nil
}

// rewriteToActor points the request at the actor's address inside its stack,
// on the port the router asked for.
func rewriteToActor(pr *httputil.ProxyRequest) {
	port := pr.In.Header.Get(atunnel.TargetPortHeader)
	pr.Out.Header.Del(atunnel.TargetPortHeader)
	p, ok := atunnel.ParsePort(port)
	if !ok {
		p = 80
	}
	pr.SetURL(&url.URL{Scheme: "http", Host: net.JoinHostPort(actoraddr.ActorVethIP, strconv.Itoa(p))})
	pr.Out.Host = pr.In.Host
	pr.SetXForwarded()
}

func upstreamError(w http.ResponseWriter, r *http.Request, err error) {
	if call, _ := r.Context().Value(ingressCallKey{}).(*ingressCall); call != nil {
		call.failed.Store(true)
	}
	slog.WarnContext(r.Context(), "anchor upstream request failed", slog.Any("err", err))
	http.Error(w, "bad gateway", http.StatusBadGateway)
}

// clock is the injectable time source; nil means the wall clock.
func (a *Anchor) clock() time.Time {
	if a.now == nil {
		return time.Now()
	}
	return a.now()
}

func (a *Anchor) lookup(ref resources.ActorRef) *actorEntry {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.actors[ref]
}

// sweepLoop drops stacks held longer than the hold TTL.
func (a *Anchor) sweepLoop(ctx context.Context) {
	if a.cfg.HoldTTL <= 0 {
		return
	}
	interval := min(a.cfg.HoldTTL/10, maxSweepInterval)
	if interval < time.Second {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			a.sweepHeld(ctx)
		}
	}
}

// sweepHeld drops every stack whose hold has outlived the TTL and returns how
// many it dropped.
func (a *Anchor) sweepHeld(ctx context.Context) int {
	a.mu.Lock()
	defer a.mu.Unlock()
	cutoff := a.clock().Add(-a.cfg.HoldTTL)
	dropped := 0
	for _, e := range a.actors {
		e.mu.Lock()
		expired := e.detach == nil && !e.heldSince.IsZero() && e.heldSince.Before(cutoff)
		e.mu.Unlock()
		if !expired {
			continue
		}
		slog.InfoContext(ctx, "anchor: hold expired, resetting connections",
			slog.Any("actor", e.ref), slog.String("actorUID", e.actorUID))
		a.dropLocked(ctx, e)
		a.metrics.recordHoldExpired(ctx)
		dropped++
	}
	return dropped
}

// ServeHTTP is the ingress path: the router's request for an actor is proxied
// into that actor's stack, upgrades included.
func (a *Anchor) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := a.clock()
	ref, ok := actorRefFromRequest(r)
	if !ok {
		a.reject(w, r, start)
		return
	}
	entry := a.lookup(ref)
	if entry == nil {
		a.reject(w, r, start)
		return
	}
	actorHost := r.Header.Get(atunnel.OriginalHostHeader)
	if actorHost == "" {
		actorHost = r.Host
	}
	r.Header.Del(atunnel.OriginalHostHeader)
	r.Host = actorHost
	call := &ingressCall{entry: entry}
	ctx := context.WithValue(r.Context(), ingressCallKey{}, call)
	entry.proxy.ServeHTTP(w, r.WithContext(ctx))
	outcome := outcomeProxied
	if call.failed.Load() {
		outcome = outcomeUpstreamError
	}
	a.metrics.recordIngress(r.Context(), a.clock().Sub(start), outcome)
}

// actorRefFromRequest resolves the actor a request is for, from the router's
// original-host header or the request Host.
func actorRefFromRequest(r *http.Request) (resources.ActorRef, bool) {
	actorHost := r.Header.Get(atunnel.OriginalHostHeader)
	if actorHost == "" {
		actorHost = r.Host
	}
	host := actorHost
	if strings.Contains(actorHost, ":") {
		h, _, err := net.SplitHostPort(actorHost)
		if err != nil {
			return resources.ActorRef{}, false
		}
		host = h
	}
	ref, err := resources.ParseActorDNSName(strings.ToLower(host))
	if err != nil {
		return resources.ActorRef{}, false
	}
	return ref, true
}

// reject answers a request for an actor this anchor does not hold, with the
// header the router treats as a stale assignment.
func (a *Anchor) reject(w http.ResponseWriter, r *http.Request, start time.Time) {
	w.Header().Set(atunnel.StaleAssignmentHeader, "true")
	http.Error(w, "misdirected request", http.StatusMisdirectedRequest)
	a.metrics.recordIngress(r.Context(), a.clock().Sub(start), outcomeMisdirected)
}

func dialInStack(ctx context.Context, st *anchornet.Stack, addr string) (net.Conn, error) {
	_, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	port, ok := atunnel.ParsePort(portStr)
	if !ok {
		return nil, fmt.Errorf("anchor: invalid port in %q", addr)
	}
	actorIP := net.ParseIP(actoraddr.ActorVethIP).To4()
	full := tcpip.FullAddress{Addr: tcpip.AddrFromSlice(actorIP), Port: uint16(port)}
	conn, err := gonet.DialContextTCP(ctx, st.Stack(), full, ipv4.ProtocolNumber)
	if err != nil {
		return nil, err
	}
	return st.GateWrites(conn), nil
}

// serveProbe runs one readiness GET inside an actor's stack for ateom, which
// has no address on the link once the actor is anchored.
func (a *Anchor) serveProbe(w http.ResponseWriter, r *http.Request) {
	start := a.clock()
	outcome := outcomeBadRequest
	defer func() { a.metrics.recordProbe(r.Context(), a.clock().Sub(start), outcome) }()

	q := r.URL.Query()
	ref := resources.ActorRef{Atespace: q.Get("atespace"), Name: q.Get("actor")}
	port, ok := atunnel.ParsePort(q.Get("port"))
	if !ok || !resources.IsValidResourceName(ref.Atespace) || !resources.IsValidResourceName(ref.Name) {
		http.Error(w, "bad probe request", http.StatusBadRequest)
		return
	}
	path := q.Get("path")
	if path == "" || path[0] != '/' {
		path = "/" + path
	}
	entry := a.lookup(ref)
	if entry == nil {
		outcome = outcomeNotAttached
		http.Error(w, "actor not attached", http.StatusNotFound)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), probeTimeout)
	defer cancel()
	client := &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, addr string) (net.Conn, error) {
			return dialInStack(ctx, entry.stack, addr)
		},
		DisableKeepAlives: true,
	}}
	target := "http://" + net.JoinHostPort(actoraddr.ActorVethIP, strconv.Itoa(port)) + path
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	resp, err := client.Do(req)
	if err != nil {
		outcome = outcomeFailed
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	outcome = outcomeNotReady
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		outcome = outcomeOK
	}
	w.WriteHeader(resp.StatusCode)
}
