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
	"time"

	"gvisor.dev/gvisor/pkg/tcpip"
	"gvisor.dev/gvisor/pkg/tcpip/adapters/gonet"
	"gvisor.dev/gvisor/pkg/tcpip/network/ipv4"

	"github.com/agent-substrate/substrate/internal/anchornet"
	"github.com/agent-substrate/substrate/internal/anchortun"
	"github.com/agent-substrate/substrate/internal/ateomnet/actoraddr"
	"github.com/agent-substrate/substrate/internal/atunnel"
	"github.com/agent-substrate/substrate/internal/resources"
)

const (
	// spiffePrefix is the trust domain every workload identity lives under.
	spiffePrefix = "spiffe://"
	// probeTimeout bounds one readiness probe through the actor's stack.
	probeTimeout = 5 * time.Second
)

// Config configures an Anchor.
type Config struct {
	IngressListen string
	AttachListen  string
	ControlListen string
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
}

// Anchor holds one network stack per actor and the listeners that reach them.
type Anchor struct {
	cfg       Config
	clientCAs *x509.CertPool
	proxy     *httputil.ReverseProxy

	mu     sync.Mutex
	actors map[resources.ActorRef]*actorEntry
}

// actorEntry is one actor's stack plus its current tunnel, if attached.
type actorEntry struct {
	ref resources.ActorRef

	mu           sync.Mutex
	stack        *anchornet.Stack
	detach       context.CancelFunc
	activationID string
}

type entryKey struct{}

// New validates the TLS material and prepares the ingress proxy.
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

	a := &Anchor{cfg: cfg, clientCAs: pool, actors: map[resources.ActorRef]*actorEntry{}}
	a.proxy = &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			port := pr.In.Header.Get(atunnel.TargetPortHeader)
			pr.Out.Header.Del(atunnel.TargetPortHeader)
			p, ok := atunnel.ParsePort(port)
			if !ok {
				p = 80
			}
			pr.SetURL(&url.URL{Scheme: "http", Host: net.JoinHostPort(actoraddr.ActorVethIP, strconv.Itoa(p))})
			pr.Out.Host = pr.In.Host
			pr.SetXForwarded()
		},
		Transport: &http.Transport{
			DialContext:         a.dialActor,
			MaxIdleConnsPerHost: 8,
			IdleConnTimeout:     90 * time.Second,
		},
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			slog.WarnContext(r.Context(), "anchor upstream request failed", slog.Any("err", err))
			http.Error(w, "bad gateway", http.StatusBadGateway)
		},
	}
	return a, nil
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

// Run serves the three listeners until ctx is canceled.
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

	ingressSrv := &http.Server{Handler: a, TLSConfig: a.tlsConfig(a.cfg.IngressCredentialBundlePath, a.verifyRouter), ReadHeaderTimeout: 10 * time.Second}
	controlMux := http.NewServeMux()
	controlMux.HandleFunc("/probe", a.serveProbe)
	controlSrv := &http.Server{Handler: controlMux, TLSConfig: a.tlsConfig(a.cfg.CredentialBundlePath, verifyWorkload), ReadHeaderTimeout: 10 * time.Second}
	attachTLS := tls.NewListener(attachLis, a.tlsConfig(a.cfg.CredentialBundlePath, verifyWorkload))

	errc := make(chan error, 3)
	go func() { errc <- ingressSrv.ServeTLS(ingressLis, "", "") }()
	go func() { errc <- controlSrv.ServeTLS(controlLis, "", "") }()
	go func() { errc <- a.serveAttach(ctx, attachTLS) }()
	slog.InfoContext(ctx, "anchor serving",
		slog.String("ingress", a.cfg.IngressListen),
		slog.String("attach", a.cfg.AttachListen),
		slog.String("control", a.cfg.ControlListen))

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
	return err
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

	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	first, err := fc.ReadFrame()
	if err != nil {
		slog.WarnContext(ctx, "anchor attach: reading header", slog.Any("err", err))
		return
	}
	hdr, err := anchortun.UnmarshalAttachHeader(first)
	if err != nil {
		slog.WarnContext(ctx, "anchor attach: bad header", slog.Any("err", err))
		return
	}
	_ = conn.SetReadDeadline(time.Time{})

	entry, err := a.getOrCreate(hdr.Ref(), hdr.Boot == anchortun.BootFresh)
	if err != nil {
		slog.ErrorContext(ctx, "anchor attach: creating stack", slog.Any("actor", hdr.Ref()), slog.Any("err", err))
		return
	}

	tunnelCtx, cancel := context.WithCancel(ctx)
	entry.mu.Lock()
	if entry.detach != nil {
		entry.detach()
	}
	entry.detach = cancel
	entry.activationID = hdr.ActivationID
	entry.mu.Unlock()

	slog.InfoContext(ctx, "anchor: actor attached",
		slog.Any("actor", hdr.Ref()),
		slog.String("worker", hdr.WorkerPodUID),
		slog.String("activation", hdr.ActivationID),
		slog.String("boot", string(hdr.Boot)))

	err = anchornet.BridgeFrameConn(tunnelCtx, entry.stack.Link(), fc)

	entry.mu.Lock()
	// Only clear the tunnel we own; a newer attach may have replaced it.
	if entry.activationID == hdr.ActivationID {
		entry.detach = nil
	}
	entry.mu.Unlock()
	cancel()
	slog.InfoContext(ctx, "anchor: actor detached, connections held",
		slog.Any("actor", hdr.Ref()), slog.Any("err", err))
}

// getOrCreate returns the actor's stack, creating it on first attach. A fresh
// boot discards a held stack, because held connections cannot belong to a new
// process.
func (a *Anchor) getOrCreate(ref resources.ActorRef, fresh bool) (*actorEntry, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if e, ok := a.actors[ref]; ok {
		if !fresh {
			return e, nil
		}
		e.mu.Lock()
		if e.detach != nil {
			e.detach()
		}
		e.mu.Unlock()
		e.stack.Close()
		delete(a.actors, ref)
	}
	st, err := anchornet.NewEthernetStack(actoraddr.ActorVethGateway, actoraddr.PrefixLen, actoraddr.GatewayMAC)
	if err != nil {
		return nil, err
	}
	e := &actorEntry{ref: ref, stack: st}
	a.actors[ref] = e
	return e, nil
}

func (a *Anchor) lookup(ref resources.ActorRef) *actorEntry {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.actors[ref]
}

// ServeHTTP is the ingress path: the router's request for an actor is proxied
// into that actor's stack, upgrades included.
func (a *Anchor) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ref, ok := actorRefFromRequest(r)
	if !ok {
		reject(w)
		return
	}
	entry := a.lookup(ref)
	if entry == nil {
		reject(w)
		return
	}
	actorHost := r.Header.Get(atunnel.OriginalHostHeader)
	if actorHost == "" {
		actorHost = r.Host
	}
	r.Header.Del(atunnel.OriginalHostHeader)
	r.Host = actorHost
	ctx := context.WithValue(r.Context(), entryKey{}, entry)
	a.proxy.ServeHTTP(w, r.WithContext(ctx))
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

func reject(w http.ResponseWriter) {
	w.Header().Set(atunnel.StaleAssignmentHeader, "true")
	http.Error(w, "misdirected request", http.StatusMisdirectedRequest)
}

// dialActor opens a TCP connection to the actor inside its own stack.
func (a *Anchor) dialActor(ctx context.Context, _ string, addr string) (net.Conn, error) {
	entry, _ := ctx.Value(entryKey{}).(*actorEntry)
	if entry == nil {
		return nil, errors.New("anchor: no actor for this request")
	}
	return dialInStack(ctx, entry.stack, addr)
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
	return gonet.DialContextTCP(ctx, st.Stack(), full, ipv4.ProtocolNumber)
}

// serveProbe runs one readiness GET inside an actor's stack for ateom, which
// has no address on the link once the actor is anchored.
func (a *Anchor) serveProbe(w http.ResponseWriter, r *http.Request) {
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
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	w.WriteHeader(resp.StatusCode)
}
