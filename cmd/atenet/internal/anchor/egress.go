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
	"bufio"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/agent-substrate/substrate/internal/anchornet"
	"github.com/agent-substrate/substrate/internal/atunnel"
)

// Ports inside every actor stack that RedirectEgress sends outbound traffic to.
const (
	egressTCPPort = 15001
	egressDNSPort = 15053
	// egressStopTimeout bounds how long dropping an actor waits for its
	// egress streams to end.
	egressStopTimeout = 5 * time.Second
)

// EgressConfig is what the anchor needs to open actors' outbound connections
// through the egress gateway with each actor's own certificate.
type EgressConfig struct {
	// BrokerSocketPath is the node-local atelet credential broker that mints
	// actor certificates. Empty disables egress for every actor.
	BrokerSocketPath string
	// GatewayTrustBundlePath verifies the egress gateway's serving certificate.
	GatewayTrustBundlePath string
	// DNSUpstream is the resolver (host:port) actor DNS queries are forwarded
	// to. Empty uses the first nameserver in /etc/resolv.conf.
	DNSUpstream string
}

func (c EgressConfig) enabled() bool { return c.BrokerSocketPath != "" }

// actorEgress is the atunnel egress proxy relocated from the worker into the
// anchor, one per actor stack. It exists from the stack's creation so held
// egress connections keep their gateway leg; it carries traffic only after
// activateEgress minted the actor's certificate.
type actorEgress struct {
	proxy  *atunnel.Egress
	cancel context.CancelFunc

	mu      sync.Mutex
	gateway string
}

// startEgress installs the redirect in a new actor stack and starts the proxy
// and the DNS forwarder behind it.
func (a *Anchor) startEgress(entry *actorEntry) error {
	entry.stack.RedirectEgress(egressTCPPort, egressDNSPort)
	listener, err := entry.stack.ListenRedirectedTCP(egressTCPPort)
	if err != nil {
		return err
	}
	proxy, err := atunnel.NewEgress(anchornet.OriginalDestination)
	if err != nil {
		listener.Close()
		return err
	}
	ctx, cancel := context.WithCancel(context.Background())
	entry.egress = &actorEgress{proxy: proxy, cancel: cancel}
	counted := &countingListener{Listener: listener, onAccept: func() { a.metrics.recordEgressConnection(ctx) }}
	go func() {
		if err := proxy.Serve(ctx, counted); err != nil {
			slog.WarnContext(ctx, "anchor: egress proxy stopped", slog.Any("actor", entry.ref), slog.Any("err", err))
		}
	}()
	go func() {
		observe := func(answered bool) { a.metrics.recordDNSQuery(ctx, answered) }
		if err := entry.stack.ServeDNS(ctx, egressDNSPort, a.dnsUpstream, observe); err != nil {
			slog.WarnContext(ctx, "anchor: DNS forwarder stopped", slog.Any("actor", entry.ref), slog.Any("err", err))
		}
	}()
	return nil
}

// activateEgress lets an actor's outbound connections through gateway with a
// certificate minted for the actor. Attaching again with the same gateway
// keeps the running activation and its renewal.
func (a *Anchor) activateEgress(ctx context.Context, entry *actorEntry, gateway string) error {
	e := entry.egress
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.gateway == gateway {
		return nil
	}
	if e.gateway != "" {
		stopCtx, cancel := context.WithTimeout(ctx, egressStopTimeout)
		_ = e.proxy.Deactivate(stopCtx)
		cancel()
		e.gateway = ""
	}
	serverName, _, err := net.SplitHostPort(gateway)
	if err != nil {
		return fmt.Errorf("anchor: invalid egress gateway %q: %w", gateway, err)
	}
	source, err := atunnel.NewBrokerCertificateSource(atunnel.BrokerConfig{
		SocketPath:           a.cfg.Egress.BrokerSocketPath,
		CredentialBundlePath: a.cfg.IngressCredentialBundlePath,
		TrustBundlePath:      a.cfg.TrustBundlePath,
		ExpectedActorUID:     entry.actorUID,
	})
	if err != nil {
		return fmt.Errorf("anchor: configuring the certificate broker: %w", err)
	}
	expiresAt, err := source.Mint(ctx)
	if err != nil {
		a.metrics.recordEgressActivation(ctx, outcomeMintFailed)
		return fmt.Errorf("anchor: minting the actor certificate: %w", err)
	}
	client, err := atunnel.NewClient(atunnel.ClientConfig{
		GatewayAddress:       gateway,
		ServerName:           serverName,
		GetClientCertificate: source.GetClientCertificate,
		TrustBundlePath:      a.cfg.Egress.GatewayTrustBundlePath,
	})
	if err != nil {
		a.metrics.recordEgressActivation(ctx, outcomeClientFailed)
		return fmt.Errorf("anchor: configuring the egress gateway client: %w", err)
	}
	if err := e.proxy.Activate(client, source, expiresAt); err != nil {
		a.metrics.recordEgressActivation(ctx, outcomeClientFailed)
		return fmt.Errorf("anchor: activating egress: %w", err)
	}
	e.gateway = gateway
	a.metrics.recordEgressActivation(ctx, outcomeActivated)
	return nil
}

// stop ends the proxy and the DNS forwarder. Held egress connections are
// reset, which is what dropping the actor's stack means.
func (e *actorEgress) stop(ctx context.Context) {
	if e == nil {
		return
	}
	stopCtx, cancel := context.WithTimeout(ctx, egressStopTimeout)
	defer cancel()
	_ = e.proxy.Deactivate(stopCtx)
	e.cancel()
}

// countingListener counts accepted connections for the metrics.
type countingListener struct {
	net.Listener
	onAccept func()
}

func (l *countingListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err == nil {
		l.onAccept()
	}
	return conn, err
}

// defaultDNSUpstream is the first nameserver in resolvConf, on port 53.
func defaultDNSUpstream(resolvConf string) (string, error) {
	f, err := os.Open(resolvConf)
	if err != nil {
		return "", err
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) >= 2 && fields[0] == "nameserver" {
			return net.JoinHostPort(fields[1], "53"), nil
		}
	}
	if err := scanner.Err(); err != nil {
		return "", err
	}
	return "", errors.New("anchor: no nameserver in " + resolvConf)
}
