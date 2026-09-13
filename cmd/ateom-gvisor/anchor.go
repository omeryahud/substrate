//go:build linux

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
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/agent-substrate/substrate/internal/anchortun"
	"github.com/agent-substrate/substrate/internal/ateomnet"
	"github.com/agent-substrate/substrate/internal/ateomnet/shuttle"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"github.com/agent-substrate/substrate/internal/readyz"
)

// attachAnchor starts the frame shuttle for an anchored activation and returns
// the function that stops it. It runs after the actor network exists and
// before the sandbox starts, so the sandbox's first frames have a path. The
// anchor's attach and control listeners present the service-DNS certificate,
// which the worker verifies with the same trust bundle it uses for the egress
// gateway.
func (s *AteomService) attachAnchor(ctx context.Context, anchor *ateompb.ConnectionAnchor, egress *ateompb.EgressGateway, atespace, actorName, actorUID string, boot anchortun.BootKind) (func(), error) {
	host, _, err := net.SplitHostPort(anchor.GetAttachAddress())
	if err != nil {
		return nil, fmt.Errorf("invalid anchor attach address %q: %w", anchor.GetAttachAddress(), err)
	}
	// ateapi mints the activation id with the assignment so the anchor can
	// check it. An older ateapi leaves it empty; then the id only tells one
	// tunnel of this activation from another.
	activationID := anchor.GetActivationId()
	if activationID == "" {
		activationID = rand.Text()
	}
	return shuttle.Attach(ctx, shuttle.Config{
		IfaceName:            ateomnet.HostVethName,
		AttachAddress:        anchor.GetAttachAddress(),
		ServerName:           host,
		CredentialBundlePath: s.workerCredentialBundlePath,
		TrustBundlePath:      s.egressGatewayTrustBundlePath,
		Header: anchortun.AttachHeader{
			Atespace:      atespace,
			ActorName:     actorName,
			ActorUID:      actorUID,
			WorkerPodUID:  *podUID,
			ActivationID:  activationID,
			Boot:          boot,
			EgressGateway: egress.GetAddress(),
		},
	})
}

// waitReadyViaAnchor is readyz.WaitAll for an anchored actor. The worker
// kernel has no address on the actor link, so each probe is a GET the anchor
// performs inside the actor's stack on ateom's behalf.
func (s *AteomService) waitReadyViaAnchor(ctx context.Context, anchor *ateompb.ConnectionAnchor, atespace, actorName string, containers []*ateompb.Container) error {
	client, err := s.anchorControlClient(anchor.GetControlAddress())
	if err != nil {
		return err
	}
	defer client.CloseIdleConnections()

	g, gctx := errgroup.WithContext(ctx)
	for _, ac := range containers {
		probe := ac.GetReadyz()
		if probe == nil {
			continue
		}
		name := ac.GetName()
		g.Go(func() error {
			return waitProbeViaAnchor(gctx, client, anchor.GetControlAddress(), atespace, actorName, name, probe)
		})
	}
	return g.Wait()
}

func waitProbeViaAnchor(ctx context.Context, client *http.Client, controlAddress, atespace, actorName, containerName string, probe *ateompb.Readyz) error {
	hg := probe.GetHttpGet()
	if hg == nil {
		return fmt.Errorf("invalid readyz config for %q: httpGet is required", containerName)
	}
	path := hg.GetPath()
	if path == "" {
		path = readyz.DefaultPath
	}
	q := url.Values{}
	q.Set("atespace", atespace)
	q.Set("actor", actorName)
	q.Set("port", strconv.Itoa(int(hg.GetPort())))
	q.Set("path", path)
	probeURL := "https://" + controlAddress + "/probe?" + q.Encode()

	timeout := readyz.DefaultOverallTimeout
	if sec := probe.GetTimeoutSeconds(); sec > 0 {
		timeout = time.Duration(sec) * time.Second
	}
	start := time.Now()
	deadline := start.Add(timeout)
	attempts := 0
	var lastErr error
	for {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("readyz via anchor cancelled for %q after %s (%d attempts, last error: %v): %w",
				containerName, time.Since(start), attempts, lastErr, err)
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("readyz via anchor for %q never returned 200 within %s (%d attempts, last error: %v)",
				containerName, timeout, attempts, lastErr)
		}
		attempts++
		ok, err := probeOnce(ctx, client, probeURL)
		if err != nil {
			lastErr = err
		}
		if ok {
			slog.InfoContext(ctx, "Readyz via anchor reached 200",
				slog.String("container", containerName),
				slog.Duration("elapsed", time.Since(start)),
				slog.Int("attempts", attempts))
			return nil
		}
		select {
		case <-ctx.Done():
		case <-time.After(readyz.PollInterval):
		}
	}
}

func probeOnce(ctx context.Context, client *http.Client, probeURL string) (bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, probeURL, nil)
	if err != nil {
		return false, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return false, err
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return false, fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	return true, nil
}

// anchorControlClient dials the anchor's control listener with the worker's
// identity, verifying the anchor by its Service name.
func (s *AteomService) anchorControlClient(controlAddress string) (*http.Client, error) {
	host, _, err := net.SplitHostPort(controlAddress)
	if err != nil {
		return nil, fmt.Errorf("invalid anchor control address %q: %w", controlAddress, err)
	}
	trustPEM, err := os.ReadFile(s.egressGatewayTrustBundlePath)
	if err != nil {
		return nil, fmt.Errorf("reading anchor trust bundle: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(trustPEM) {
		return nil, fmt.Errorf("anchor trust bundle %q contains no certificates", s.egressGatewayTrustBundlePath)
	}
	credPath := s.workerCredentialBundlePath
	tlsCfg := &tls.Config{
		MinVersion: tls.VersionTLS12,
		RootCAs:    roots,
		ServerName: host,
		GetClientCertificate: func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
			pemBytes, err := os.ReadFile(credPath)
			if err != nil {
				return nil, err
			}
			cert, err := tls.X509KeyPair(pemBytes, pemBytes)
			if err != nil {
				return nil, err
			}
			return &cert, nil
		},
	}
	return &http.Client{
		Transport: &http.Transport{TLSClientConfig: tlsCfg, ResponseHeaderTimeout: readyz.RequestTimeout},
		Timeout:   readyz.RequestTimeout,
	}, nil
}
