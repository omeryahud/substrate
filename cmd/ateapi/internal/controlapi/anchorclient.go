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

package controlapi

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"

	"github.com/agent-substrate/substrate/internal/credbundle"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// quiesceTimeout bounds the anchor's drain toward the actor before a
// checkpoint. A timeout never fails the suspend: a few segments in flight
// only mean a slower first exchange after resume.
const quiesceTimeout = 250 * time.Millisecond

// anchorControl calls the connection anchor's control listener. It is nil
// when anchoring is off or ateapi has no credentials for it.
type anchorControl struct {
	address string
	client  *http.Client
}

func newAnchorControl(cfg AnchorConfig) (*anchorControl, error) {
	if !cfg.enabled() || cfg.ClientCredBundlePath == "" || cfg.TrustBundlePath == "" {
		return nil, nil
	}
	host, _, err := net.SplitHostPort(cfg.ControlAddress)
	if err != nil {
		return nil, fmt.Errorf("invalid anchor control address %q: %w", cfg.ControlAddress, err)
	}
	trustPEM, err := os.ReadFile(cfg.TrustBundlePath)
	if err != nil {
		return nil, fmt.Errorf("reading anchor trust bundle: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(trustPEM) {
		return nil, fmt.Errorf("anchor trust bundle %q contains no certificates", cfg.TrustBundlePath)
	}
	tlsCfg := &tls.Config{
		MinVersion:           tls.VersionTLS12,
		RootCAs:              roots,
		ServerName:           host,
		GetClientCertificate: credbundle.ClientLoader(cfg.ClientCredBundlePath),
	}
	return &anchorControl{
		address: cfg.ControlAddress,
		client:  &http.Client{Transport: &http.Transport{TLSClientConfig: tlsCfg}, Timeout: 5 * time.Second},
	}, nil
}

type quiesceResult struct {
	Drained bool `json:"drained"`
}

// Quiesce asks the anchor to stop feeding bytes toward the actor and drain
// what is in flight, bounded by timeout. It reports whether the drain
// finished in time.
func (c *anchorControl) Quiesce(ctx context.Context, ref *ateapipb.ObjectRef, activationID string, timeout time.Duration) (bool, error) {
	q := url.Values{}
	q.Set("atespace", ref.GetAtespace())
	q.Set("actor", ref.GetName())
	q.Set("activationID", activationID)
	q.Set("timeout", strconv.FormatInt(timeout.Milliseconds(), 10)+"ms")
	body, err := c.post(ctx, "/quiesce", q)
	if err != nil {
		return false, err
	}
	var res quiesceResult
	if err := json.Unmarshal(body, &res); err != nil {
		return false, fmt.Errorf("anchor quiesce: decoding reply: %w", err)
	}
	return res.Drained, nil
}

// Release asks the anchor to drop the actor's stack and reset every
// connection it held. It succeeds when the anchor holds no such actor.
func (c *anchorControl) Release(ctx context.Context, ref *ateapipb.ObjectRef, actorUID, reason string) error {
	q := url.Values{}
	q.Set("atespace", ref.GetAtespace())
	q.Set("actor", ref.GetName())
	q.Set("actorUID", actorUID)
	q.Set("reason", reason)
	_, err := c.post(ctx, "/release", q)
	return err
}

func (c *anchorControl) post(ctx context.Context, path string, q url.Values) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://"+c.address+path+"?"+q.Encode(), nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("anchor %s: %w", path, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("anchor %s: HTTP %d: %s", path, resp.StatusCode, body)
	}
	return body, nil
}

// quiesceAnchor drains the anchor toward an anchored actor before its
// checkpoint. It never fails the caller.
func (w *ActorWorkflow) quiesceAnchor(ctx context.Context, actor *ateapipb.Actor, tmpl *atev1alpha1.ActorTemplate) {
	if w.anchorControl == nil || !w.anchored(tmpl) {
		return
	}
	ref := &ateapipb.ObjectRef{Atespace: actor.GetMetadata().GetAtespace(), Name: actor.GetMetadata().GetName()}
	qctx, cancel := context.WithTimeout(ctx, 2*quiesceTimeout)
	defer cancel()
	drained, err := w.anchorControl.Quiesce(qctx, ref, actor.GetStatus().GetWorkerAssignment().GetActivationId(), quiesceTimeout)
	if err != nil {
		slog.WarnContext(ctx, "Anchor quiesce failed; checkpointing anyway", slog.Any("actor", ref), slog.Any("err", err))
		return
	}
	if !drained {
		slog.InfoContext(ctx, "Anchor did not fully drain before checkpoint", slog.Any("actor", ref))
	}
}

// releaseAnchor drops an anchored actor's held connections. It never fails
// the caller; the anchor's hold TTL is the backstop.
func (w *ActorWorkflow) releaseAnchor(ctx context.Context, actor *ateapipb.Actor, reason string) {
	if w.anchorControl == nil {
		return
	}
	tmpl, err := w.actorTemplateLister.ActorTemplates(actor.GetActorTemplateNamespace()).Get(actor.GetActorTemplateName())
	if err != nil || !w.anchored(tmpl) {
		return
	}
	ref := &ateapipb.ObjectRef{Atespace: actor.GetMetadata().GetAtespace(), Name: actor.GetMetadata().GetName()}
	rctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := w.anchorControl.Release(rctx, ref, actor.GetMetadata().GetUid(), reason); err != nil {
		slog.WarnContext(ctx, "Anchor release failed; the hold TTL will reclaim the stack", slog.Any("actor", ref), slog.Any("err", err))
	}
}
