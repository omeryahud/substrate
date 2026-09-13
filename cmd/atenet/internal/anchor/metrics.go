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
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"

	"github.com/agent-substrate/substrate/internal/ateattr"
)

// ServiceName is the anchor's OpenTelemetry service and meter name.
const ServiceName = "atenet-anchor"

const (
	attachMetricName          = "atenet.anchor.attach"
	tunnelsActiveMetricName   = "atenet.anchor.tunnels.active"
	actorsMetricName          = "atenet.anchor.actors"
	holdsExpiredMetricName    = "atenet.anchor.holds.expired"
	framesMetricName          = "atenet.anchor.frames"
	frameBytesMetricName      = "atenet.anchor.frame.bytes"
	ingressRequestsMetricName = "atenet.anchor.ingress.requests"
	ingressDurationMetricName = "atenet.anchor.ingress.duration"
	probesMetricName          = "atenet.anchor.probes"
	probeDurationMetricName   = "atenet.anchor.probe.duration"
	egressActivationsName     = "atenet.anchor.egress.activations"
	egressConnectionsName     = "atenet.anchor.egress.connections"
	dnsQueriesMetricName      = "atenet.anchor.dns.queries"
)

// Outcomes for the egress activation instrument.
const (
	outcomeActivated    = "activated"
	outcomeMintFailed   = "mint_failed"
	outcomeClientFailed = "client_failed"
)

// Outcomes for the attach, ingress, and probe instruments.
const (
	outcomeAttached      = "attached"
	outcomeBadHeader     = "bad_header"
	outcomeStackError    = "stack_error"
	outcomeProxied       = "proxied"
	outcomeMisdirected   = "misdirected"
	outcomeUpstreamError = "upstream_error"
	outcomeOK            = "ok"
	outcomeNotReady      = "not_ready"
	outcomeFailed        = "failed"
	outcomeNotAttached   = "not_attached"
	outcomeBadRequest    = "bad_request"
)

// Directions for the frame instruments, seen from the anchor.
const (
	directionToActor   = "to_actor"
	directionFromActor = "from_actor"
)

// Metrics bundles the anchor's instruments. A nil *Metrics is safe to use:
// every method is a no-op, which keeps tests simple.
type Metrics struct {
	attach            metric.Int64Counter
	tunnelsActive     metric.Int64UpDownCounter
	actors            metric.Int64UpDownCounter
	holdsExpired      metric.Int64Counter
	frames            metric.Int64Counter
	frameBytes        metric.Int64Counter
	ingressRequests   metric.Int64Counter
	ingressDuration   metric.Float64Histogram
	probes            metric.Int64Counter
	probeDuration     metric.Float64Histogram
	egressActivations metric.Int64Counter
	egressConnections metric.Int64Counter
	dnsQueries        metric.Int64Counter
}

// NewMetrics creates the anchor's instruments from the global MeterProvider.
func NewMetrics() (*Metrics, error) {
	meter := otel.Meter(ServiceName)
	m := &Metrics{}
	var err error

	if m.attach, err = meter.Int64Counter(attachMetricName,
		metric.WithUnit("{attach}"),
		metric.WithDescription("worker tunnel attaches, by outcome and boot kind")); err != nil {
		return nil, fmt.Errorf("create %s: %w", attachMetricName, err)
	}
	if m.tunnelsActive, err = meter.Int64UpDownCounter(tunnelsActiveMetricName,
		metric.WithUnit("{tunnel}"),
		metric.WithDescription("worker tunnels currently attached")); err != nil {
		return nil, fmt.Errorf("create %s: %w", tunnelsActiveMetricName, err)
	}
	if m.actors, err = meter.Int64UpDownCounter(actorsMetricName,
		metric.WithUnit("{actor}"),
		metric.WithDescription("actors with a network stack in this anchor, attached or held")); err != nil {
		return nil, fmt.Errorf("create %s: %w", actorsMetricName, err)
	}
	if m.holdsExpired, err = meter.Int64Counter(holdsExpiredMetricName,
		metric.WithUnit("{actor}"),
		metric.WithDescription("held actor stacks dropped because no worker reattached within the hold TTL")); err != nil {
		return nil, fmt.Errorf("create %s: %w", holdsExpiredMetricName, err)
	}
	if m.frames, err = meter.Int64Counter(framesMetricName,
		metric.WithUnit("{frame}"),
		metric.WithDescription("Ethernet frames moved between actor stacks and worker tunnels, by direction")); err != nil {
		return nil, fmt.Errorf("create %s: %w", framesMetricName, err)
	}
	if m.frameBytes, err = meter.Int64Counter(frameBytesMetricName,
		metric.WithUnit("By"),
		metric.WithDescription("bytes of Ethernet frames moved between actor stacks and worker tunnels, by direction")); err != nil {
		return nil, fmt.Errorf("create %s: %w", frameBytesMetricName, err)
	}
	if m.ingressRequests, err = meter.Int64Counter(ingressRequestsMetricName,
		metric.WithUnit("{request}"),
		metric.WithDescription("ingress requests from the router, by outcome")); err != nil {
		return nil, fmt.Errorf("create %s: %w", ingressRequestsMetricName, err)
	}
	if m.ingressDuration, err = meter.Float64Histogram(ingressDurationMetricName,
		metric.WithUnit("s"),
		metric.WithDescription("time to serve an ingress request through the actor's stack, by outcome"),
		metric.WithExplicitBucketBoundaries(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60)); err != nil {
		return nil, fmt.Errorf("create %s: %w", ingressDurationMetricName, err)
	}
	if m.probes, err = meter.Int64Counter(probesMetricName,
		metric.WithUnit("{probe}"),
		metric.WithDescription("readiness probes run inside actor stacks for workers, by outcome")); err != nil {
		return nil, fmt.Errorf("create %s: %w", probesMetricName, err)
	}
	if m.probeDuration, err = meter.Float64Histogram(probeDurationMetricName,
		metric.WithUnit("s"),
		metric.WithDescription("time to run a readiness probe inside an actor's stack, by outcome"),
		metric.WithExplicitBucketBoundaries(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5)); err != nil {
		return nil, fmt.Errorf("create %s: %w", probeDurationMetricName, err)
	}
	if m.egressActivations, err = meter.Int64Counter(egressActivationsName,
		metric.WithUnit("{activation}"),
		metric.WithDescription("attempts to open an actor's egress through the gateway, by outcome")); err != nil {
		return nil, fmt.Errorf("create %s: %w", egressActivationsName, err)
	}
	if m.egressConnections, err = meter.Int64Counter(egressConnectionsName,
		metric.WithUnit("{connection}"),
		metric.WithDescription("outbound TCP connections actors opened through the anchor")); err != nil {
		return nil, fmt.Errorf("create %s: %w", egressConnectionsName, err)
	}
	if m.dnsQueries, err = meter.Int64Counter(dnsQueriesMetricName,
		metric.WithUnit("{query}"),
		metric.WithDescription("actor DNS queries forwarded by the anchor, by outcome")); err != nil {
		return nil, fmt.Errorf("create %s: %w", dnsQueriesMetricName, err)
	}
	return m, nil
}

func (m *Metrics) recordEgressActivation(ctx context.Context, outcome string) {
	if m == nil {
		return
	}
	m.egressActivations.Add(ctx, 1, metric.WithAttributes(ateattr.AnchorOutcomeKey.String(outcome)))
}

func (m *Metrics) recordEgressConnection(ctx context.Context) {
	if m == nil {
		return
	}
	m.egressConnections.Add(ctx, 1)
}

func (m *Metrics) recordDNSQuery(ctx context.Context, answered bool) {
	if m == nil {
		return
	}
	outcome := outcomeOK
	if !answered {
		outcome = outcomeFailed
	}
	m.dnsQueries.Add(ctx, 1, metric.WithAttributes(ateattr.AnchorOutcomeKey.String(outcome)))
}

func (m *Metrics) recordAttach(ctx context.Context, outcome, boot string) {
	if m == nil {
		return
	}
	m.attach.Add(ctx, 1, metric.WithAttributes(
		ateattr.AnchorOutcomeKey.String(outcome),
		ateattr.AnchorBootKey.String(boot)))
}

func (m *Metrics) addTunnels(ctx context.Context, delta int64) {
	if m == nil {
		return
	}
	m.tunnelsActive.Add(ctx, delta)
}

func (m *Metrics) addActors(ctx context.Context, delta int64) {
	if m == nil {
		return
	}
	m.actors.Add(ctx, delta)
}

func (m *Metrics) recordHoldExpired(ctx context.Context) {
	if m == nil {
		return
	}
	m.holdsExpired.Add(ctx, 1)
}

func (m *Metrics) recordFrame(ctx context.Context, direction string, size int) {
	if m == nil {
		return
	}
	attrs := metric.WithAttributes(ateattr.AnchorDirectionKey.String(direction))
	m.frames.Add(ctx, 1, attrs)
	m.frameBytes.Add(ctx, int64(size), attrs)
}

func (m *Metrics) recordIngress(ctx context.Context, d time.Duration, outcome string) {
	if m == nil {
		return
	}
	attrs := metric.WithAttributes(ateattr.AnchorOutcomeKey.String(outcome))
	m.ingressRequests.Add(ctx, 1, attrs)
	m.ingressDuration.Record(ctx, d.Seconds(), attrs)
}

func (m *Metrics) recordProbe(ctx context.Context, d time.Duration, outcome string) {
	if m == nil {
		return
	}
	attrs := metric.WithAttributes(ateattr.AnchorOutcomeKey.String(outcome))
	m.probes.Add(ctx, 1, attrs)
	m.probeDuration.Record(ctx, d.Seconds(), attrs)
}
