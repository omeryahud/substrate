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

package ingress

import (
	"context"
	"sync"
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestParkingLot_CapacityAndRelease(t *testing.T) {
	lot := newParkingLot(ParkedRequestConfig{Budget: time.Second, Max: 2}, nil)
	ctx := context.Background()

	r1, ok := lot.enter(ctx)
	if !ok {
		t.Fatal("first enter should be admitted")
	}
	r2, ok := lot.enter(ctx)
	if !ok {
		t.Fatal("second enter should be admitted")
	}
	if got := lot.activeCount(); got != 2 {
		t.Fatalf("active = %d, want 2", got)
	}

	// Lot is full; the third request must be shed.
	if _, ok := lot.enter(ctx); ok {
		t.Fatal("third enter should be rejected when lot is full")
	}

	// Releasing a slot frees room for a new request.
	r1(parkOutcomeServed)
	if got := lot.activeCount(); got != 1 {
		t.Fatalf("active after release = %d, want 1", got)
	}
	r3, ok := lot.enter(ctx)
	if !ok {
		t.Fatal("enter should be admitted after a slot was released")
	}

	r2(parkOutcomeServed)
	r3(parkOutcomeServed)
	if got := lot.activeCount(); got != 0 {
		t.Fatalf("active after all released = %d, want 0", got)
	}
}

func TestParkingLot_ReleaseIsIdempotent(t *testing.T) {
	lot := newParkingLot(ParkedRequestConfig{Budget: time.Second, Max: 1}, nil)

	release, ok := lot.enter(context.Background())
	if !ok {
		t.Fatal("enter should be admitted")
	}
	release(parkOutcomeServed)
	release(parkOutcomeServed) // double release must not double-count
	if got := lot.activeCount(); got != 0 {
		t.Fatalf("active = %d, want 0 after idempotent release", got)
	}
}

func TestParkingLot_DisabledAlwaysAdmits(t *testing.T) {
	// maxParked == 0 means parking is disabled: every request is admitted
	// with no slot accounting.
	lot := newParkingLot(ParkedRequestConfig{Max: 0}, nil)

	for i := 0; i < 5; i++ {
		release, ok := lot.enter(context.Background())
		if !ok {
			t.Fatalf("disabled lot rejected request %d", i)
		}
		release(parkOutcomeServed)
	}
	if got := lot.activeCount(); got != 0 {
		t.Fatalf("disabled lot should not account slots, active = %d", got)
	}
	if s := lot.status(); s.Enabled {
		t.Fatalf("maxParked=0 must report parking disabled, got %+v", s)
	}
}

func TestParkingLot_ConcurrentEntryRespectsCapacity(t *testing.T) {
	const capacity = 8
	const goroutines = 100
	lot := newParkingLot(ParkedRequestConfig{Budget: time.Second, Max: capacity}, nil)

	var admitted int64
	var mu sync.Mutex
	releases := make([]func(parkOutcome), 0, capacity)

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			if release, ok := lot.enter(context.Background()); ok {
				mu.Lock()
				admitted++
				releases = append(releases, release)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if admitted != capacity {
		t.Fatalf("admitted = %d, want exactly %d", admitted, capacity)
	}
	if got := lot.activeCount(); got != capacity {
		t.Fatalf("active = %d, want %d", got, capacity)
	}
	for _, r := range releases {
		r(parkOutcomeServed)
	}
	if got := lot.activeCount(); got != 0 {
		t.Fatalf("active after releasing all = %d, want 0", got)
	}
}

func TestParkOutcomeFor(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want parkOutcome
	}{
		{"nil is served", nil, parkOutcomeServed},
		{"budget exhaustion is explicit", &budgetExhaustedError{lastErr: errOther}, parkOutcomeBudgetExhausted},
		{"canceled", context.Canceled, parkOutcomeCanceled},
		{"deadline is timeout", context.DeadlineExceeded, parkOutcomeTimeout},
		{"other is error", errOther, parkOutcomeError},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := parkOutcomeFor(tc.err); got != tc.want {
				t.Errorf("parkOutcomeFor(%v) = %q, want %q", tc.err, got, tc.want)
			}
		})
	}
}

type simpleErr string

func (e simpleErr) Error() string { return string(e) }

const errOther simpleErr = "boom"

// The router's hold and Envoy's patience must never drift apart: a message
// timeout at or below the worst-case hold silently replaces the router's
// meaningful verdict (200 or a capacity 503) with a generic Envoy gateway
// error, which is exactly the failure mode the derivation exists to prevent.
func TestExtProcMessageTimeoutCoversMaxResumeHold(t *testing.T) {
	cfgs := []ParkedRequestConfig{
		{Budget: 0, Max: DefaultParkedRequestMax},
		{Budget: DefaultParkedRequestBudget, Max: DefaultParkedRequestMax},
		{Budget: time.Second, Max: DefaultParkedRequestMax},
		{Budget: 30 * time.Second, Max: DefaultParkedRequestMax},
		{Max: 0}, // parking disabled: the fail-fast hold still needs covering
	}
	for _, cfg := range cfgs {
		hold, envoy := cfg.MaxResumeHold(), ExtProcMessageTimeoutFor(cfg)
		if envoy <= hold {
			t.Errorf("cfg %+v: ext_proc timeout %v must exceed the worst-case hold %v", cfg, envoy, hold)
		}
	}
}

func TestMaxResumeHold(t *testing.T) {
	// The worst-case hold is the mode's retry budget plus the committed-attempt
	// wait; a non-positive budget normalizes to the default first, and disabled
	// parking holds against the fail-fast budget instead. At the defaults the
	// deployed Envoy timeout works out to the historical 10s — the derivation
	// changed shape without moving the deployed value.
	if got, want := (ParkedRequestConfig{Budget: 5 * time.Second, Max: 1}).MaxResumeHold(), 5*time.Second+CommittedAttemptWait; got != want {
		t.Errorf("MaxResumeHold() = %v, want %v", got, want)
	}
	if got, want := (ParkedRequestConfig{Max: 1}).MaxResumeHold(), DefaultParkedRequestBudget+CommittedAttemptWait; got != want {
		t.Errorf("MaxResumeHold() with zero budget = %v, want normalized %v", got, want)
	}
	if got, want := (ParkedRequestConfig{Max: 0}).MaxResumeHold(), failFastResumeBudget+CommittedAttemptWait; got != want {
		t.Errorf("MaxResumeHold() with parking disabled = %v, want fail-fast %v", got, want)
	}
	if got, want := ExtProcMessageTimeoutFor(ParkedRequestConfig{Max: DefaultParkedRequestMax}), 10*time.Second; got != want {
		t.Errorf("ExtProcMessageTimeoutFor(defaults) = %v, want the deployed %v", got, want)
	}
}

// The /statusz card must state the true worst case a caller can be held —
// the retry budget + committed-attempt wait — not just the budget;
// understating it misleads whoever is debugging a slow request.
func TestParkingLotStatusReportsWorstCaseHold(t *testing.T) {
	lot := newParkingLot(ParkedRequestConfig{Budget: 5 * time.Second, Max: 4}, nil)
	release, ok := lot.enter(t.Context())
	if !ok {
		t.Fatal("enter into an empty lot must succeed")
	}
	got := lot.status()
	want := ParkingStatus{Enabled: true, Active: 1, MaxParked: 4, MaxWait: "8s"}
	if got != want {
		t.Errorf("status() = %+v, want %+v", got, want)
	}
	release(parkOutcomeServed)
	if got := lot.status().Active; got != 0 {
		t.Errorf("Active after release = %d, want 0", got)
	}
}

func TestNewParkingMetrics(t *testing.T) {
	m, err := NewParkingMetrics()
	if err != nil {
		t.Fatalf("NewParkingMetrics() failed: %v", err)
	}
	if m.active == nil || m.wait == nil || m.rejected == nil || m.resumeDetached == nil {
		t.Errorf("NewParkingMetrics() left an instrument nil: %+v", m)
	}
	// The nil-receiver contract: every method is a safe no-op.
	var nilMetrics *ParkingMetrics
	nilMetrics.addActive(t.Context(), 1)
	nilMetrics.recordWait(t.Context(), time.Second, parkOutcomeServed)
	nilMetrics.recordRejected(t.Context())
	nilMetrics.recordResumeDetached(t.Context(), true)
}

func TestParkedRequestConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     ParkedRequestConfig
		wantErr bool
	}{
		{"defaults valid", DefaultParkedRequestConfig(), false},
		{"factor below one rejected", ParkedRequestConfig{RetryFactor: 0.5}, true},
		{"negative jitter rejected", ParkedRequestConfig{RetryJitter: -0.1}, true},
		{"jitter of one rejected", ParkedRequestConfig{RetryJitter: 1}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.cfg.Validate(); (err != nil) != tc.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

// TestParkingLotServedLateReachesHistogram pins the served_late relabel end
// to end: a release after the budget must land on the wait-duration histogram
// with outcome=served_late, not the raw served the caller passed in. The
// docs single this label out as an operator-visible split, so the label
// actually reaching the instrument deserves the same end-to-end pinning the
// detached counter has.
func TestParkingLotServedLateReachesHistogram(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	meter := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test")
	wait, err := meter.Float64Histogram(parkingWaitMetricName)
	if err != nil {
		t.Fatalf("creating test histogram: %v", err)
	}
	metrics := &ParkingMetrics{wait: wait}

	// A microscopic budget so a real (non-fake-time) sleep lands past it.
	lot := newParkingLot(ParkedRequestConfig{Budget: time.Millisecond, Max: 1}, metrics)
	release, ok := lot.enter(t.Context())
	if !ok {
		t.Fatal("enter into an empty lot must succeed")
	}
	time.Sleep(5 * time.Millisecond)
	release(parkOutcomeServed)

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(t.Context(), &rm); err != nil {
		t.Fatalf("collecting metrics: %v", err)
	}
	var points []metricdata.HistogramDataPoint[float64]
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != parkingWaitMetricName {
				continue
			}
			hist, ok := m.Data.(metricdata.Histogram[float64])
			if !ok {
				t.Fatalf("unexpected data type %T", m.Data)
			}
			points = hist.DataPoints
		}
	}
	if len(points) != 1 {
		t.Fatalf("wait histogram datapoints = %d, want 1", len(points))
	}
	got, ok := points[0].Attributes.Value("outcome")
	if !ok || got.AsString() != string(parkOutcomeServedLate) {
		t.Errorf("outcome attribute = %v (present=%v), want %q for a past-budget serve", got, ok, parkOutcomeServedLate)
	}
}

func TestParkingLotFinalOutcome(t *testing.T) {
	lot := newParkingLot(ParkedRequestConfig{Budget: 5 * time.Second, Max: 1}, nil)
	tests := []struct {
		name    string
		outcome parkOutcome
		waited  time.Duration
		want    parkOutcome
	}{
		{"served inside the budget stays served", parkOutcomeServed, 3 * time.Second, parkOutcomeServed},
		{"served past the budget becomes served_late", parkOutcomeServed, 6 * time.Second, parkOutcomeServedLate},
		{"exhaustion is never relabeled", parkOutcomeBudgetExhausted, 8 * time.Second, parkOutcomeBudgetExhausted},
		{"cancel past the budget is not a late serve", parkOutcomeCanceled, 6 * time.Second, parkOutcomeCanceled},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := lot.finalOutcome(tc.outcome, tc.waited); got != tc.want {
				t.Errorf("finalOutcome(%q, %v) = %q, want %q", tc.outcome, tc.waited, got, tc.want)
			}
		})
	}
}
