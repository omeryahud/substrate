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
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc"
)

// BenchmarkResumeActorWarmPath measures the resumer's per-request overhead on
// the path every ingress request takes — an actor that is already RUNNING, so
// the control-plane call returns immediately. The parking machinery (flight
// setup, caller wait bound) must stay negligible against the real work.
func BenchmarkResumeActorWarmPath(b *testing.B) {
	resp := &ateapipb.ResumeActorResponse{
		Actor: &ateapipb.Actor{
			Metadata:         &ateapipb.ResourceMetadata{Name: "bench"},
			Status:           ateapipb.Actor_STATUS_RUNNING,
			WorkerAssignment: &ateapipb.WorkerAssignment{WorkerPodIp: "10.0.0.1"},
		},
	}
	mock := &resumerMockClient{
		resumeFn: func(ctx context.Context, in *ateapipb.ResumeActorRequest, opts ...grpc.CallOption) (*ateapipb.ResumeActorResponse, error) {
			return resp, nil
		},
	}
	resumer := NewActorResumer(mock, withParking(ParkedRequestConfig{Max: 1024, Budget: 5 * time.Second}))
	ref := resources.ActorRef{Atespace: "bench", Name: "bench"}
	ctx := context.Background()

	b.ReportAllocs()
	for b.Loop() {
		if _, _, err := resumer.ResumeActor(ctx, ref); err != nil {
			b.Fatal(err)
		}
	}
}
