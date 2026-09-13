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

	"github.com/agent-substrate/substrate/internal/anchortun"
	"github.com/agent-substrate/substrate/internal/ateomnet/anchorclient"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"github.com/agent-substrate/substrate/internal/resources"
)

func (s *AteomService) anchorCredentials() anchorclient.Credentials {
	return anchorclient.Credentials{
		WorkerPodUID:         s.podUID,
		CredentialBundlePath: s.workerCredentialBundlePath,
		TrustBundlePath:      s.egressGatewayTrustBundlePath,
	}
}

// attachAnchor starts the frame shuttle for an anchored activation and returns
// the function that stops it. The guest's frames reach the anchor exactly as a
// gVisor sandbox's do: the tap is mirrored onto the same veth.
func (s *AteomService) attachAnchor(ctx context.Context, anchor *ateompb.ConnectionAnchor, egress *ateompb.EgressGateway, ref resources.ActorRef, actorUID string, boot anchortun.BootKind) (func(), error) {
	return anchorclient.Attach(ctx, s.anchorCredentials(), anchor, egress, ref, actorUID, boot)
}

// waitReadyViaAnchor is readyz.WaitAll for an anchored actor.
func (s *AteomService) waitReadyViaAnchor(ctx context.Context, anchor *ateompb.ConnectionAnchor, ref resources.ActorRef, containers []*ateompb.Container) error {
	return anchorclient.WaitReady(ctx, s.anchorCredentials(), anchor, ref, containers)
}
