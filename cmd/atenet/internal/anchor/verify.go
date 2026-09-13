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
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"time"

	"google.golang.org/grpc"

	"github.com/agent-substrate/substrate/internal/anchortun"
	"github.com/agent-substrate/substrate/internal/ateapiauth"
	"github.com/agent-substrate/substrate/internal/substratex509"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// verifyTimeout bounds the control plane lookup on attach.
const verifyTimeout = 5 * time.Second

// AteapiConfig lets the anchor check an attaching worker against the
// control plane's assignment. Empty disables the check; the anchor then
// trusts any worker identity for any actor.
type AteapiConfig struct {
	Address        string
	CAFile         string
	ServerName     string
	ClientCertPath string
}

func (c AteapiConfig) enabled() bool {
	return c.Address != "" && c.CAFile != "" && c.ClientCertPath != ""
}

// assignmentReader is the part of the control API the anchor needs.
type assignmentReader interface {
	GetActor(ctx context.Context, in *ateapipb.GetActorRequest, opts ...grpc.CallOption) (*ateapipb.Actor, error)
}

func newAssignmentReader(cfg AteapiConfig) (assignmentReader, error) {
	dialOpts, err := ateapiauth.DialOptions(ateapiauth.ClientConfig{
		CAFile:           cfg.CAFile,
		ServerName:       cfg.ServerName,
		ClientCredBundle: cfg.ClientCertPath,
	})
	if err != nil {
		return nil, fmt.Errorf("anchor: ateapi dial options: %w", err)
	}
	conn, err := grpc.NewClient(cfg.Address, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("anchor: ateapi client: %w", err)
	}
	return ateapipb.NewControlClient(conn), nil
}

// Rejections verifyAttach can return, also used as attach metric outcomes.
var (
	errIdentityMismatch   = errors.New("worker identity does not match the attach header")
	errAssignmentMismatch = errors.New("attach does not match the control plane assignment")
)

// verifyAttach checks that the tunnel comes from the worker the control
// plane placed this actor incarnation on, for the activation it minted. The
// TLS peer's pod UID must be the header's, so a worker cannot claim another
// worker's placement, and the assignment must name that worker.
func (a *Anchor) verifyAttach(ctx context.Context, hdr anchortun.AttachHeader, peerPodUID string) error {
	if peerPodUID != "" && peerPodUID != hdr.WorkerPodUID {
		return fmt.Errorf("%w: certificate pod %s, header pod %s", errIdentityMismatch, peerPodUID, hdr.WorkerPodUID)
	}
	if a.assignments == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, verifyTimeout)
	defer cancel()
	actor, err := a.assignments.GetActor(ctx, &ateapipb.GetActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: hdr.Atespace, Name: hdr.ActorName},
	})
	if err != nil {
		return fmt.Errorf("%w: %v", errAssignmentMismatch, err)
	}
	if actor.GetMetadata().GetUid() != hdr.ActorUID {
		return fmt.Errorf("%w: actor incarnation %s, header %s", errAssignmentMismatch, actor.GetMetadata().GetUid(), hdr.ActorUID)
	}
	assignment := actor.GetStatus().GetWorkerAssignment()
	if assignment.GetWorkerPodUid() != hdr.WorkerPodUID {
		return fmt.Errorf("%w: assigned worker %q, header worker %s", errAssignmentMismatch, assignment.GetWorkerPodUid(), hdr.WorkerPodUID)
	}
	if id := assignment.GetActivationId(); id != "" && id != hdr.ActivationID {
		return fmt.Errorf("%w: activation %s is not current", errAssignmentMismatch, hdr.ActivationID)
	}
	return nil
}

// peerPodUID is the pod UID in the TLS peer's pod identity certificate, or
// empty when the connection is not TLS or carries no pod identity.
func peerPodUID(conn net.Conn) string {
	tc, ok := conn.(*tls.Conn)
	if !ok {
		return ""
	}
	certs := tc.ConnectionState().PeerCertificates
	if len(certs) == 0 {
		return ""
	}
	identity, err := substratex509.PodIdentityFromCertificate(certs[0])
	if err != nil || identity == nil {
		return ""
	}
	return identity.PodUID
}
