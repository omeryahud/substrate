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
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/spf13/cobra"

	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/serverboot"
)

// NewAnchorCmd returns the `atenet anchor` subcommand.
func NewAnchorCmd() *cobra.Command {
	var cfg Config
	var logLevel string
	cmd := &cobra.Command{
		Use:   "anchor",
		Short: "Connection anchor: terminates actors' TCP connections off the worker so they survive suspend and resume",
		RunE: func(cmd *cobra.Command, args []string) error {
			slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: serverboot.LogLevel()})))
			if err := serverboot.SetLogLevel(logLevel); err != nil {
				return err
			}
			a, err := New(cfg)
			if err != nil {
				return fmt.Errorf("creating anchor: %w", err)
			}
			return a.Run(cmd.Context())
		},
	}
	cmd.Flags().StringVar(&cfg.IngressListen, "ingress-listen", ":443", "mTLS listener the router sends actor ingress to")
	cmd.Flags().StringVar(&cfg.AttachListen, "attach-listen", ":445", "mTLS listener workers attach actor frame tunnels to")
	cmd.Flags().StringVar(&cfg.ControlListen, "control-listen", ":446", "mTLS listener for worker control requests such as readiness probes")
	cmd.Flags().StringVar(&cfg.IngressCredentialBundlePath, "ingress-credential-bundle", "/run/podidentity.podcert.ate.dev/credential-bundle.pem", "PEM pod-identity credential bundle the ingress listener presents to the router")
	cmd.Flags().StringVar(&cfg.CredentialBundlePath, "credential-bundle", "/run/servicedns.podcert.ate.dev/credential-bundle.pem", "PEM service-DNS credential bundle the attach and control listeners present to workers")
	cmd.Flags().StringVar(&cfg.TrustBundlePath, "trust-bundle", "/run/podidentity.podcert.ate.dev/trust-bundle.pem", "PEM trust bundle that verifies router and worker client certificates")
	cmd.Flags().StringVar(&cfg.RouterClientID, "router-client-identity", "spiffe://cluster.local/ns/ate-system/sa/atenet-router", "SPIFFE identity allowed on the ingress listener")
	cmd.Flags().StringVar(&cfg.MetricsAddr, "metrics-listen-addr", ":9090", "Address the Prometheus metrics and health server listens on; empty disables it")
	cmd.Flags().DurationVar(&cfg.HoldTTL, "hold-ttl", 24*time.Hour, "Drop an actor's held connections when no worker reattaches within this time; 0 holds forever")
	cmd.Flags().BoolVar(&cfg.LogFrames, "log-frames", false, "Log one line per Ethernet frame at debug level")
	cmd.Flags().StringVar(&cfg.Egress.BrokerSocketPath, "credential-broker-socket", ateompath.CredentialBrokerSocket, "Node-local atelet socket that mints actor certificates for egress; empty disables actor egress")
	cmd.Flags().StringVar(&cfg.Egress.GatewayTrustBundlePath, "egress-gateway-trust-bundle", "/run/servicedns.podcert.ate.dev/trust-bundle.pem", "PEM trust bundle that verifies the egress gateway's serving certificate")
	cmd.Flags().StringVar(&cfg.Egress.DNSUpstream, "dns-upstream", "", "Resolver (host:port) actor DNS queries are forwarded to; empty uses /etc/resolv.conf")
	cmd.Flags().StringVar(&cfg.ConnectListen, "connect-listen", ":444", "mTLS listener for the router's raw CONNECT tunnels into actors; empty disables it")
	cmd.Flags().StringVar(&cfg.StatusListen, "status-listen", ":8080", "Plain HTTP listener for /statusz; empty disables it")
	cmd.Flags().IntVar(&cfg.Hold.MaxActors, "max-actors", 4096, "Most actor stacks this anchor holds, attached or held; further attaches are refused")
	cmd.Flags().IntVar(&cfg.Hold.MaxConnections, "max-connections", 65536, "Most TCP connections across all actor stacks; further connections are refused")
	cmd.Flags().DurationVar(&cfg.Hold.WakeInterval, "wake-interval", time.Second, "Least time between two resume attempts for one actor woken by data")
	cmd.Flags().DurationVar(&cfg.Hold.UnquiesceAfter, "unquiesce-after", 3*time.Second, "How long after a worker attaches held writes are let go when no readiness probe reported the actor back sooner")
	cmd.Flags().StringVar(&cfg.Ateapi.Address, "ateapi-address", "", "gRPC dial target of ateapi; with --ateapi-ca-file and --ateapi-client-cert, every attach is checked against the actor's assignment. Empty skips the check")
	cmd.Flags().StringVar(&cfg.Ateapi.CAFile, "ateapi-ca-file", "", "PEM file with CAs trusted to verify the ateapi server certificate")
	cmd.Flags().StringVar(&cfg.Ateapi.ServerName, "ateapi-server-name", "", "Hostname expected on the ateapi server certificate")
	cmd.Flags().StringVar(&cfg.Ateapi.ClientCertPath, "ateapi-client-cert", "", "Credential bundle presented as the client certificate when dialing ateapi")
	cmd.Flags().StringVar(&logLevel, "log-level", "info", "Log level: debug, info, warn, error")
	return cmd
}
