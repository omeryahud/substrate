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

// Package shuttle carries an actor's Ethernet frames between the worker's end
// of the sandbox veth and the connection anchor. It holds no protocol state
// above Ethernet: the actor's TCP endpoints live in the anchor, so this is the
// only thing about the actor's network that stays on the worker.
package shuttle

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"time"

	"golang.org/x/sys/unix"

	"github.com/agent-substrate/substrate/internal/anchortun"
)

// Config configures one actor's shuttle.
type Config struct {
	// IfaceName is the worker-side end of the sandbox veth.
	IfaceName string
	// AttachAddress is the anchor's attach listener as host:port.
	AttachAddress string
	// ServerName is the name expected on the anchor's serving certificate.
	ServerName string
	// CredentialBundlePath is the worker's client certificate and key.
	CredentialBundlePath string
	// TrustBundlePath verifies the anchor's serving certificate.
	TrustBundlePath string
	// Header identifies the actor and activation to the anchor.
	Header anchortun.AttachHeader
}

const (
	maxFrame        = 65536
	redialMin       = 200 * time.Millisecond
	redialMax       = 5 * time.Second
	handshakeExpiry = 10 * time.Second
)

// Attach opens the packet socket, dials the anchor, sends the attach header,
// and starts moving frames. It returns once the first attach succeeded. The
// returned stop tears the tunnel and the socket down; until then the shuttle
// redials the anchor with backoff if the tunnel drops, resending the header,
// so an anchor restart does not strand a running actor.
func Attach(ctx context.Context, cfg Config) (stop func(), err error) {
	if err := cfg.Header.Valid(); err != nil {
		return nil, err
	}
	tlsCfg, err := clientTLS(cfg)
	if err != nil {
		return nil, err
	}
	sock, err := openPacketSocket(cfg.IfaceName)
	if err != nil {
		return nil, err
	}

	runCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	conn, err := dialAndAttach(runCtx, cfg, tlsCfg)
	if err != nil {
		cancel()
		sock.Close()
		return nil, err
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		run(runCtx, cfg, tlsCfg, sock, conn)
	}()
	return func() {
		cancel()
		sock.Close()
		<-done
	}, nil
}

// run pumps frames over conn, then redials for as long as ctx lives.
func run(ctx context.Context, cfg Config, tlsCfg *tls.Config, sock *os.File, conn net.Conn) {
	backoff := redialMin
	for {
		err := pump(ctx, sock, conn)
		conn.Close()
		if ctx.Err() != nil {
			return
		}
		slog.WarnContext(ctx, "shuttle: tunnel dropped, redialing anchor",
			slog.Any("actor", cfg.Header.Ref()), slog.Any("err", err))
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			conn, err = dialAndAttach(ctx, cfg, tlsCfg)
			if err == nil {
				backoff = redialMin
				break
			}
			slog.WarnContext(ctx, "shuttle: redial failed", slog.Any("err", err))
			backoff = min(backoff*2, redialMax)
		}
	}
}

// pump copies frames both ways until either side fails or ctx ends.
func pump(ctx context.Context, sock *os.File, conn net.Conn) error {
	fc := anchortun.NewFrameConn(conn)
	errc := make(chan error, 2)
	go func() {
		buf := make([]byte, maxFrame)
		for {
			n, err := sock.Read(buf)
			if err != nil {
				errc <- fmt.Errorf("reading veth: %w", err)
				return
			}
			if n == 0 {
				continue
			}
			if err := fc.WriteFrame(buf[:n]); err != nil {
				errc <- fmt.Errorf("writing tunnel: %w", err)
				return
			}
		}
	}()
	go func() {
		for {
			frame, err := fc.ReadFrame()
			if err != nil {
				errc <- fmt.Errorf("reading tunnel: %w", err)
				return
			}
			if _, err := sock.Write(frame); err != nil {
				errc <- fmt.Errorf("writing veth: %w", err)
				return
			}
		}
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errc:
		return err
	}
}

// dialAndAttach opens the mTLS tunnel and sends the attach header as its
// first frame.
func dialAndAttach(ctx context.Context, cfg Config, tlsCfg *tls.Config) (net.Conn, error) {
	d := &tls.Dialer{NetDialer: &net.Dialer{Timeout: handshakeExpiry}, Config: tlsCfg}
	conn, err := d.DialContext(ctx, "tcp", cfg.AttachAddress)
	if err != nil {
		return nil, fmt.Errorf("shuttle: dialing anchor %s: %w", cfg.AttachAddress, err)
	}
	hdr, err := anchortun.MarshalAttachHeader(cfg.Header)
	if err != nil {
		conn.Close()
		return nil, err
	}
	_ = conn.SetWriteDeadline(time.Now().Add(handshakeExpiry))
	if err := anchortun.WriteFrame(conn, hdr); err != nil {
		conn.Close()
		return nil, fmt.Errorf("shuttle: sending attach header: %w", err)
	}
	_ = conn.SetWriteDeadline(time.Time{})
	return conn, nil
}

func clientTLS(cfg Config) (*tls.Config, error) {
	trustPEM, err := os.ReadFile(cfg.TrustBundlePath)
	if err != nil {
		return nil, fmt.Errorf("shuttle: reading trust bundle: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(trustPEM) {
		return nil, fmt.Errorf("shuttle: trust bundle %q contains no certificates", cfg.TrustBundlePath)
	}
	return &tls.Config{
		MinVersion: tls.VersionTLS12,
		RootCAs:    roots,
		ServerName: cfg.ServerName,
		GetClientCertificate: func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
			pemBytes, err := os.ReadFile(cfg.CredentialBundlePath)
			if err != nil {
				return nil, err
			}
			cert, err := tls.X509KeyPair(pemBytes, pemBytes)
			if err != nil {
				return nil, err
			}
			return &cert, nil
		},
	}, nil
}

// openPacketSocket binds a raw packet socket to iface in promiscuous mode and
// wraps it in an os.File so reads honor Close and the runtime poller.
func openPacketSocket(iface string) (*os.File, error) {
	ifi, err := net.InterfaceByName(iface)
	if err != nil {
		return nil, fmt.Errorf("shuttle: interface %q: %w", iface, err)
	}
	proto := htons(unix.ETH_P_ALL)
	fd, err := unix.Socket(unix.AF_PACKET, unix.SOCK_RAW|unix.SOCK_CLOEXEC|unix.SOCK_NONBLOCK, int(proto))
	if err != nil {
		return nil, fmt.Errorf("shuttle: packet socket: %w", err)
	}
	if err := unix.Bind(fd, &unix.SockaddrLinklayer{Protocol: proto, Ifindex: ifi.Index}); err != nil {
		unix.Close(fd)
		return nil, fmt.Errorf("shuttle: binding packet socket to %q: %w", iface, err)
	}
	mreq := unix.PacketMreq{Ifindex: int32(ifi.Index), Type: unix.PACKET_MR_PROMISC}
	if err := unix.SetsockoptPacketMreq(fd, unix.SOL_PACKET, unix.PACKET_ADD_MEMBERSHIP, &mreq); err != nil {
		unix.Close(fd)
		return nil, fmt.Errorf("shuttle: enabling promiscuous mode on %q: %w", iface, err)
	}
	return os.NewFile(uintptr(fd), "af_packet:"+iface), nil
}

// htons converts a 16-bit protocol number to network byte order as the
// packet socket API expects it in the protocol argument.
func htons(v uint16) uint16 {
	var b [2]byte
	binary.BigEndian.PutUint16(b[:], v)
	return binary.NativeEndian.Uint16(b[:])
}

var _ = errors.New
