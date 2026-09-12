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

package anchornet

import (
	"context"
	"errors"
	"io"

	"gvisor.dev/gvisor/pkg/buffer"
	"gvisor.dev/gvisor/pkg/tcpip/link/channel"
	"gvisor.dev/gvisor/pkg/tcpip/network/ipv4"
	"gvisor.dev/gvisor/pkg/tcpip/stack"

	"github.com/agent-substrate/substrate/internal/anchortun"
)

// Bridge carries a stack's frames over one tunnel stream using the anchortun
// frame codec, in both directions, until ctx is canceled or the tunnel fails.
// It is the shared core of both ends of the frame tunnel: the worker's shuttle
// runs it between the sandbox link and the tunnel, and the anchor runs it
// between the actor's link and the tunnel. Frames carry IP packets here because
// the test stacks are IP-only; the worker path carries Ethernet.
func Bridge(ctx context.Context, link *channel.Endpoint, tunnel io.ReadWriter) error {
	return BridgeFrameConn(ctx, link, anchortun.NewFrameConn(tunnel), nil)
}

// FrameHook observes every frame a bridge moves. outbound is true for frames
// the stack sends toward the tunnel. The frame is only valid during the call.
type FrameHook func(outbound bool, frame []byte)

// BridgeFrameConn is Bridge over an existing FrameConn, for a caller that has
// already read the attach header from the stream. hook may be nil.
func BridgeFrameConn(ctx context.Context, link *channel.Endpoint, fc *anchortun.FrameConn, hook FrameHook) error {
	errc := make(chan error, 2)

	go func() {
		errc <- writeOutbound(ctx, link, fc, hook)
	}()
	go func() {
		errc <- readInbound(link, fc, hook)
	}()

	err := <-errc
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

// writeOutbound serializes each outbound packet the stack emits into a tunnel
// frame.
func writeOutbound(ctx context.Context, link *channel.Endpoint, fc *anchortun.FrameConn, hook FrameHook) error {
	for {
		pkt := link.ReadContext(ctx)
		if pkt == nil {
			return ctx.Err()
		}
		buf := pkt.ToBuffer()
		frame := buf.Flatten()
		buf.Release()
		pkt.DecRef()
		if hook != nil {
			hook(true, frame)
		}
		if err := fc.WriteFrame(frame); err != nil {
			return err
		}
	}
}

// readInbound injects each tunnel frame into the stack as an inbound packet.
func readInbound(link *channel.Endpoint, fc *anchortun.FrameConn, hook FrameHook) error {
	for {
		frame, err := fc.ReadFrame()
		if err != nil {
			return err
		}
		if hook != nil {
			hook(false, frame)
		}
		pkt := stack.NewPacketBuffer(stack.PacketBufferOptions{
			Payload: buffer.MakeWithData(frame),
		})
		link.InjectInbound(ipv4.ProtocolNumber, pkt)
		pkt.DecRef()
	}
}
