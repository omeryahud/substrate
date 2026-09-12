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

// Package anchortun defines the wire protocol for the frame tunnel between a
// worker's frame shuttle and a connection anchor. The tunnel carries whole
// Ethernet frames of one actor over a single authenticated stream, so the
// actor's TCP connections can terminate in the anchor and survive suspend and
// resume on any worker.
package anchortun

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// MaxFrameSize bounds one tunneled Ethernet frame and caps the allocation a
// length prefix can force. The shuttle disables offloads, so frames stay at
// the link MTU of about 1514 bytes; the larger cap only leaves headroom
// without trusting the peer's length.
const MaxFrameSize = 65535

// frameLenBytes is the width of the big-endian length prefix on each frame.
const frameLenBytes = 4

// ErrFrameTooLarge reports a frame whose length prefix exceeds MaxFrameSize.
var ErrFrameTooLarge = errors.New("anchortun: frame exceeds max size")

// ErrEmptyFrame reports an attempt to write a zero-length frame.
var ErrEmptyFrame = errors.New("anchortun: empty frame")

// WriteFrame writes one length-prefixed Ethernet frame to w. It writes the
// prefix and payload in a single call so a stream shared by one writer never
// interleaves a prefix with another frame's payload.
func WriteFrame(w io.Writer, frame []byte) error {
	if len(frame) == 0 {
		return ErrEmptyFrame
	}
	if len(frame) > MaxFrameSize {
		return fmt.Errorf("%w: %d bytes", ErrFrameTooLarge, len(frame))
	}
	buf := make([]byte, frameLenBytes+len(frame))
	binary.BigEndian.PutUint32(buf, uint32(len(frame)))
	copy(buf[frameLenBytes:], frame)
	if _, err := w.Write(buf); err != nil {
		return fmt.Errorf("anchortun: writing frame: %w", err)
	}
	return nil
}

// ReadFrame reads one length-prefixed Ethernet frame from r. A clean end of
// stream at a frame boundary returns io.EOF, the signal a read loop uses to
// stop. A length above MaxFrameSize returns ErrFrameTooLarge and leaves r
// positioned after the prefix, so the caller must close the stream rather than
// keep reading. A short read of the payload returns io.ErrUnexpectedEOF.
func ReadFrame(r io.Reader) ([]byte, error) {
	var lenBuf [frameLenBytes]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(lenBuf[:])
	if n == 0 {
		return nil, ErrEmptyFrame
	}
	if n > MaxFrameSize {
		return nil, fmt.Errorf("%w: %d bytes", ErrFrameTooLarge, n)
	}
	frame := make([]byte, n)
	if _, err := io.ReadFull(r, frame); err != nil {
		return nil, err
	}
	return frame, nil
}

// FrameConn carries frames over one stream while reusing its read and write
// buffers, so steady-state framing on the data path does not allocate per
// frame. One FrameConn is owned by a single goroutine for reads and a single
// goroutine for writes; it is not safe for concurrent reads or concurrent
// writes.
type FrameConn struct {
	rw     io.ReadWriter
	wbuf   []byte
	rbuf   []byte
	lenBuf [frameLenBytes]byte
}

// NewFrameConn wraps a stream for buffered framing.
func NewFrameConn(rw io.ReadWriter) *FrameConn {
	return &FrameConn{rw: rw}
}

// WriteFrame writes one frame, reusing an internal buffer. It has the same
// size rules as the package-level WriteFrame.
func (c *FrameConn) WriteFrame(frame []byte) error {
	if len(frame) == 0 {
		return ErrEmptyFrame
	}
	if len(frame) > MaxFrameSize {
		return fmt.Errorf("%w: %d bytes", ErrFrameTooLarge, len(frame))
	}
	need := frameLenBytes + len(frame)
	if cap(c.wbuf) < need {
		c.wbuf = make([]byte, need)
	}
	c.wbuf = c.wbuf[:need]
	binary.BigEndian.PutUint32(c.wbuf, uint32(len(frame)))
	copy(c.wbuf[frameLenBytes:], frame)
	if _, err := c.rw.Write(c.wbuf); err != nil {
		return fmt.Errorf("anchortun: writing frame: %w", err)
	}
	return nil
}

// ReadFrame reads one frame into an internal buffer and returns a slice that is
// valid only until the next ReadFrame on this FrameConn, like bufio.Scanner.
// Copy the bytes to keep them. Its error semantics match the package-level
// ReadFrame.
func (c *FrameConn) ReadFrame() ([]byte, error) {
	if _, err := io.ReadFull(c.rw, c.lenBuf[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(c.lenBuf[:])
	if n == 0 {
		return nil, ErrEmptyFrame
	}
	if n > MaxFrameSize {
		return nil, fmt.Errorf("%w: %d bytes", ErrFrameTooLarge, n)
	}
	if uint32(cap(c.rbuf)) < n {
		c.rbuf = make([]byte, n)
	}
	c.rbuf = c.rbuf[:n]
	if _, err := io.ReadFull(c.rw, c.rbuf); err != nil {
		return nil, err
	}
	return c.rbuf, nil
}
