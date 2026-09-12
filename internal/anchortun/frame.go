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

// MaxFrameSize bounds one tunneled Ethernet frame. It is large enough for a
// GSO super-frame; the shuttle disables offloads so ordinary frames stay at
// the link MTU. The cap stops a corrupt or hostile length prefix from forcing
// a huge allocation.
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

// ReadFrame reads one length-prefixed Ethernet frame from r. A length above
// MaxFrameSize returns ErrFrameTooLarge and leaves r positioned after the
// prefix, so the caller must close the stream rather than keep reading. A
// short read of the payload returns io.ErrUnexpectedEOF.
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
