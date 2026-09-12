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

package anchortun

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"
)

func TestWriteReadFrame_RoundTrip(t *testing.T) {
	frames := [][]byte{
		{0x01},
		[]byte("a single ethernet frame's worth of bytes"),
		bytes.Repeat([]byte{0xab}, 1514),
		bytes.Repeat([]byte{0xcd}, MaxFrameSize),
	}
	var buf bytes.Buffer
	for _, f := range frames {
		if err := WriteFrame(&buf, f); err != nil {
			t.Fatalf("WriteFrame(%d bytes): %v", len(f), err)
		}
	}
	for i, want := range frames {
		got, err := ReadFrame(&buf)
		if err != nil {
			t.Fatalf("ReadFrame #%d: %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("frame #%d round-trip mismatch: got %d bytes, want %d", i, len(got), len(want))
		}
	}
	if _, err := ReadFrame(&buf); !errors.Is(err, io.EOF) {
		t.Errorf("ReadFrame after last frame = %v, want io.EOF", err)
	}
}

func TestWriteFrame_Rejects(t *testing.T) {
	for _, tc := range []struct {
		name  string
		frame []byte
		want  error
	}{
		{"empty", nil, ErrEmptyFrame},
		{"tooLarge", bytes.Repeat([]byte{0}, MaxFrameSize+1), ErrFrameTooLarge},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := WriteFrame(io.Discard, tc.frame); !errors.Is(err, tc.want) {
				t.Errorf("WriteFrame = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestReadFrame_Rejects(t *testing.T) {
	oversize := make([]byte, frameLenBytes)
	binary.BigEndian.PutUint32(oversize, MaxFrameSize+1)

	zero := make([]byte, frameLenBytes) // length prefix of 0

	truncatedPrefix := []byte{0x00, 0x01}

	truncatedPayload := make([]byte, frameLenBytes+2)
	binary.BigEndian.PutUint32(truncatedPayload, 8) // claims 8, supplies 2

	for _, tc := range []struct {
		name string
		in   []byte
		want error
	}{
		{"oversizeLength", oversize, ErrFrameTooLarge},
		{"zeroLength", zero, ErrEmptyFrame},
		{"truncatedPrefix", truncatedPrefix, io.ErrUnexpectedEOF},
		{"truncatedPayload", truncatedPayload, io.ErrUnexpectedEOF},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ReadFrame(bytes.NewReader(tc.in))
			if !errors.Is(err, tc.want) {
				t.Errorf("ReadFrame = %v, want %v", err, tc.want)
			}
		})
	}
}
