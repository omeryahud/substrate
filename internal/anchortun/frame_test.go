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

// errWriter fails every write, to exercise the write-error path.
type errWriter struct{}

func (errWriter) Write([]byte) (int, error) { return 0, errors.New("boom") }

// rwPipe joins a bytes.Buffer for reads and a target writer, so a FrameConn
// can write to one place and read from another in a test.
type rwPipe struct {
	io.Reader
	io.Writer
}

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

func TestWriteFrame_WriteError(t *testing.T) {
	if err := WriteFrame(errWriter{}, []byte("data")); err == nil {
		t.Error("WriteFrame to a failing writer succeeded, want error")
	}
	fc := NewFrameConn(rwPipe{Reader: &bytes.Buffer{}, Writer: errWriter{}})
	if err := fc.WriteFrame([]byte("data")); err == nil {
		t.Error("FrameConn.WriteFrame to a failing writer succeeded, want error")
	}
}

// TestFrameConn_RoundTripNoAlloc: FrameConn round-trips frames and, after its
// buffers are warm, does not allocate per frame.
func TestFrameConn_RoundTripNoAlloc(t *testing.T) {
	var buf bytes.Buffer
	fc := NewFrameConn(&buf)
	frames := [][]byte{
		[]byte("first"),
		bytes.Repeat([]byte{0x7f}, 1514),
		[]byte("third"),
	}
	for i, f := range frames {
		if err := fc.WriteFrame(f); err != nil {
			t.Fatalf("WriteFrame #%d: %v", i, err)
		}
		got, err := fc.ReadFrame()
		if err != nil {
			t.Fatalf("ReadFrame #%d: %v", i, err)
		}
		if !bytes.Equal(got, f) {
			t.Errorf("frame #%d mismatch", i)
		}
	}

	warm := bytes.Repeat([]byte{0x5a}, 1514)
	allocs := testing.AllocsPerRun(100, func() {
		buf.Reset()
		if err := fc.WriteFrame(warm); err != nil {
			t.Fatal(err)
		}
		if _, err := fc.ReadFrame(); err != nil {
			t.Fatal(err)
		}
	})
	if allocs != 0 {
		t.Errorf("FrameConn steady-state allocs/op = %v, want 0", allocs)
	}
}

func TestFrameConn_Rejects(t *testing.T) {
	fc := NewFrameConn(&bytes.Buffer{})
	if err := fc.WriteFrame(nil); !errors.Is(err, ErrEmptyFrame) {
		t.Errorf("WriteFrame(nil) = %v, want ErrEmptyFrame", err)
	}
	if err := fc.WriteFrame(bytes.Repeat([]byte{0}, MaxFrameSize+1)); !errors.Is(err, ErrFrameTooLarge) {
		t.Errorf("WriteFrame(oversize) = %v, want ErrFrameTooLarge", err)
	}

	oversize := make([]byte, frameLenBytes)
	binary.BigEndian.PutUint32(oversize, MaxFrameSize+1)
	rfc := NewFrameConn(rwPipe{Reader: bytes.NewReader(oversize), Writer: io.Discard})
	if _, err := rfc.ReadFrame(); !errors.Is(err, ErrFrameTooLarge) {
		t.Errorf("ReadFrame(oversize) = %v, want ErrFrameTooLarge", err)
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
