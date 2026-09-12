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
	"io"
	"testing"
)

// BenchmarkWriteReadFrame measures the per-frame codec cost at the link MTU,
// the size of an ordinary tunneled Ethernet frame.
func BenchmarkWriteReadFrame(b *testing.B) {
	frame := bytes.Repeat([]byte{0xab}, 1514)
	buf := bytes.NewBuffer(make([]byte, 0, 2048))
	b.SetBytes(int64(len(frame)))
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		buf.Reset()
		if err := WriteFrame(buf, frame); err != nil {
			b.Fatal(err)
		}
		if _, err := ReadFrame(buf); err != nil && err != io.EOF {
			b.Fatal(err)
		}
	}
}
