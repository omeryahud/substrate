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
	"bufio"
	"net"
	"strings"
	"time"
)

// echoLines echoes every line it reads. A line that starts with
// "delay=<duration> " is echoed without that prefix after the delay, so a
// client can get its reply long after it asked, for example after a suspend.
func echoLines(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	for {
		line, err := reader.ReadString('\n')
		if len(line) > 0 {
			delay, payload := parseDelay(line)
			time.Sleep(delay)
			if _, werr := conn.Write([]byte(payload)); werr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

func parseDelay(line string) (time.Duration, string) {
	spec, payload, ok := strings.Cut(strings.TrimPrefix(line, "delay="), " ")
	if !ok || !strings.HasPrefix(line, "delay=") {
		return 0, line
	}
	delay, err := time.ParseDuration(spec)
	if err != nil || delay < 0 {
		return 0, line
	}
	return delay, payload
}
