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

package resources

const (
	// ConnectionPolicyAnnotation on an ActorTemplate selects how an Actor's
	// open TCP connections behave across Suspend and Resume.
	ConnectionPolicyAnnotation = "ate.dev/connection-policy"
	// ConnectionPolicyPreserve terminates the Actor's connections in the
	// connection anchor so they survive Suspend and Resume on any worker.
	ConnectionPolicyPreserve = "Preserve"
)

// PreservesConnections reports whether an ActorTemplate with these
// annotations opted into the connection anchor.
func PreservesConnections(annotations map[string]string) bool {
	return annotations[ConnectionPolicyAnnotation] == ConnectionPolicyPreserve
}
