// Copyright 2026 Aerospike, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package testutil

import (
	"os"
	"testing"
)

// TestIntegrationEnabledInCI guards against the integration suite being
// silently disabled. Because RequireIntegration skips by default, dropping
// EnvIntegration from the workflow would reduce CI to unit tests while still
// reporting green. This fails loudly instead.
func TestIntegrationEnabledInCI(t *testing.T) {
	t.Parallel()

	if os.Getenv("CI") == "" {
		t.Skip("not running in CI")
	}

	if os.Getenv(EnvIntegration) == "" {
		t.Fatalf("%s must be set in CI, otherwise the integration tests are skipped silently", EnvIntegration)
	}
}
