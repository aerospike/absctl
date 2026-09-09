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

// Package testutil holds helpers shared between the package test suites.
package testutil

import (
	"os"
	"testing"
)

// EnvIntegration enables the tests that need the services defined in
// .github/workflows/tests.yml. It is unset by default so that a clean
// checkout can run `go test ./...` without any of them.
const EnvIntegration = "ABSCTL_INTEGRATION"

// Service descriptions used in skip messages, so a developer reading the
// output knows exactly what to start.
const (
	ServiceAerospike = "an Aerospike cluster at 127.0.0.1:3000"
	ServiceS3        = "MinIO at 127.0.0.1:9000"
	ServiceGCP       = "fake-gcs-server at 127.0.0.1:4443"
	ServiceAzure     = "Azurite at 127.0.0.1:10000"
)

// RequireIntegration skips the test unless integration testing is enabled.
// service names the dependency the caller needs.
func RequireIntegration(t *testing.T, service string) {
	t.Helper()

	if os.Getenv(EnvIntegration) == "" {
		t.Skipf("skipping: requires %s; set %s=1 to run", service, EnvIntegration)
	}
}
