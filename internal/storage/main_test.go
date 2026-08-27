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

package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// awsCredentials is the shared credentials file the S3 tests authenticate with.
// It matches the MinIO instance started by .github/workflows/tests.yml.
const awsCredentials = `[minio]
aws_access_key_id = minioadmin
aws_secret_access_key = minioadminpassword`

// TestMain points the AWS SDK at a throwaway credentials file. The tests need a
// "minio" profile to exist, but must never write to the developer's real
// ~/.aws/credentials. This runs before any test starts, so the environment is
// set once for the whole package rather than by each test; t.Setenv is not an
// option because these tests run in parallel.
func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "absctl-storage-test")
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create temp dir: %v\n", err)
		os.Exit(1)
	}

	path := filepath.Join(dir, "credentials")
	if err := os.WriteFile(path, []byte(awsCredentials), 0o600); err != nil {
		fmt.Fprintf(os.Stderr, "failed to write credentials file: %v\n", err)
		os.RemoveAll(dir)
		os.Exit(1)
	}

	//nolint:usetesting // TestMain runs before the parallel tests, so t.Setenv is unavailable here.
	if err := os.Setenv("AWS_SHARED_CREDENTIALS_FILE", path); err != nil {
		fmt.Fprintf(os.Stderr, "failed to set credentials env: %v\n", err)
		os.RemoveAll(dir)
		os.Exit(1)
	}

	code := m.Run()

	os.RemoveAll(dir)
	os.Exit(code)
}
