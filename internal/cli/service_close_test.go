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

package cli

import (
	"testing"

	"github.com/aerospike/absctl/internal/testutil"
	"github.com/aerospike/backup-go/io/encoding/asb"
	"github.com/stretchr/testify/require"
)

// TestRestoreReleasesStorage checks that the restore command closes its service: the
// storage client of the reader leaves no connection, and so no goroutine, behind.
func TestRestoreReleasesStorage(t *testing.T) {
	// Not parallel: VerifyNoGoroutineLeaks and RunCLI need the process to themselves.
	const (
		bucket = "restore-bucket"
		dir    = "backup"
		set    = "release"
	)

	// Started before the leak check, so that the server outlives it and cannot close
	// the client connections itself.
	fakeS3 := testutil.NewFakeS3(t, bucket, map[string][]byte{
		dir + "/data" + asb.Extension: testutil.NewBackupFile(t, argNamespaceValue, set),
	})

	testutil.VerifyNoGoroutineLeaks(t)
	testutil.IsolateAWSConfig(t)

	// Validation reads the backup without a cluster, so the reader is all there is to
	// release.
	_, _, err := testutil.RunCLI(t, newTestRootCmd,
		argRestore, "--validate",
		argDirShort, dir,
		argNamespaceShort, argNamespaceValue,
		"--s3-bucket-name", bucket,
		"--s3-endpoint-override", fakeS3.URL,
		"--s3-region", "us-east-1",
		"--s3-access-key-id", "fake-key-id",
		"--s3-secret-access-key", "fake-secret",
	)
	require.NoError(t, err)
}

// TestBackupReleasesCluster checks that the backup command closes its service: the
// Aerospike client leaves no connection, and so no goroutine, behind.
func TestBackupReleasesCluster(t *testing.T) {
	// Not parallel: VerifyNoGoroutineLeaks and RunCLI need the process to themselves.
	const (
		clusterHost = "127.0.0.1"
		clusterPort = "3000"
		// clusterCredentials is both the user and the password of the test cluster.
		clusterCredentials = "admin"
	)

	testutil.RequireIntegration(t, testutil.ServiceAerospike)
	testutil.VerifyNoGoroutineLeaks(t)

	// A local directory holds no storage client, so the cluster client is all there is
	// to release.
	_, _, err := testutil.RunCLI(t, newTestRootCmd,
		argBackup,
		argDirShort, t.TempDir(),
		argNamespaceShort, argNamespaceValue,
		"--host", clusterHost,
		"--port", clusterPort,
		"--user", clusterCredentials,
		"--password", clusterCredentials,
	)
	require.NoError(t, err)
}
