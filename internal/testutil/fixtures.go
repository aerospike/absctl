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
	"path/filepath"
	"testing"

	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/backup-go/io/encoding/asb"
	bgmodels "github.com/aerospike/backup-go/models"
	"github.com/stretchr/testify/require"
)

// awsEnvToClear lists the AWS SDK environment variables that change how a client with
// an explicit endpoint, region and credentials is built, or make building it fail.
var awsEnvToClear = []string{
	"AWS_PROFILE",
	"AWS_CA_BUNDLE",
	"AWS_REGION",
	"AWS_DEFAULT_REGION",
	"AWS_ENDPOINT_URL",
	"AWS_ENDPOINT_URL_S3",
	"AWS_USE_FIPS_ENDPOINT",
	"AWS_USE_DUALSTACK_ENDPOINT",
}

// IsolateAWSConfig makes the AWS SDK ignore the shared config and credentials files and
// the AWS settings of the environment, so that a test configuring S3 explicitly does
// not depend on the machine it runs on. It uses t.Setenv, so the test must not be
// parallel.
func IsolateAWSConfig(t *testing.T) {
	t.Helper()

	for _, name := range awsEnvToClear {
		t.Setenv(name, "")
	}

	missing := t.TempDir()

	t.Setenv("AWS_CONFIG_FILE", filepath.Join(missing, "config"))
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", filepath.Join(missing, "credentials"))
}

// NewBackupFile returns the content of a valid backup file of namespace holding one
// record in set.
func NewBackupFile(t *testing.T, namespace, set string) []byte {
	t.Helper()

	key, aerr := aerospike.NewKey(namespace, set, "key")
	require.NoError(t, aerr)

	record := &bgmodels.Record{Record: &aerospike.Record{
		Key:        key,
		Bins:       aerospike.BinMap{"bin": 1},
		Generation: 1,
	}}

	encoder := asb.NewEncoder(asb.NewEncoderConfig(namespace, false, bgmodels.SIndexInfo{}))

	data, err := encoder.EncodeToken(bgmodels.NewRecordToken(record, 0, nil), encoder.GetHeader(true))
	require.NoError(t, err)

	return data
}
