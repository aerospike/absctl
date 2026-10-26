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

package flags

import (
	"slices"
	"testing"

	"github.com/aerospike/absctl/internal/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSnapshotS3_NewFlagSet_Modes(t *testing.T) {
	t.Parallel()

	connectionFlags := []string{
		flagS3BucketName, flagS3Region, flagS3Profile,
		flagS3AccessKeyID, flagS3SecretAccessKey, flagS3Endpoint,
	}
	optionalFlags := []string{
		flagS3Throughput, flagS3RetryMaxAttempts, flagS3RetryMaxBackoff,
		flagS3MaxConnsPerHost, flagS3RequestTimeout,
	}

	tests := []struct {
		name     string
		giveMode SnapshotS3Mode
		// wantOptional lists the flags besides the connection ones.
		wantOptional []string
	}{
		{
			name:         "backup start",
			giveMode:     SnapshotS3ModeBackupStart,
			wantOptional: []string{flagS3Throughput},
		},
		{
			name:     "restore start",
			giveMode: SnapshotS3ModeRestoreStart,
		},
		{
			name:     "reader",
			giveMode: SnapshotS3ModeReader,
			wantOptional: []string{
				flagS3RetryMaxAttempts, flagS3RetryMaxBackoff,
				flagS3MaxConnsPerHost, flagS3RequestTimeout,
			},
		},
		{
			name:     "progress",
			giveMode: SnapshotS3ModeProgress,
			wantOptional: []string{
				flagS3RetryMaxAttempts, flagS3RetryMaxBackoff, flagS3MaxConnsPerHost,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			flagSet := NewSnapshotS3(tt.giveMode).NewFlagSet()

			for _, name := range connectionFlags {
				assert.NotNil(t, flagSet.Lookup(name), name)
			}

			for _, name := range optionalFlags {
				if slices.Contains(tt.wantOptional, name) {
					assert.NotNil(t, flagSet.Lookup(name), name)
					continue
				}

				assert.Nil(t, flagSet.Lookup(name), name)
			}

			// The client-side flags of the scan S3 set that the snapshot commands do not use.
			assert.Nil(t, flagSet.Lookup(flagS3AccessTier))
			assert.Nil(t, flagSet.Lookup(flagS3RestorePollDuration))
			assert.Nil(t, flagSet.Lookup(flagS3RetryReadBackoff))
		})
	}
}

func TestSnapshotS3_NewFlagSet_Defaults(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		giveMode SnapshotS3Mode
	}{
		{name: "backup start", giveMode: SnapshotS3ModeBackupStart},
		{name: "restore start", giveMode: SnapshotS3ModeRestoreStart},
		{name: "reader", giveMode: SnapshotS3ModeReader},
		{name: "progress", giveMode: SnapshotS3ModeProgress},
	}

	want := &models.SnapshotS3{
		RetryMaxAttempts: models.DefaultS3RetryMaxAttempts,
		RetryMaxBackoff:  models.DefaultS3RetryMaxBackoff,
		MaxConnsPerHost:  models.DefaultCloudMaxConnsPerHost,
		RequestTimeout:   models.DefaultCloudRequestTimeout,
		Throughput:       models.DefaultSnapshotS3Throughput,
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// The settings a mode does not expose keep their defaults as well.
			s3 := NewSnapshotS3(tt.giveMode)
			require.NoError(t, s3.NewFlagSet().Parse(nil))

			assert.Equal(t, want, s3.GetSnapshotS3())
		})
	}
}

func TestSnapshotS3_NewFlagSet_Parse(t *testing.T) {
	t.Parallel()

	const (
		testBucket    = "my-bucket"
		testRegion    = "eu-central-1"
		testProfile   = "default"
		testEndpoint  = "http://127.0.0.1:9000"
		testAccessKey = "access"
		testSecretKey = "secret"
	)

	connectionArgs := []string{
		"--" + flagS3BucketName, testBucket,
		"--" + flagS3Region, testRegion,
		"--" + flagS3Profile, testProfile,
		"--" + flagS3Endpoint, testEndpoint,
		"--" + flagS3AccessKeyID, testAccessKey,
		"--" + flagS3SecretAccessKey, testSecretKey,
	}

	connection := models.SnapshotS3{
		BucketName:       testBucket,
		Region:           testRegion,
		Profile:          testProfile,
		Endpoint:         testEndpoint,
		AccessKeyID:      testAccessKey,
		SecretAccessKey:  testSecretKey,
		RetryMaxAttempts: models.DefaultS3RetryMaxAttempts,
		RetryMaxBackoff:  models.DefaultS3RetryMaxBackoff,
		MaxConnsPerHost:  models.DefaultCloudMaxConnsPerHost,
		RequestTimeout:   models.DefaultCloudRequestTimeout,
	}

	tests := []struct {
		name     string
		giveMode SnapshotS3Mode
		giveArgs []string
		want     func() models.SnapshotS3
	}{
		{
			name:     "backup start",
			giveMode: SnapshotS3ModeBackupStart,
			giveArgs: []string{"--" + flagS3Throughput, "10"},
			want: func() models.SnapshotS3 {
				want := connection
				want.Throughput = 10

				return want
			},
		},
		{
			name:     "restore start",
			giveMode: SnapshotS3ModeRestoreStart,
			want:     func() models.SnapshotS3 { return connection },
		},
		{
			name:     "reader",
			giveMode: SnapshotS3ModeReader,
			giveArgs: []string{
				"--" + flagS3RetryMaxAttempts, "3",
				"--" + flagS3RetryMaxBackoff, "1000",
				"--" + flagS3MaxConnsPerHost, "16",
				"--" + flagS3RequestTimeout, "5000",
			},
			want: func() models.SnapshotS3 {
				want := connection
				want.RetryMaxAttempts = 3
				want.RetryMaxBackoff = 1000
				want.MaxConnsPerHost = 16
				want.RequestTimeout = 5000

				return want
			},
		},
		{
			name:     "progress",
			giveMode: SnapshotS3ModeProgress,
			giveArgs: []string{
				"--" + flagS3RetryMaxAttempts, "3",
				"--" + flagS3RetryMaxBackoff, "1000",
				"--" + flagS3MaxConnsPerHost, "16",
			},
			want: func() models.SnapshotS3 {
				want := connection
				want.RetryMaxAttempts = 3
				want.RetryMaxBackoff = 1000
				want.MaxConnsPerHost = 16

				return want
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s3 := NewSnapshotS3(tt.giveMode)
			args := append(append([]string{}, connectionArgs...), tt.giveArgs...)
			require.NoError(t, s3.NewFlagSet().Parse(args))

			want := tt.want()
			assert.Equal(t, &want, s3.GetSnapshotS3())
		})
	}
}
