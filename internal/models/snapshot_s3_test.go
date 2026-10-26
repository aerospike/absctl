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

package models

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateS3Throughput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		giveThroughput int
		wantErrMessage string
	}{
		{name: "server default"},
		{name: "at the limit", giveThroughput: maxSnapshotS3Throughput},
		{
			name:           "above range",
			giveThroughput: maxSnapshotS3Throughput + 1,
			wantErrMessage: "s3-throughput must be between 0 and 400, got 401",
		},
		{
			name:           "negative",
			giveThroughput: -1,
			wantErrMessage: "s3-throughput must be between 0 and 400, got -1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := ValidateS3Throughput(tt.giveThroughput)
			if tt.wantErrMessage != "" {
				require.EqualError(t, err, tt.wantErrMessage)
				return
			}

			require.NoError(t, err)
		})
	}
}

func TestSnapshotS3_ToAwsS3(t *testing.T) {
	t.Parallel()

	s3 := &SnapshotS3{
		BucketName:       "my-bucket",
		Region:           "eu-central-1",
		Profile:          "default",
		Endpoint:         "http://127.0.0.1:9000",
		AccessKeyID:      "access",
		SecretAccessKey:  "secret",
		RetryMaxAttempts: 3,
		RetryMaxBackoff:  1000,
		MaxConnsPerHost:  16,
		RequestTimeout:   5000,
		Throughput:       10,
	}

	want := &AwsS3{
		BucketName:       s3.BucketName,
		Region:           s3.Region,
		Profile:          s3.Profile,
		Endpoint:         s3.Endpoint,
		AccessKeyID:      s3.AccessKeyID,
		SecretAccessKey:  s3.SecretAccessKey,
		RetryMaxAttempts: s3.RetryMaxAttempts,
		RetryMaxBackoff:  s3.RetryMaxBackoff,
		// The settings the snapshot commands do not expose keep their defaults.
		RestorePollDuration:  DefaultS3RestorePollDuration,
		ChunkSize:            DefaultS3ChunkSize,
		MaxConnsPerHost:      s3.MaxConnsPerHost,
		RequestTimeout:       s3.RequestTimeout,
		RetryReadBackoff:     DefaultCloudRetryReadBackoff,
		RetryReadMultiplier:  DefaultCloudRetryReadMultiplier,
		RetryReadMaxAttempts: DefaultCloudRetryReadMaxAttempts,
	}

	got := s3.ToAwsS3()
	assert.Equal(t, want, got)
	require.NoError(t, got.Validate(false))

	var nilS3 *SnapshotS3
	assert.Nil(t, nilS3.ToAwsS3())
}
