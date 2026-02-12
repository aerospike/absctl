// Copyright 2024 Aerospike, Inc.
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

	"github.com/stretchr/testify/require"
)

func TestGcpStorage_Validate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		gcp     *GcpStorage
		wantErr string
	}{
		{
			name: "valid configuration",
			gcp: &GcpStorage{
				BucketName:             testBucketName,
				RetryMaxAttempts:       3,
				RetryBackoffMax:        60,
				RetryBackoffInit:       1,
				RetryBackoffMultiplier: 2.0,
				ChunkSize:              1024,
			},
			wantErr: "",
		},
		{
			name: "empty bucket name",
			gcp: &GcpStorage{
				BucketName: "",
			},
			wantErr: "bucket name is required",
		},
		{
			name: "negative retry max attempts",
			gcp: &GcpStorage{
				BucketName:       testBucketName,
				RetryMaxAttempts: -1,
			},
			wantErr: "retry maximum attempts must be non-negative",
		},
		{
			name: "negative retry backoff max milliseconds",
			gcp: &GcpStorage{
				BucketName:       testBucketName,
				RetryMaxAttempts: 3,
				RetryBackoffMax:  -1,
			},
			wantErr: "retry max backoff must be non-negative",
		},
		{
			name: "negative retry backoff init milliseconds",
			gcp: &GcpStorage{
				BucketName:       testBucketName,
				RetryMaxAttempts: 3,
				RetryBackoffMax:  60,
				RetryBackoffInit: -1,
			},
			wantErr: "retry backoff must be non-negative",
		},
		{
			name: "retry backoff multiplier less than 1",
			gcp: &GcpStorage{
				BucketName:             testBucketName,
				RetryMaxAttempts:       3,
				RetryBackoffMax:        60,
				RetryBackoffInit:       1,
				RetryBackoffMultiplier: 0.5,
			},
			wantErr: "retry backoff multiplier must be positive",
		},
		{
			name: "retry backoff multiplier exactly 1 is invalid",
			gcp: &GcpStorage{
				BucketName:             testBucketName,
				RetryMaxAttempts:       3,
				RetryBackoffMax:        60,
				RetryBackoffInit:       1,
				RetryBackoffMultiplier: -1.0,
			},
			wantErr: "retry backoff multiplier must be positive",
		},
		{
			name: "negative chunk size",
			gcp: &GcpStorage{
				BucketName:             testBucketName,
				RetryMaxAttempts:       3,
				RetryBackoffMax:        60,
				RetryBackoffInit:       1,
				RetryBackoffMultiplier: 2.0,
				ChunkSize:              -1,
			},
			wantErr: "chunk size can't be less than 1",
		},
		{
			name: "zero values are valid except multiplier",
			gcp: &GcpStorage{
				BucketName:             testBucketName,
				RetryMaxAttempts:       0,
				RetryBackoffMax:        0,
				RetryBackoffInit:       0,
				RetryBackoffMultiplier: 1.1,
				ChunkSize:              1,
			},
			wantErr: "",
		},
		{
			name: "multiplier slightly above 1 is valid",
			gcp: &GcpStorage{
				BucketName:             testBucketName,
				RetryMaxAttempts:       0,
				RetryBackoffMax:        0,
				RetryBackoffInit:       0,
				RetryBackoffMultiplier: 1.0001,
				ChunkSize:              1,
			},
			wantErr: "",
		},
		{
			name: "negative max connections per host",
			gcp: &GcpStorage{
				BucketName:             testBucketName,
				RetryMaxAttempts:       3,
				RetryBackoffMax:        60,
				RetryBackoffInit:       1,
				RetryBackoffMultiplier: 2.0,
				ChunkSize:              1024,
				StorageCommon:          StorageCommon{MaxConnsPerHost: -1},
			},
			wantErr: "max connections per host must be non-negative",
		},
		{
			name: "negative request timeout",
			gcp: &GcpStorage{
				BucketName:             testBucketName,
				RetryMaxAttempts:       3,
				RetryBackoffMax:        60,
				RetryBackoffInit:       1,
				RetryBackoffMultiplier: 2.0,
				ChunkSize:              1024,
				StorageCommon:          StorageCommon{RequestTimeout: -1},
			},
			wantErr: "request timeout must be non-negative",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.gcp.LoadSecrets(t.Context(), nil)
			require.NoError(t, err)

			err = tt.gcp.Validate(true)

			if tt.wantErr != "" {
				switch {
				case err == nil:
					t.Errorf("Validate() expected error %q, got nil", tt.wantErr)
				case err.Error() != tt.wantErr:
					t.Errorf("Validate() error = %q, want %q", err.Error(), tt.wantErr)
				}

				return
			}

			if err != nil {
				t.Errorf("Validate() unexpected error: %v", err)
			}
		})
	}
}
