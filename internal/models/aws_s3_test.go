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

const (
	testBucketName = "test-bucket"
)

func TestAwsS3_Validate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		aws      *AwsS3
		isBackup bool
		wantErr  string
	}{
		{
			name: "valid backup configuration",
			aws: &AwsS3{
				BucketName:        testBucketName,
				RetryMaxAttempts:  3,
				RetryMaxBackoff:   30,
				ChunkSize:         1024,
				UploadConcurrency: 1,
			},
			isBackup: true,
			wantErr:  "",
		},
		{
			name: "valid restore configuration",
			aws: &AwsS3{
				BucketName:          testBucketName,
				RetryMaxAttempts:    3,
				RetryMaxBackoff:     30,
				ChunkSize:           1024,
				RestorePollDuration: 10,
				StorageCommon: StorageCommon{
					RetryReadMultiplier: 3,
					RetryReadBackoff:    3,
				},
			},
			isBackup: false,
			wantErr:  "",
		},
		{
			name: "empty bucket name",
			aws: &AwsS3{
				BucketName: "",
			},
			isBackup: true,
			wantErr:  "bucket name is required",
		},
		{
			name: "negative retry max attempts",
			aws: &AwsS3{
				BucketName:       testBucketName,
				RetryMaxAttempts: -1,
				ChunkSize:        5,
			},
			isBackup: true,
			wantErr:  "retry maximum attempts must be non-negative",
		},
		{
			name: "negative retry max backoff milliseconds",
			aws: &AwsS3{
				BucketName:       testBucketName,
				RetryMaxAttempts: 3,
				RetryMaxBackoff:  -1,
			},
			isBackup: true,
			wantErr:  "retry max backoff must be non-negative",
		},
		{
			name: "small chunk size",
			aws: &AwsS3{
				BucketName:       testBucketName,
				RetryMaxAttempts: 3,
				RetryMaxBackoff:  30,
				ChunkSize:        4,
			},
			isBackup: true,
			wantErr:  "chunk size can't be less than 5",
		},
		{
			name: "restore poll duration less than 1 for restore",
			aws: &AwsS3{
				BucketName:          testBucketName,
				RetryMaxAttempts:    3,
				RetryMaxBackoff:     30,
				ChunkSize:           1024,
				RestorePollDuration: 0,
			},
			isBackup: false,
			wantErr:  "restore poll duration can't be less than 1",
		},
		{
			name: "restore poll duration less than 1 for backup is ok",
			aws: &AwsS3{
				BucketName:          testBucketName,
				RetryMaxAttempts:    3,
				RetryMaxBackoff:     30,
				ChunkSize:           1024,
				RestorePollDuration: 0,
				UploadConcurrency:   1,
			},
			isBackup: true,
			wantErr:  "",
		},
		{
			name: "zero values are valid",
			aws: &AwsS3{
				BucketName:          testBucketName,
				RetryMaxAttempts:    0,
				RetryMaxBackoff:     0,
				ChunkSize:           5,
				RestorePollDuration: 1,
				StorageCommon: StorageCommon{
					RetryReadMultiplier: 3,
					RetryReadBackoff:    3,
				},
			},
			isBackup: false,
			wantErr:  "",
		},
		{
			name: "restore poll duration exactly 1 is valid",
			aws: &AwsS3{
				BucketName:          testBucketName,
				RetryMaxAttempts:    0,
				RetryMaxBackoff:     0,
				ChunkSize:           5,
				RestorePollDuration: 1,
				StorageCommon: StorageCommon{
					RetryReadMultiplier: 3,
					RetryReadBackoff:    3,
				},
			},
			isBackup: false,
			wantErr:  "",
		},
		{
			name: "negative max connections per host",
			aws: &AwsS3{
				BucketName:        testBucketName,
				RetryMaxAttempts:  3,
				RetryMaxBackoff:   30,
				UploadConcurrency: 1,
				StorageCommon:     StorageCommon{MaxConnsPerHost: -1},
				ChunkSize:         5,
			},
			isBackup: true,
			wantErr:  "max connections per host must be non-negative",
		},
		{
			name: "negative request timeout",
			aws: &AwsS3{
				BucketName:        testBucketName,
				RetryMaxAttempts:  3,
				RetryMaxBackoff:   30,
				UploadConcurrency: 1,
				StorageCommon:     StorageCommon{RequestTimeout: -1},
				ChunkSize:         5,
			},
			isBackup: true,
			wantErr:  "request timeout must be non-negative",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.aws.LoadSecrets(t.Context(), nil)
			require.NoError(t, err)

			err = tt.aws.Validate(tt.isBackup)

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
