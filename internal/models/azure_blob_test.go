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
)

const testEndpoint = "test-endpoint"

func TestAzureBlob_Validate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		azure    *AzureBlob
		isBackup bool
		wantErr  string
	}{
		{
			name: "valid backup configuration",
			azure: &AzureBlob{
				ContainerName:     testBucketName,
				RetryMaxAttempts:  3,
				RetryDelay:        5,
				RetryMaxDelay:     60,
				BlockSize:         1024,
				UploadConcurrency: 1,
				Endpoint:          testEndpoint,
			},
			isBackup: true,
			wantErr:  "",
		},
		{
			name: "valid restore configuration",
			azure: &AzureBlob{
				ContainerName:       testBucketName,
				RetryMaxAttempts:    3,
				RetryDelay:          5,
				RetryMaxDelay:       60,
				BlockSize:           1024,
				RestorePollDuration: 10,
				Endpoint:            testEndpoint,
				StorageCommon: StorageCommon{
					RetryReadMultiplier: 3,
					RetryReadBackoff:    3,
				},
			},
			isBackup: false,
			wantErr:  "",
		},
		{
			name: "empty container name",
			azure: &AzureBlob{
				ContainerName: "",
			},
			isBackup: true,
			wantErr:  "container name is required",
		},
		{
			name: "empty endpoint",
			azure: &AzureBlob{
				ContainerName: testBucketName,
			},
			isBackup: true,
			wantErr:  "endpoint is required",
		},
		{
			name: "negative retry max attempts",
			azure: &AzureBlob{
				ContainerName:    testBucketName,
				RetryMaxAttempts: -1,
				Endpoint:         testEndpoint,
			},
			isBackup: true,
			wantErr:  "retry maximum attempts must be non-negative",
		},
		{
			name: "negative retry delay milliseconds",
			azure: &AzureBlob{
				ContainerName:    testBucketName,
				RetryMaxAttempts: 3,
				RetryDelay:       -1,
				Endpoint:         testEndpoint,
			},
			isBackup: true,
			wantErr:  "retry delay must be non-negative",
		},
		{
			name: "negative retry max delay milliseconds",
			azure: &AzureBlob{
				ContainerName:    testBucketName,
				RetryMaxAttempts: 3,
				RetryDelay:       5,
				RetryMaxDelay:    -1,
				Endpoint:         testEndpoint,
			},
			isBackup: true,
			wantErr:  "retry max delay must be non-negative",
		},
		{
			name: "negative block size",
			azure: &AzureBlob{
				ContainerName:    testBucketName,
				RetryMaxAttempts: 3,
				RetryDelay:       5,
				RetryMaxDelay:    60,
				BlockSize:        -1,
				Endpoint:         testEndpoint,
			},
			isBackup: true,
			wantErr:  "block size can't be less than 1",
		},
		{
			name: "restore poll duration less than 1 for restore",
			azure: &AzureBlob{
				ContainerName:       testBucketName,
				RetryMaxAttempts:    3,
				RetryDelay:          5,
				RetryMaxDelay:       60,
				BlockSize:           1024,
				RestorePollDuration: 0,
				Endpoint:            testEndpoint,
			},
			isBackup: false,
			wantErr:  "restore poll duration can't be less than 1",
		},
		{
			name: "restore poll duration less than 1 for backup is ok",
			azure: &AzureBlob{
				ContainerName:       testBucketName,
				RetryMaxAttempts:    3,
				RetryDelay:          5,
				RetryMaxDelay:       60,
				BlockSize:           1024,
				RestorePollDuration: 0,
				UploadConcurrency:   1,
				Endpoint:            testEndpoint,
			},
			isBackup: true,
			wantErr:  "",
		},
		{
			name: "zero values are valid",
			azure: &AzureBlob{
				ContainerName:       testBucketName,
				RetryMaxAttempts:    0,
				RetryDelay:          0,
				RetryMaxDelay:       0,
				BlockSize:           1,
				RestorePollDuration: 1,
				Endpoint:            testEndpoint,
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
			azure: &AzureBlob{
				ContainerName:     testBucketName,
				RetryDelay:        5,
				RetryMaxDelay:     60,
				BlockSize:         1024,
				UploadConcurrency: 1,
				StorageCommon:     StorageCommon{MaxConnsPerHost: -1},
				Endpoint:          testEndpoint,
			},
			isBackup: true,
			wantErr:  "max connections per host must be non-negative",
		},
		{
			name: "restore poll duration exactly 1 is valid",
			azure: &AzureBlob{
				ContainerName:       testBucketName,
				RetryMaxAttempts:    0,
				RetryDelay:          0,
				RetryMaxDelay:       0,
				BlockSize:           1,
				RestorePollDuration: 1,
				Endpoint:            testEndpoint,
				StorageCommon: StorageCommon{
					RetryReadMultiplier: 3,
					RetryReadBackoff:    3,
				},
			},
			isBackup: false,
			wantErr:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.azure.Validate(tt.isBackup)

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
