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

package config

import (
	"testing"

	"github.com/aerospike/absctl/internal/models"
	"github.com/stretchr/testify/assert"
)

const (
	testBucket   = "test-bucket"
	testEndpoint = "test-endpoint"
)

func TestValidateStorages(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		isBackup   bool
		awsS3      *models.AwsS3
		gcpStorage *models.GcpStorage
		azureBlob  *models.AzureBlob
		local      *models.Local
		wantErr    bool
	}{
		{
			name:     "Valid AWS S3 configuration only",
			isBackup: false,
			awsS3: &models.AwsS3{
				Region:              "us-west-2",
				BucketName:          testBucket,
				RestorePollDuration: 1,
				StorageCommon: models.StorageCommon{
					RetryReadMultiplier: 2,
					RetryReadBackoff:    100,
				},
				ChunkSize: 5,
			},
			wantErr: false,
		},
		{
			name:     "Valid GCP Storage configuration only",
			isBackup: false,
			gcpStorage: &models.GcpStorage{
				BucketName:             testBucket,
				RetryBackoffMultiplier: 2,
				StorageCommon: models.StorageCommon{
					RetryReadMultiplier: 2,
					RetryReadBackoff:    100,
				},
				ChunkSize: 5,
			},
			wantErr: false,
		},
		{
			name:     "Valid Azure Blob configuration only",
			isBackup: true,
			azureBlob: &models.AzureBlob{
				ContainerName:       testBucket,
				AccountName:         "account-name",
				AccountKey:          "account-key",
				RestorePollDuration: 1,
				UploadConcurrency:   10,
				BlockSize:           5,
				Endpoint:            testEndpoint,
			},
			wantErr: false,
		},
		{
			name:     "AWS S3 and GCP Storage both configured",
			isBackup: true,
			awsS3: &models.AwsS3{
				Region: "us-west-2",
			},
			gcpStorage: &models.GcpStorage{
				BucketName: "my-bucket",
			},
			wantErr: true,
		},
		{
			name:     "All three providers configured",
			isBackup: true,
			awsS3: &models.AwsS3{
				Region: "us-west-2",
			},
			gcpStorage: &models.GcpStorage{
				BucketName: "my-bucket",
			},
			azureBlob: &models.AzureBlob{
				ContainerName: "my-container",
				AccountName:   "account-name",
			},
			wantErr: true,
		},
		{
			name:     "None of the providers configured",
			isBackup: true,
			wantErr:  false,
		},
		{
			name:     "Partial AWS S3 configuration",
			isBackup: false,
			awsS3: &models.AwsS3{
				BucketName:          testBucket,
				Region:              "",
				Profile:             "default",
				RestorePollDuration: 1,
				StorageCommon: models.StorageCommon{
					RetryReadMultiplier: 2,
					RetryReadBackoff:    100,
				},
				ChunkSize: 5,
			},
			wantErr: false,
		},
		{
			name:     "Partial GCP Storage configuration",
			isBackup: true,
			gcpStorage: &models.GcpStorage{
				BucketName:             testBucket,
				KeyFile:                "",
				RetryBackoffMultiplier: 2,
				ChunkSize:              5,
			},
			wantErr: false,
		},
		{
			name:     "Partial Azure Blob configuration",
			isBackup: true,
			azureBlob: &models.AzureBlob{
				ContainerName:       testBucket,
				AccountName:         "account-name",
				AccountKey:          "",
				RestorePollDuration: 1,
				UploadConcurrency:   10,
				BlockSize:           5,
				Endpoint:            testEndpoint,
			},
			wantErr: false,
		},
		{
			name:     "Azure Blob with client credentials",
			isBackup: true,
			azureBlob: &models.AzureBlob{
				ContainerName:       testBucket,
				TenantID:            "tenant-id",
				ClientID:            "client-id",
				ClientSecret:        "client-secret",
				RestorePollDuration: 1,
				UploadConcurrency:   10,
				BlockSize:           5,
				Endpoint:            testEndpoint,
			},
			wantErr: false,
		},
		{
			name:     "Only endpoints configured",
			isBackup: true,
			awsS3: &models.AwsS3{
				BucketName:          "custom-bucket",
				Endpoint:            "custom-endpoint",
				RestorePollDuration: 1,
				UploadConcurrency:   10,
				ChunkSize:           5,
			},
			wantErr: false,
		},
		{
			name:     "Multiple providers with only endpoints",
			isBackup: true,
			awsS3: &models.AwsS3{
				Endpoint: "aws-endpoint",
			},
			gcpStorage: &models.GcpStorage{
				Endpoint: "gcp-endpoint",
			},
			wantErr: true,
		},
		{
			name:     "Negative AWS S3 poll duration",
			isBackup: false,
			awsS3: &models.AwsS3{
				Region:              "us-west-2",
				BucketName:          testBucket,
				RestorePollDuration: 0,
			},
			wantErr: true,
		},
		{
			name:     "Negative Azure poll duration",
			isBackup: false,
			azureBlob: &models.AzureBlob{
				ContainerName:       testBucket,
				AccountName:         "account-name",
				AccountKey:          "account-key",
				RestorePollDuration: -11,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := validateStorages(tt.isBackup, tt.awsS3, tt.gcpStorage, tt.azureBlob, tt.local)
			if tt.wantErr {
				assert.Error(t, err, "Expected error but got none")
			} else {
				assert.NoError(t, err, "Expected no error but got one")
			}
		})
	}
}
