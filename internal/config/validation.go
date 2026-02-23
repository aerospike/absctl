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
	"fmt"

	"github.com/aerospike/absctl/internal/models"
)

//nolint:gocyclo // It is a long validation function.
func ValidateStorages(
	isBackup bool,
	awsS3 *models.AwsS3,
	gcpStorage *models.GcpStorage,
	azureBlob *models.AzureBlob,
	local *models.Local,
) error {
	// TODO: think how to rework this func. I want to get rid of it.
	var count int

	if local != nil {
		if err := local.Validate(isBackup); err != nil {
			return fmt.Errorf("failed to validate local storage: %w", err)
		}
	}

	if awsS3 != nil && (awsS3.BucketName != "" || awsS3.Region != "" || awsS3.Profile != "" || awsS3.Endpoint != "") {
		if err := awsS3.Validate(isBackup); err != nil {
			return fmt.Errorf("failed to validate aws s3: %w", err)
		}

		count++
	}

	if gcpStorage != nil && (gcpStorage.BucketName != "" || gcpStorage.KeyFile != "" || gcpStorage.Endpoint != "") {
		if err := gcpStorage.Validate(isBackup); err != nil {
			return fmt.Errorf("failed to validate gcp storage: %w", err)
		}

		count++
	}

	if azureBlob != nil && (azureBlob.ContainerName != "" || azureBlob.AccountName != "" || azureBlob.AccountKey != "" ||
		azureBlob.Endpoint != "" || azureBlob.TenantID != "" || azureBlob.ClientID != "" ||
		azureBlob.ClientSecret != "") {
		if err := azureBlob.Validate(isBackup); err != nil {
			return fmt.Errorf("failed to validate azure blob: %w", err)
		}

		count++
	}

	if count > 1 {
		return fmt.Errorf("only one cloud provider can be configured")
	}

	return nil
}
