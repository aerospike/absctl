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
	"github.com/aerospike/tools-common-go/client"
)

// ServiceConfigCommon is the common configuration for all services.
type ServiceConfigCommon struct {
	App          *models.App
	ClientConfig *client.AerospikeConfig
	ClientPolicy *models.ClientPolicy
	Compression  *models.Compression
	Encryption   *models.Encryption
	SecretAgent  *models.SecretAgent
	AwsS3        *models.AwsS3
	GcpStorage   *models.GcpStorage
	AzureBlob    *models.AzureBlob
	Local        *models.Local
}

// NewServiceConfigCommon initializes and returns a ServiceConfigCommon
// object with the provided configuration parameters.
func NewServiceConfigCommon(
	app *models.App,
	clientConfig *client.AerospikeConfig,
	clientPolicy *models.ClientPolicy,
	compression *models.Compression,
	encryption *models.Encryption,
	secretAgent *models.SecretAgent,
	awsS3 *models.AwsS3,
	gcpStorage *models.GcpStorage,
	azureBlob *models.AzureBlob,
	local *models.Local,
) *ServiceConfigCommon {
	return &ServiceConfigCommon{
		App:          app,
		ClientConfig: clientConfig,
		ClientPolicy: clientPolicy,
		Compression:  compression,
		Encryption:   encryption,
		SecretAgent:  secretAgent,
		AwsS3:        awsS3,
		GcpStorage:   gcpStorage,
		AzureBlob:    azureBlob,
		Local:        local,
	}
}

// GetApp returns the App configuration.
func (r *ServiceConfigCommon) GetApp() *models.App {
	return r.App
}

// Validate validates the backup configuration and returns an error if any validation fails.
func (r *ServiceConfigCommon) Validate(isBackup bool) error {
	if err := validateStorages(
		isBackup,
		r.AwsS3,
		r.GcpStorage,
		r.AzureBlob,
		r.Local,
	); err != nil {
		return err
	}

	if err := r.SecretAgent.Validate(); err != nil {
		return err
	}

	return nil
}

// validateStorages performs storages validation.
// As we allow only one cloud provider to be configured, we can check it here.
func validateStorages(
	isBackup bool,
	awsS3 *models.AwsS3,
	gcpStorage *models.GcpStorage,
	azureBlob *models.AzureBlob,
	local *models.Local,
) error {
	var count int

	if local != nil {
		if err := local.Validate(isBackup); err != nil {
			return fmt.Errorf("failed to validate local storage: %w", err)
		}
	}

	if awsS3.IsConfigured() {
		if err := awsS3.Validate(isBackup); err != nil {
			return fmt.Errorf("failed to validate aws s3: %w", err)
		}

		count++
	}

	if gcpStorage.IsConfigured() {
		if err := gcpStorage.Validate(isBackup); err != nil {
			return fmt.Errorf("failed to validate gcp storage: %w", err)
		}

		count++
	}

	if azureBlob.IsConfigured() {
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
