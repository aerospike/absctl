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
	"github.com/aerospike/absctl/internal/models"
	"github.com/aerospike/tools-common-go/client"
)

// IntegratedServiceConfig holds the configuration for the server-integrated Backup (SSB) service.
type IntegratedServiceConfig struct {
	IntegratedBackup *models.IntegratedBackup

	ServiceConfigCommon
}

// NewIntegratedServiceConfig initializes a new IntegratedServiceConfig
// using the provided parameters for backup service configuration.
func NewIntegratedServiceConfig(
	integratedBackup *models.IntegratedBackup,
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
) *IntegratedServiceConfig {
	return &IntegratedServiceConfig{
		IntegratedBackup: integratedBackup,
		ServiceConfigCommon: ServiceConfigCommon{
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
		},
	}
}

// Validate checks if the IntegratedServiceConfig and its embedded ServiceConfigCommon are correctly configured.
func (s *IntegratedServiceConfig) Validate(isBackup bool) error {
	if err := s.ServiceConfigCommon.Validate(isBackup); err != nil {
		return err
	}

	return nil
}
