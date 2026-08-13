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

// ServerRestoreServiceConfig holds the configuration for the server-integrated Start service.
type ServerRestoreServiceConfig struct {
	Start    *models.ServerRestore
	Prepare  *models.ServerRestorePrepare
	Progress *models.ServerRestoreProgress

	ServiceConfigCommon
}

// NewServerRestoreServiceConfig initializes a new ServerRestoreServiceConfig.
func NewServerRestoreServiceConfig(
	start *models.ServerRestore,
	prepare *models.ServerRestorePrepare,
	progress *models.ServerRestoreProgress,
	app *models.App,
	clientConfig *client.AerospikeConfig,
	clientPolicy *models.ClientPolicy,
	secretAgent *models.SecretAgent,
	awsS3 *models.AwsS3,
) *ServerRestoreServiceConfig {
	return &ServerRestoreServiceConfig{
		Start:    start,
		Prepare:  prepare,
		Progress: progress,
		ServiceConfigCommon: ServiceConfigCommon{
			App:          app,
			ClientConfig: clientConfig,
			ClientPolicy: clientPolicy,
			SecretAgent:  secretAgent,
			AwsS3:        awsS3,
		},
	}
}

// Validate checks if the ServerBackupServiceConfig and its embedded ServiceConfigCommon are correctly configured.
func (s *ServerRestoreServiceConfig) Validate(isBackup bool) error {
	if err := s.Start.Validate(); err != nil {
		return err
	}

	if err := s.Prepare.Validate(); err != nil {
		return err
	}

	if err := s.Progress.Validate(); err != nil {
		return err
	}

	if err := s.ServiceConfigCommon.Validate(isBackup); err != nil {
		return err
	}

	return nil
}
