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

package config

import (
	"fmt"

	"github.com/aerospike/absctl/internal/models"
	"github.com/aerospike/tools-common-go/client"
)

// ServerBackupServiceConfig holds the configuration for the server-integrated Start service.
type ServerBackupServiceConfig struct {
	Start      *models.ServerBackup
	List       *models.ServerBackupList
	Validation *models.ServerBackupValidate
	Progress   *models.ServerBackupProgress

	ServiceConfigCommon
}

// NewServerBackupServiceConfig initializes a new ServerBackupServiceConfig
// using the provided parameters for backup service configuration.
func NewServerBackupServiceConfig(
	start *models.ServerBackup,
	list *models.ServerBackupList,
	validation *models.ServerBackupValidate,
	progress *models.ServerBackupProgress,
	app *models.App,
	clientConfig *client.AerospikeConfig,
	clientPolicy *models.ClientPolicy,
	secretAgent *models.SecretAgent,
	awsS3 *models.AwsS3,
) *ServerBackupServiceConfig {
	return &ServerBackupServiceConfig{
		Start:        start,
		List:         list,
		Validation:   validation,
		Progress:     progress,
		App:          app,
		ClientConfig: clientConfig,
		ClientPolicy: clientPolicy,
		SecretAgent:  secretAgent,
		AwsS3:        awsS3,
	}
}

// Validate checks if the ServerBackupServiceConfig and its embedded ServiceConfigCommon are correctly configured.
func (s *ServerBackupServiceConfig) Validate(isBackup bool) error {
	if err := s.Start.Validate(); err != nil {
		return err
	}

	if err := s.List.Validate(); err != nil {
		return err
	}

	if err := s.Validation.Validate(); err != nil {
		return err
	}

	if err := s.Progress.Validate(); err != nil {
		return err
	}

	// Every snapshot-backup subcommand reads from or writes to object storage.
	if err := validateServerObjectStorage(s.AwsS3); err != nil {
		return err
	}

	if err := s.ServiceConfigCommon.Validate(isBackup); err != nil {
		return err
	}

	return nil
}

// validateServerObjectStorage makes sure the object storage of a
// server-integrated command is addressable. An empty S3 section would otherwise
// pass silently: AwsS3.IsConfigured reports false, validateStorages skips it,
// and the command only fails later against an empty bucket name.
func validateServerObjectStorage(awsS3 *models.AwsS3) error {
	if awsS3 == nil || awsS3.BucketName == "" {
		return fmt.Errorf("s3-bucket-name is required")
	}

	return nil
}
