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
	"github.com/aerospike/absctl/internal/models"
	"github.com/aerospike/tools-common-go/client"
)

// ServerRestoreServiceConfig holds the configuration for the server-integrated Start service.
type ServerRestoreServiceConfig struct {
	Start        *models.ServerRestore
	Prepare      *models.ServerRestorePrepare
	Progress     *models.ServerRestoreProgress
	Abort        *models.ServerRestoreAbort
	Open         *models.ServerRestoreOpen
	ClearStorage *models.ServerRestoreClearStorage

	ServiceConfigCommon
}

// ServerRestoreCommands holds the configurations of the snapshot-restore subcommands. A
// command sets only its own one, so the others are left out by name.
type ServerRestoreCommands struct {
	Start        *models.ServerRestore
	Prepare      *models.ServerRestorePrepare
	Progress     *models.ServerRestoreProgress
	Abort        *models.ServerRestoreAbort
	Open         *models.ServerRestoreOpen
	ClearStorage *models.ServerRestoreClearStorage
}

// NewServerRestoreServiceConfig initializes a new ServerRestoreServiceConfig.
func NewServerRestoreServiceConfig(
	commands ServerRestoreCommands,
	app *models.App,
	clientConfig *client.AerospikeConfig,
	clientPolicy *models.ClientPolicy,
	secretAgent *models.SecretAgent,
	awsS3 *models.AwsS3,
) *ServerRestoreServiceConfig {
	return &ServerRestoreServiceConfig{
		Start:        commands.Start,
		Prepare:      commands.Prepare,
		Progress:     commands.Progress,
		Abort:        commands.Abort,
		Open:         commands.Open,
		ClearStorage: commands.ClearStorage,
		App:          app,
		ClientConfig: clientConfig,
		ClientPolicy: clientPolicy,
		SecretAgent:  secretAgent,
		AwsS3:        awsS3,
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

	if err := s.Abort.Validate(); err != nil {
		return err
	}

	if err := s.Open.Validate(); err != nil {
		return err
	}

	if err := s.ClearStorage.Validate(); err != nil {
		return err
	}

	// Only "snapshot-restore start" talks to object storage; the other subcommands
	// are served by the cluster alone.
	if s.Start != nil {
		if _, err := s.ObjectStorageType(); err != nil {
			return err
		}
	}

	if err := s.ServiceConfigCommon.Validate(isBackup); err != nil {
		return err
	}

	return nil
}
