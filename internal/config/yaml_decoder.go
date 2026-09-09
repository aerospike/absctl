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
	"context"
	"fmt"
	"os"

	"github.com/aerospike/absctl/internal/config/dto"
	"github.com/aerospike/absctl/internal/models"
	"gopkg.in/yaml.v3"
)

// DecodeBackupServiceConfig reads a backup configuration file and decodes it into BackupServiceConfig.
// Secret agent references (secrets:resource:key) in the YAML are resolved before conversion.
// Returns an error on failure.
func DecodeBackupServiceConfig(ctx context.Context, filename string) (*BackupServiceConfig, error) {
	backupDto := dto.DefaultBackup()
	if err := decodeFromFile(filename, &backupDto); err != nil {
		return nil, err
	}

	if err := backupDto.LoadSecrets(ctx); err != nil {
		return nil, fmt.Errorf("failed to resolve secrets: %w", err)
	}

	serviceConfig, err := dtoToBackupServiceConfig(backupDto)
	if err != nil {
		return nil, err
	}

	return serviceConfig, nil
}

func dtoToBackupServiceConfig(dtoBackup *dto.Backup) (*BackupServiceConfig, error) {
	if dtoBackup == nil {
		return nil, fmt.Errorf("dto is nil")
	}

	asConfig, err := dtoBackup.Cluster.ToAerospikeConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to map to aerospike config: %w", err)
	}

	return &BackupServiceConfig{
		Backup:       dtoBackup.ToModelBackup(),
		App:          dtoBackup.App.ToModelApp(),
		ClientConfig: asConfig,
		ClientPolicy: dtoBackup.Cluster.ToModelClientPolicy(),
		Compression:  dtoBackup.Compression.ToModelCompression(),
		Encryption:   dtoBackup.Encryption.ToModelEncryption(),
		SecretAgent:  dtoBackup.SecretAgent.ToModelSecretAgent(),
		AwsS3:        dtoBackup.Aws.S3.ToModelAwsS3(),
		GcpStorage:   dtoBackup.Gcp.Storage.ToModelGcpStorage(),
		AzureBlob:    dtoBackup.Azure.Blob.ToModelAzureBlob(),
		Local:        dtoBackup.Local.Disk.ToModelLocal(),
	}, nil
}

// DecodeRestoreServiceConfig reads a restore configuration file and decodes it into RestoreServiceConfig.
// Secret agent references (secrets:resource:key) in the YAML are resolved before conversion.
// Returns an error on failure.
func DecodeRestoreServiceConfig(ctx context.Context, filename string) (*RestoreServiceConfig, error) {
	restoreDto := dto.DefaultRestore()
	if err := decodeFromFile(filename, restoreDto); err != nil {
		return nil, err
	}

	if err := restoreDto.LoadSecrets(ctx); err != nil {
		return nil, fmt.Errorf("failed to resolve secrets: %w", err)
	}

	serviceConfig, err := dtoToRestoreServiceConfig(restoreDto)
	if err != nil {
		return nil, err
	}

	return serviceConfig, nil
}

func dtoToRestoreServiceConfig(dtoRestore *dto.Restore) (*RestoreServiceConfig, error) {
	if dtoRestore == nil {
		return nil, fmt.Errorf("dto is nil")
	}

	asConfig, err := dtoRestore.Cluster.ToAerospikeConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to map to aerospike config: %w", err)
	}

	return &RestoreServiceConfig{
		Restore:      dtoRestore.ToModelRestore(),
		App:          dtoRestore.App.ToModelApp(),
		ClientConfig: asConfig,
		ClientPolicy: dtoRestore.Cluster.ToModelClientPolicy(),
		Compression:  dtoRestore.Compression.ToModelCompression(),
		Encryption:   dtoRestore.Encryption.ToModelEncryption(),
		SecretAgent:  dtoRestore.SecretAgent.ToModelSecretAgent(),
		AwsS3:        dtoRestore.Aws.S3.ToModelAwsS3(),
		GcpStorage:   dtoRestore.Gcp.Storage.ToModelGcpStorage(),
		AzureBlob:    dtoRestore.Azure.Blob.ToModelAzureBlob(),
	}, nil
}

// ServerBackupCommand identifies which snapshot-backup subcommand a decoded
// config is built for. One config file describes the whole command tree, so
// only the section belonging to the invoked subcommand is populated — the rest
// stay nil and are skipped by ServerBackupServiceConfig.Validate.
type ServerBackupCommand int

const (
	ServerBackupCommandStart ServerBackupCommand = iota
	ServerBackupCommandList
	ServerBackupCommandValidate
	ServerBackupCommandProgress
)

// DecodeServerBackupServiceConfig reads a snapshot-backup configuration file and
// decodes it into ServerBackupServiceConfig for the given subcommand.
// Secret agent references (secrets:resource:key) in the YAML are resolved before conversion.
func DecodeServerBackupServiceConfig(
	ctx context.Context, filename string, command ServerBackupCommand,
) (*ServerBackupServiceConfig, error) {
	backupDto := dto.DefaultServerBackup()
	if err := decodeFromFile(filename, backupDto); err != nil {
		return nil, err
	}

	if err := backupDto.LoadSecrets(ctx); err != nil {
		return nil, fmt.Errorf("failed to resolve secrets: %w", err)
	}

	return dtoToServerBackupServiceConfig(backupDto, command)
}

func dtoToServerBackupServiceConfig(
	dtoBackup *dto.ServerBackup, command ServerBackupCommand,
) (*ServerBackupServiceConfig, error) {
	if dtoBackup == nil {
		return nil, fmt.Errorf("dto is nil")
	}

	asConfig, err := dtoBackup.Cluster.ToAerospikeConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to map to aerospike config: %w", err)
	}

	cfg := &ServerBackupServiceConfig{
		App:          dtoBackup.App.ToModelApp(),
		ClientConfig: asConfig,
		ClientPolicy: dtoBackup.Cluster.ToModelClientPolicy(),
		SecretAgent:  dtoBackup.SecretAgent.ToModelSecretAgent(),
		AwsS3:        dtoBackup.Aws.S3.ToModelAwsS3(),
	}

	switch command {
	case ServerBackupCommandStart:
		cfg.Start = dtoBackup.ToModelServerBackup()
	case ServerBackupCommandList:
		cfg.List = dtoBackup.ToModelServerBackupList()
	case ServerBackupCommandValidate:
		cfg.Validation = dtoBackup.ToModelServerBackupValidate()
	case ServerBackupCommandProgress:
		cfg.Progress = dtoBackup.ToModelServerBackupProgress()
	default:
		return nil, fmt.Errorf("unknown snapshot-backup command %d", command)
	}

	return cfg, nil
}

// ServerRestoreCommand identifies which snapshot-restore subcommand a decoded
// config is built for. See ServerBackupCommand.
type ServerRestoreCommand int

const (
	ServerRestoreCommandStart ServerRestoreCommand = iota
	ServerRestoreCommandPrepare
	ServerRestoreCommandProgress
)

// DecodeServerRestoreServiceConfig reads a snapshot-restore configuration file and
// decodes it into ServerRestoreServiceConfig for the given subcommand.
// Secret agent references (secrets:resource:key) in the YAML are resolved before conversion.
func DecodeServerRestoreServiceConfig(
	ctx context.Context, filename string, command ServerRestoreCommand,
) (*ServerRestoreServiceConfig, error) {
	restoreDto := dto.DefaultServerRestore()
	if err := decodeFromFile(filename, restoreDto); err != nil {
		return nil, err
	}

	if err := restoreDto.LoadSecrets(ctx); err != nil {
		return nil, fmt.Errorf("failed to resolve secrets: %w", err)
	}

	return dtoToServerRestoreServiceConfig(restoreDto, command)
}

func dtoToServerRestoreServiceConfig(
	dtoRestore *dto.ServerRestore, command ServerRestoreCommand,
) (*ServerRestoreServiceConfig, error) {
	if dtoRestore == nil {
		return nil, fmt.Errorf("dto is nil")
	}

	asConfig, err := dtoRestore.Cluster.ToAerospikeConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to map to aerospike config: %w", err)
	}

	cfg := &ServerRestoreServiceConfig{
		App:          dtoRestore.App.ToModelApp(),
		ClientConfig: asConfig,
		ClientPolicy: dtoRestore.Cluster.ToModelClientPolicy(),
		SecretAgent:  dtoRestore.SecretAgent.ToModelSecretAgent(),
		AwsS3:        dtoRestore.Aws.S3.ToModelAwsS3(),
	}

	switch command {
	case ServerRestoreCommandStart:
		cfg.Start = dtoRestore.ToModelServerRestore()
	case ServerRestoreCommandPrepare:
		cfg.Prepare = dtoRestore.ToModelServerRestorePrepare()
	case ServerRestoreCommandProgress:
		cfg.Progress = dtoRestore.ToModelServerRestoreProgress()
	default:
		return nil, fmt.Errorf("unknown snapshot-restore command %d", command)
	}

	return cfg, nil
}

// DecodeAppConfig reads only the "app" section of a configuration file.
//
// The server command tree initializes its logger in PersistentPreRunE, before
// the subcommand knows which schema to decode, so the app settings are read
// separately here. Unknown fields are tolerated on purpose: every other section
// of the file is irrelevant to this call and is validated by the full decode.
func DecodeAppConfig(filename string) (*models.App, error) {
	if filename == "" {
		return nil, fmt.Errorf("config path is empty")
	}

	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open config file %s: %w", filename, err)
	}

	defer func() {
		_ = file.Close()
	}()

	appDto := struct {
		App dto.App `yaml:"app"`
	}{App: dto.DefaultAppDTO()}

	if err := yaml.NewDecoder(file).Decode(&appDto); err != nil {
		return nil, fmt.Errorf("failed to decode config file %s: %w", filename, err)
	}

	return appDto.App.ToModelApp(), nil
}

// decodeFromFile decode yaml to params.
func decodeFromFile(filename string, params any) error {
	if filename == "" {
		return fmt.Errorf("config path is empty")
	}

	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("failed to open config file %s: %w", filename, err)
	}

	defer func() {
		_ = file.Close()
	}()

	yamlDec := yaml.NewDecoder(file)
	yamlDec.KnownFields(true)

	if err := yamlDec.Decode(params); err != nil {
		return fmt.Errorf("failed to decode config file %s: %w", filename, err)
	}

	return nil
}

// DumpFile used for tests.
func DumpFile(filename string, params any) error {
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0o666)
	if err != nil {
		return fmt.Errorf("failed to open config file %s: %w", filename, err)
	}

	defer func() {
		_ = file.Close()
	}()

	yamlEnc := yaml.NewEncoder(file)
	if err := yamlEnc.Encode(params); err != nil {
		return fmt.Errorf("failed to encode config file %s: %w", filename, err)
	}

	return nil
}
