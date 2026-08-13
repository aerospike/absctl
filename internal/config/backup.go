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
	"log/slog"
	"path"

	"github.com/aerospike/absctl/internal/models"
	"github.com/aerospike/backup-go"
	"github.com/aerospike/tools-common-go/client"
)

const (
	// MaxRack max number of racks that can exist.
	MaxRack = 1000000
	// StdPlaceholder is the placeholder for stdout file name.
	StdPlaceholder = "-"

	// bytesInMiB is used to convert MiB flag values to bytes.
	bytesInMiB = 1024 * 1024
)

// BackupServiceConfig represents the configuration structure for the backup service
// involving various policies and integrations.
type BackupServiceConfig struct {
	Backup *models.Backup

	ServiceConfigCommon
}

// NewBackupServiceConfig initializes and returns a BackupServiceConfig struct
// with the provided configuration components.
func NewBackupServiceConfig(
	app *models.App,
	clientConfig *client.AerospikeConfig,
	clientPolicy *models.ClientPolicy,
	backupScan *models.Backup,
	compression *models.Compression,
	encryption *models.Encryption,
	secretAgent *models.SecretAgent,
	awsS3 *models.AwsS3,
	gcpStorage *models.GcpStorage,
	azureBlob *models.AzureBlob,
	local *models.Local,
) *BackupServiceConfig {
	return &BackupServiceConfig{
		Backup: backupScan,
		ServiceConfigCommon: *NewServiceConfigCommon(
			app,
			clientConfig,
			clientPolicy,
			compression,
			encryption,
			secretAgent,
			awsS3,
			gcpStorage,
			azureBlob,
			local,
		),
	}
}

// IsContinue determines if the backup configuration is a continue backup
// by checking if Backup is non-nil and Continue is non-empty.
func (b *BackupServiceConfig) IsContinue() bool {
	return b.Backup != nil && b.Backup.Continue != ""
}

// ShouldInitWriter reports whether the backup writer must be initialized.
// The writer is not needed for estimate runs.
func (b *BackupServiceConfig) ShouldInitWriter() bool {
	if b.Backup == nil {
		return true
	}

	return !b.Backup.Estimate
}

// IsStdout checks if the backup operation should write to stdout
// by verifying that Backup is non-nil and OutputFile is StdPlaceholder.
func (b *BackupServiceConfig) IsStdout() bool {
	return b.Backup != nil && b.Backup.OutputFile == StdPlaceholder
}

// Validate validates the backup configuration and returns an error if any validation fails.
func (b *BackupServiceConfig) Validate() error {
	if err := b.Backup.Validate(); err != nil {
		return err
	}

	if err := b.ServiceConfigCommon.Validate(true); err != nil {
		return err
	}

	return nil
}

// NewBackupConfig creates and returns a new ConfigBackup object
// initialized with the given backup parameters.
// It sets namespace, file limits, parallelism options, bandwidth,
// compression, encryption, and partition filters.
// It returns an error if any validation or parsing fails.
func NewBackupConfig(serviceConfig *BackupServiceConfig, logger *slog.Logger,
) (*backup.ConfigBackup, error) {
	logger.Info("initializing backup config")

	backupConfig, err := newBackupConfig(serviceConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to map backup config: %w", err)
	}

	logBackupConfig(logger, serviceConfig, backupConfig)

	return backupConfig, nil
}

// newBackupConfig initializes and returns a configured instance of ConfigBackup based on the provided params.
func newBackupConfig(config *BackupServiceConfig) (*backup.ConfigBackup, error) {
	c := backup.NewDefaultBackupConfig()
	c.Namespace = config.Backup.Namespace
	c.SetList = config.Backup.Sets()
	c.BinList = config.Backup.Bins()
	c.NoRecords = config.Backup.NoRecords
	c.NoIndexes = config.Backup.NoIndexes
	c.RecordsPerSecond = config.Backup.RecordsPerSecond
	c.FileLimit = config.Backup.FileLimit * bytesInMiB
	c.NoUDFs = config.Backup.NoUDFs
	// The original backup tools have a single parallelism configuration property.
	// We may consider splitting the configuration in the future.
	c.ParallelWrite = config.Backup.Parallel
	c.ParallelRead = config.Backup.Parallel
	// As we set --bandwidth in MiB we must convert it to bytes.
	c.Bandwidth = config.Backup.Bandwidth * bytesInMiB
	c.Compact = config.Backup.Compact
	c.NoTTLOnly = config.Backup.NoTTLOnly
	c.OutputFilePrefix = config.Backup.OutputFilePrefix
	c.MetricsEnabled = true

	// Reconfigure params for stdout or single file backup.
	if config.IsStdout() || config.Backup.OutputFile != "" {
		// If we back up to stdout, file limit can break the output stream
		// because it will write file headers and close descriptors,
		// so the file limit is disabled.
		c.FileLimit = 0
		// Parallel write must be 1 for stdout, otherwise records data will be mixed and corrupted.
		c.ParallelWrite = 1
	}

	if config.Backup.RackList != "" {
		list, err := config.Backup.Racks()
		if err != nil {
			return nil, err
		}

		c.RackList = list
	}

	if config.Backup.Continue != "" {
		c.StateFile = path.Join(config.Backup.Directory, config.Backup.Continue)
		c.Continue = true
		c.PageSize = config.Backup.ScanPageSize
	}

	if config.Backup.StateFileDst != "" {
		c.StateFile = path.Join(config.Backup.Directory, config.Backup.StateFileDst)
		c.PageSize = config.Backup.ScanPageSize
	}

	// Overwrite partitions if we use nodes.
	if config.Backup.NodeList != "" {
		c.NodeList = config.Backup.Nodes()
	}

	pf, err := config.Backup.PartitionFilters()
	if err != nil {
		return nil, err
	}

	c.PartitionFilters = pf

	sp, err := config.Backup.ScanPolicy()
	if err != nil {
		return nil, err
	}

	c.ScanPolicy = sp
	c.CompressionPolicy = config.Compression.Policy()
	c.EncryptionPolicy = config.Encryption.Policy()
	c.SecretAgentConfig = config.SecretAgent.Config()

	if config.Backup.ModifiedBefore != "" {
		modBeforeTime, err := config.Backup.ModifiedBeforeTime()
		if err != nil {
			return nil, fmt.Errorf("failed to parse modified before date: %w", err)
		}

		c.ModBefore = &modBeforeTime
	}

	if config.Backup.ModifiedAfter != "" {
		modAfterTime, err := config.Backup.ModifiedAfterTime()
		if err != nil {
			return nil, fmt.Errorf("failed to parse modified after date: %w", err)
		}

		c.ModAfter = &modAfterTime
	}

	return c, nil
}

func logBackupConfig(logger *slog.Logger, params *BackupServiceConfig, backupConfig *backup.ConfigBackup) {
	logger.Info("initialized scan backup config",
		slog.String("namespace", backupConfig.Namespace),
		getEncryptionLog(params.Encryption),
		getCompressionLog(params.Compression),
		slog.String("partition-list", params.Backup.PartitionList),
		slog.Any("node-list", backupConfig.NodeList),
		slog.Any("set-list", backupConfig.SetList),
		slog.Any("bin-list", backupConfig.BinList),
		slog.Any("rack-list", backupConfig.RackList),
		// ParallelRead and ParallelWrite are the same for scan backup.
		slog.Any("parallel", backupConfig.ParallelRead),
		slog.Bool("no-records", backupConfig.NoRecords),
		slog.Bool("no-indexes", backupConfig.NoIndexes),
		slog.Bool("no-udfs", backupConfig.NoUDFs),
		slog.Int("records-per-second", backupConfig.RecordsPerSecond),
		slog.Int64("bandwidth", backupConfig.Bandwidth),
		slog.Uint64("file-limit", backupConfig.FileLimit),
		slog.Bool("compact", backupConfig.Compact),
		slog.Bool("no-ttl-only", backupConfig.NoTTLOnly),
		slog.String("state-file-dst", backupConfig.StateFile),
		slog.Bool("continue", backupConfig.Continue),
		slog.Int64("scan-page-size", backupConfig.PageSize),
		slog.String("output-file-prefix", backupConfig.OutputFilePrefix),
	)
}
