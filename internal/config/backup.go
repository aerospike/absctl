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
	"runtime"
	"time"

	"github.com/aerospike/absctl/internal/models"
	"github.com/aerospike/backup-go"
	"github.com/aerospike/tools-common-go/client"
)

const (
	// MaxRack max number of racks that can exist.
	MaxRack = 1000000
	// StdPlaceholder is the placeholder for stdout file name.
	StdPlaceholder = "-"
)

// BackupServiceConfig represents the configuration structure for the backup service
// involving various policies and integrations.
type BackupServiceConfig struct {
	App          *models.App
	ClientConfig *client.AerospikeConfig
	ClientPolicy *models.ClientPolicy
	Backup       *models.Backup
	BackupXDR    *models.BackupXDR
	Compression  *models.Compression
	Encryption   *models.Encryption
	SecretAgent  *models.SecretAgent
	AwsS3        *models.AwsS3
	GcpStorage   *models.GcpStorage
	AzureBlob    *models.AzureBlob
	Local        *models.Local
}

// NewBackupServiceConfig initializes and returns a BackupServiceConfig struct
// with the provided configuration components. It optionally loads configuration from a file
// if specified in the app.ConfigFilePath.
func NewBackupServiceConfig(
	app *models.App,
	clientConfig *client.AerospikeConfig,
	clientPolicy *models.ClientPolicy,
	backupScan *models.Backup,
	backupXDR *models.BackupXDR,
	compression *models.Compression,
	encryption *models.Encryption,
	secretAgent *models.SecretAgent,
	awsS3 *models.AwsS3,
	gcpStorage *models.GcpStorage,
	azureBlob *models.AzureBlob,
	local *models.Local,
) (*BackupServiceConfig, error) {
	return &BackupServiceConfig{
		App:          app,
		ClientConfig: clientConfig,
		ClientPolicy: clientPolicy,
		Backup:       backupScan,
		BackupXDR:    backupXDR,
		Compression:  compression,
		Encryption:   encryption,
		SecretAgent:  secretAgent,
		AwsS3:        awsS3,
		GcpStorage:   gcpStorage,
		AzureBlob:    azureBlob,
		Local:        local,
	}, nil
}

// IsXDR determines if the backup configuration is an XDR backup by checking if BackupXDR is non-nil and Backup is nil.
func (p *BackupServiceConfig) IsXDR() bool {
	return p.BackupXDR != nil && p.Backup == nil
}

// IsContinue determines if the backup configuration is a continue backup
// by checking if Backup is non-nil and Continue is non-empty.
func (p *BackupServiceConfig) IsContinue() bool {
	return p.Backup != nil && p.Backup.Continue != ""
}

// IsStopXDR checks if the backup operation should stop XDR by verifying that BackupXDR is non-nil and StopXDR is true.
func (p *BackupServiceConfig) IsStopXDR() bool {
	return p.BackupXDR != nil && p.BackupXDR.StopXDR
}

// IsUnblockMRT checks if the backup operation should unblock MRT writes
// by verifying that BackupXDR is non-nil and UnblockMRT is true.
func (p *BackupServiceConfig) IsUnblockMRT() bool {
	return p.BackupXDR != nil && p.BackupXDR.UnblockMRT
}

// SkipWriterInit checks if the backup operation should skip writer initialization
// by verifying that Backup is non-nil and Estimate is false.
func (p *BackupServiceConfig) SkipWriterInit() bool {
	if p.Backup != nil {
		return !p.Backup.Estimate
	}

	return true
}

// IsStdout checks if the backup operation should write to stdout
// by verifying that Backup is non-nil and OutputFile is StdPlaceholder.
func (p *BackupServiceConfig) IsStdout() bool {
	if p.Backup != nil && p.Backup.OutputFile == StdPlaceholder {
		return true
	}

	return false
}

// NewBackupConfigs creates and returns a new ConfigBackup and ConfigBackupXDR object,
// initialized with given backup parameters.
// This function sets various backup parameters including namespace, file limits, parallelism options, bandwidth,
// compression, encryption, and partition filters. It returns an error if any validation or parsing fails.
// If the backup is an XDR backup, it will return a ConfigBackupXDR object.
// Otherwise, it will return a ConfigBackup object.
func NewBackupConfigs(serviceConfig *BackupServiceConfig, logger *slog.Logger,
) (*backup.ConfigBackup, *backup.ConfigBackupXDR, error) {
	var (
		backupConfig    *backup.ConfigBackup
		backupXDRConfig *backup.ConfigBackupXDR
		err             error
	)

	logger.Info("initializing backup config")

	switch serviceConfig.IsXDR() {
	case false:
		backupConfig, err = newBackupConfig(serviceConfig)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to map backup config: %w", err)
		}

		logBackupConfig(logger, serviceConfig, backupConfig)
	case true:
		backupXDRConfig = newBackupXDRConfig(serviceConfig)

		// On xdr backup we backup only uds and indexes.
		backupConfig = backup.NewDefaultBackupConfig()

		backupConfig.NoRecords = true
		backupConfig.Namespace = backupXDRConfig.Namespace

		logXdrBackupConfig(logger, serviceConfig, backupXDRConfig)
	}

	return backupConfig, backupXDRConfig, nil
}

// newBackupConfig initializes and returns a configured instance of ConfigBackup based on the provided params.
// This function sets various backup parameters including namespace, file limits, parallelism options, bandwidth,
// compression, encryption, and partition filters. It returns an error if any validation or parsing fails.
func newBackupConfig(config *BackupServiceConfig) (*backup.ConfigBackup, error) {
	c := backup.NewDefaultBackupConfig()
	c.Namespace = config.Backup.Namespace
	c.SetList = config.Backup.Sets()
	c.BinList = config.Backup.Bins()
	c.NoRecords = config.Backup.NoRecords
	c.NoIndexes = config.Backup.NoIndexes
	c.RecordsPerSecond = config.Backup.RecordsPerSecond
	c.FileLimit = config.Backup.FileLimit * 1024 * 1024
	c.NoUDFs = config.Backup.NoUDFs
	// The original backup tools have a single parallelism configuration property.
	// We may consider splitting the configuration in the future.
	c.ParallelWrite = config.Backup.Parallel
	c.ParallelRead = config.Backup.Parallel
	// As we set --bandwidth in MiB we must convert it to bytes
	c.Bandwidth = config.Backup.Bandwidth * 1024 * 1024
	c.Compact = config.Backup.Compact
	c.NoTTLOnly = config.Backup.NoTTLOnly
	c.OutputFilePrefix = config.Backup.OutputFilePrefix
	c.MetricsEnabled = true

	// Reconfigure params for stdout or single file backup.
	if config.IsStdout() || config.Backup.OutputFile != "" {
		// If we back up to stdout, file limit can break the input stream because it will file headers and close descriptors.
		// So the file limit is disabled for stdout.
		c.FileLimit = 0
		// Parallel write can be only 1 to stdout, otherwise records data will be mixed and corrupted.
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

// newBackupXDRConfig creates a ConfigBackupXDR instance based on the provided backup parameters.
func newBackupXDRConfig(params *BackupServiceConfig) *backup.ConfigBackupXDR {
	parallelWrite := runtime.NumCPU()
	if params.BackupXDR.ParallelWrite > 0 {
		parallelWrite = params.BackupXDR.ParallelWrite
	}

	c := &backup.ConfigBackupXDR{
		EncryptionPolicy:  params.Encryption.Policy(),
		CompressionPolicy: params.Compression.Policy(),
		SecretAgentConfig: params.SecretAgent.Config(),
		EncoderType:       backup.EncoderTypeASBX,
		FileLimit:         params.BackupXDR.FileLimit * 1024 * 1024,
		ParallelWrite:     parallelWrite,
		DC:                params.BackupXDR.DC,
		LocalAddress:      params.BackupXDR.LocalAddress,
		LocalPort:         params.BackupXDR.LocalPort,
		Namespace:         params.BackupXDR.Namespace,
		Rewind:            params.BackupXDR.Rewind,
		TLSConfig:         nil,
		ReadTimeout:       time.Duration(params.BackupXDR.ReadTimeoutMilliseconds) * time.Millisecond,
		WriteTimeout:      time.Duration(params.BackupXDR.WriteTimeoutMilliseconds) * time.Millisecond,
		ResultQueueSize:   params.BackupXDR.ResultQueueSize,
		AckQueueSize:      params.BackupXDR.AckQueueSize,
		MaxConnections:    params.BackupXDR.MaxConnections,
		InfoPollingPeriod: time.Duration(params.BackupXDR.InfoPolingPeriodMilliseconds) * time.Millisecond,
		StartTimeout:      time.Duration(params.BackupXDR.StartTimeoutMilliseconds) * time.Millisecond,
		MaxThroughput:     params.BackupXDR.MaxThroughput,
		Forward:           params.BackupXDR.Forward,
		MetricsEnabled:    true,
	}

	return c
}

func logBackupConfig(logger *slog.Logger, params *BackupServiceConfig, backupConfig *backup.ConfigBackup) {
	logger.Info("initialized scan backup config",
		slog.String("namespace", backupConfig.Namespace),
		getEncryptionLog(params.Encryption),
		getCompressionLog(params.Compression),
		slog.String("filters", params.Backup.PartitionList),
		slog.Any("nodes", backupConfig.NodeList),
		slog.Any("sets", backupConfig.SetList),
		slog.Any("bins", backupConfig.BinList),
		slog.Any("rack", backupConfig.RackList),
		slog.Any("parallel_read", backupConfig.ParallelRead),
		slog.Any("parallel_write", backupConfig.ParallelWrite),
		slog.Bool("no_records", backupConfig.NoRecords),
		slog.Bool("no_indexes", backupConfig.NoIndexes),
		slog.Bool("no_udfs", backupConfig.NoUDFs),
		slog.Int("records_per_second", backupConfig.RecordsPerSecond),
		slog.Int64("bandwidth", backupConfig.Bandwidth),
		slog.Uint64("file_limit", backupConfig.FileLimit),
		slog.Bool("compact", backupConfig.Compact),
		slog.Bool("not_ttl_only", backupConfig.NoTTLOnly),
		slog.String("state_file", backupConfig.StateFile),
		slog.Bool("continue", backupConfig.Continue),
		slog.Int64("page_size", backupConfig.PageSize),
		slog.String("output_prefix", backupConfig.OutputFilePrefix),
	)
}

func logXdrBackupConfig(logger *slog.Logger, params *BackupServiceConfig, backupXDRConfig *backup.ConfigBackupXDR) {
	logger.Info("initialized xdr backup config",
		slog.String("namespace", backupXDRConfig.Namespace),
		getEncryptionLog(params.Encryption),
		getCompressionLog(params.Compression),
		slog.Any("parallel_write", backupXDRConfig.ParallelWrite),
		slog.Uint64("file_limit", backupXDRConfig.FileLimit),
		slog.String("dc", backupXDRConfig.DC),
		slog.String("local_address", backupXDRConfig.LocalAddress),
		slog.Int("local_port", backupXDRConfig.LocalPort),
		slog.String("rewind", backupXDRConfig.Rewind),
		slog.Int("max_throughput", backupXDRConfig.MaxThroughput),
		slog.Duration("read_timeout", backupXDRConfig.ReadTimeout),
		slog.Duration("write_timeout", backupXDRConfig.WriteTimeout),
		slog.Int("result_queue_size", backupXDRConfig.ResultQueueSize),
		slog.Int("ack_queue_size", backupXDRConfig.AckQueueSize),
		slog.Int("max_connections", backupXDRConfig.MaxConnections),
	)
}
