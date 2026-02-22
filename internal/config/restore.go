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
	"log/slog"
	"runtime"
	"strings"

	"github.com/aerospike/absctl/internal/models"
	"github.com/aerospike/backup-go"
	"github.com/aerospike/tools-common-go/client"
)

// RestoreServiceConfig contains configuration settings for the restore service,
// including client, restore, and storage details.
type RestoreServiceConfig struct {
	App          *models.App
	ClientConfig *client.AerospikeConfig
	ClientPolicy *models.ClientPolicy
	Restore      *models.Restore
	Compression  *models.Compression
	Encryption   *models.Encryption
	SecretAgent  *models.SecretAgent
	AwsS3        *models.AwsS3
	GcpStorage   *models.GcpStorage
	AzureBlob    *models.AzureBlob
}

// NewRestoreServiceConfig creates and returns a new RestoreServiceConfig initialized with the provided parameters.
// If a config file path is specified in the app, parameters are loaded from the file instead.
// Returns an error if the config file cannot be loaded or parsed.
func NewRestoreServiceConfig(
	app *models.App,
	clientConfig *client.AerospikeConfig,
	clientPolicy *models.ClientPolicy,
	restore *models.Restore,
	compression *models.Compression,
	encryption *models.Encryption,
	secretAgent *models.SecretAgent,
	awsS3 *models.AwsS3,
	gcpStorage *models.GcpStorage,
	azureBlob *models.AzureBlob,
) (*RestoreServiceConfig, error) {
	return &RestoreServiceConfig{
		App:          app,
		ClientConfig: clientConfig,
		ClientPolicy: clientPolicy,
		Restore:      restore,
		Compression:  compression,
		Encryption:   encryption,
		SecretAgent:  secretAgent,
		AwsS3:        awsS3,
		GcpStorage:   gcpStorage,
		AzureBlob:    azureBlob,
	}, nil
}

// IsStdin checks if the restore operation should read from stdin
// by verifying that Restore is non-nil and InputFile is StdPlaceholder.
func (r *RestoreServiceConfig) IsStdin() bool {
	if r.Restore != nil && r.Restore.InputFile == StdPlaceholder {
		return true
	}

	return false
}

// NewRestoreConfig creates and returns a new ConfigRestore object, initialized with given restore parameters.
func NewRestoreConfig(config *RestoreServiceConfig, logger *slog.Logger) *backup.ConfigRestore {
	logger.Info("initializing restore config")

	parallel := runtime.NumCPU()
	if config.Restore.Parallel > 0 {
		parallel = config.Restore.Parallel
	}

	c := backup.NewDefaultRestoreConfig()
	c.Namespace = newRestoreNamespace(config.Restore.Namespace)
	c.SetList = SplitByComma(config.Restore.SetList)
	c.BinList = SplitByComma(config.Restore.BinList)
	c.NoRecords = config.Restore.NoRecords
	c.NoIndexes = config.Restore.NoIndexes
	c.NoUDFs = config.Restore.NoUDFs
	c.RecordsPerSecond = config.Restore.RecordsPerSecond
	c.Parallel = parallel
	c.WritePolicy = newWritePolicy(config.Restore)
	// As we set --bandwidth in MiB we must convert it to bytes
	c.Bandwidth = config.Restore.Bandwidth * 1024 * 1024
	c.ExtraTTL = config.Restore.ExtraTTL
	c.IgnoreRecordError = config.Restore.IgnoreRecordError
	c.DisableBatchWrites = config.Restore.DisableBatchWrites
	c.BatchSize = config.Restore.BatchSize
	c.MaxAsyncBatches = config.Restore.MaxAsyncBatches
	c.MetricsEnabled = true

	c.CompressionPolicy = config.Compression.ToPolicy()
	c.EncryptionPolicy = config.Encryption.ToPolicy()
	c.SecretAgentConfig = config.SecretAgent.ToConfig()
	c.RetryPolicy = NewRetryPolicy(
		config.Restore.RetryBaseInterval,
		config.Restore.RetryMultiplier,
		config.Restore.RetryMaxAttempts,
	)
	c.ValidateOnly = config.Restore.ValidateOnly
	c.ApplyMetadataLast = config.Restore.ApplyMetadataLast

	if !c.ValidateOnly {
		logRestoreConfig(logger, config, c)
	}

	return c
}

func logRestoreConfig(logger *slog.Logger, params *RestoreServiceConfig, restoreConfig *backup.ConfigRestore) {
	logger.Info("initialized restore config",
		getNamespaceLog(restoreConfig),
		getEncryptionLog(params.Encryption),
		getCompressionLog(params.Compression),
		slog.Any("retry", *restoreConfig.RetryPolicy),
		slog.Any("sets", restoreConfig.SetList),
		slog.Any("bins", restoreConfig.BinList),
		slog.Int("parallel", restoreConfig.Parallel),
		slog.Int64("bandwidth", restoreConfig.Bandwidth),
		slog.Bool("no_records", restoreConfig.NoRecords),
		slog.Bool("no_indexes", restoreConfig.NoIndexes),
		slog.Bool("no_udfs", restoreConfig.NoUDFs),
		slog.Bool("disable_batch_writes", restoreConfig.DisableBatchWrites),
		slog.Int("batch_size", restoreConfig.BatchSize),
		slog.Int("max_asynx_batches", restoreConfig.MaxAsyncBatches),
		slog.Int64("extra_ttl", restoreConfig.ExtraTTL),
		slog.Bool("ignore_records_error", restoreConfig.IgnoreRecordError),
	)
}

func getEncryptionLog(e *models.Encryption) slog.Attr {
	if e == nil {
		return slog.String("encryption", noneVal)
	}

	if e.Mode == "" || strings.EqualFold(e.Mode, noneVal) {
		return slog.String("encryption", noneVal)
	}

	return slog.String("encryption", e.Mode)
}

func getCompressionLog(c *models.Compression) slog.Attr {
	if c == nil {
		return slog.String("compression", noneVal)
	}
	// Separated nil check and value check for easy read.
	if c.Mode == "" || strings.EqualFold(c.Mode, noneVal) {
		return slog.String("compression", noneVal)
	}

	return slog.Group("compression",
		slog.String("mode", c.Mode), slog.Int("level", c.Level),
	)
}

func getNamespaceLog(r *backup.ConfigRestore) slog.Attr {
	if r.Namespace == nil {
		// It can't happen, but just in case. Not to catch nil pointer.
		return slog.String("namespace", noneVal)
	}

	var source, destination string
	if r.Namespace.Source != nil {
		source = *r.Namespace.Source
	}

	if r.Namespace.Destination != nil {
		destination = *r.Namespace.Destination
	}

	return slog.Group("namespace",
		slog.String("source", source), slog.String("destination", destination),
	)
}
