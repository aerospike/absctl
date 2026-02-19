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

package dto

import (
	"strings"

	"github.com/aerospike/absctl/internal/models"
)

// Restore is used to map yaml config.
type Restore struct {
	App         App           `yaml:"app"`
	Cluster     Cluster       `yaml:"cluster"`
	Restore     RestoreConfig `yaml:"restore"`
	Compression Compression   `yaml:"compression"`
	Encryption  Encryption    `yaml:"encryption"`
	SecretAgent SecretAgent   `yaml:"secret-agent"`
	Aws         struct {
		S3 AwsS3 `yaml:"s3"`
	} `yaml:"aws"`
	Gcp struct {
		Storage GcpStorage `yaml:"storage"`
	} `yaml:"gcp"`
	Azure struct {
		Blob AzureBlob `yaml:"blob"`
	} `yaml:"azure"`
}

// DefaultRestore returns a Restore with default values.
func DefaultRestore() *Restore {
	return &Restore{
		App:         defaultApp(),
		Cluster:     defaultCluster(),
		Restore:     defaultRestoreConfig(),
		Compression: defaultCompression(),
		Encryption:  defaultEncryption(),
		SecretAgent: defaultSecretAgent(),
		Aws: struct {
			S3 AwsS3 `yaml:"s3"`
		}{S3: defaultAwsS3()},
		Gcp: struct {
			Storage GcpStorage `yaml:"storage"`
		}{Storage: defaultGcpStorage()},
		Azure: struct {
			Blob AzureBlob `yaml:"blob"`
		}{Blob: defaultAzureBlob()},
	}
}

func (r *Restore) ToModelRestore() *models.Restore {
	if r == nil {
		return nil
	}

	return &models.Restore{
		//nolint:dupl // Mappings looks the same for common values.
		Common: models.Common{
			Directory:                     derefString(r.Restore.Directory),
			Namespace:                     derefString(r.Restore.Namespace),
			SetList:                       strings.Join(r.Restore.SetList, ","),
			BinList:                       strings.Join(r.Restore.BinList, ","),
			Parallel:                      derefInt(r.Restore.Parallel),
			NoRecords:                     derefBool(r.Restore.NoRecords),
			NoIndexes:                     derefBool(r.Restore.NoIndexes),
			NoUDFs:                        derefBool(r.Restore.NoUDFs),
			RecordsPerSecond:              derefInt(r.Restore.RecordsPerSecond),
			TotalTimeout:                  derefInt64(r.Restore.TotalTimeout),
			SocketTimeout:                 derefInt64(r.Restore.SocketTimeout),
			Bandwidth:                     derefInt64(r.Restore.Bandwidth),
			InfoTimeout:                   derefInt64(r.Restore.InfoTimeout),
			InfoMaxRetries:                derefUint(r.Restore.InfoMaxRetries),
			InfoRetriesMultiplier:         derefFloat64(r.Restore.InfoRetriesMultiplier),
			InfoRetryIntervalMilliseconds: derefInt64(r.Restore.InfoRetryIntervalMilliseconds),
			StdBufferSize:                 derefInt(r.Restore.StdBufferSize),
		},
		InputFile:          derefString(r.Restore.InputFile),
		DirectoryList:      strings.Join(r.Restore.DirectoryList, ","),
		ParentDirectory:    derefString(r.Restore.ParentDirectory),
		DisableBatchWrites: derefBool(r.Restore.DisableBatchWrites),
		BatchSize:          derefInt(r.Restore.BatchSize),
		MaxAsyncBatches:    derefInt(r.Restore.MaxAsyncBatches),
		WarmUp:             derefInt(r.Restore.WarmUp),
		ExtraTTL:           derefInt64(r.Restore.ExtraTTL),
		IgnoreRecordError:  derefBool(r.Restore.IgnoreRecordError),
		Uniq:               derefBool(r.Restore.Uniq),
		Replace:            derefBool(r.Restore.Replace),
		NoGeneration:       derefBool(r.Restore.NoGeneration),
		RetryBaseInterval:  derefInt64(r.Restore.RetryBaseInterval),
		RetryMultiplier:    derefFloat64(r.Restore.RetryMultiplier),
		RetryMaxAttempts:   derefUint(r.Restore.RetryMaxAttempts),
		ValidateOnly:       derefBool(r.Restore.ValidateOnly),
		ApplyMetadataLast:  derefBool(r.Restore.ApplyMetadataLast),
	}
}

type RestoreConfig struct {
	Directory                     *string  `yaml:"directory"`
	Namespace                     *string  `yaml:"namespace"`
	SetList                       []string `yaml:"set-list"`
	BinList                       []string `yaml:"bin-list"`
	Parallel                      *int     `yaml:"parallel"`
	NoRecords                     *bool    `yaml:"no-records"`
	NoIndexes                     *bool    `yaml:"no-indexes"`
	NoUDFs                        *bool    `yaml:"no-udfs"`
	RecordsPerSecond              *int     `yaml:"records-per-second"`
	TotalTimeout                  *int64   `yaml:"total-timeout"`
	SocketTimeout                 *int64   `yaml:"socket-timeout"`
	Bandwidth                     *int64   `yaml:"bandwidth"`
	InputFile                     *string  `yaml:"input-file"`
	DirectoryList                 []string `yaml:"directory-list"`
	ParentDirectory               *string  `yaml:"parent-directory"`
	DisableBatchWrites            *bool    `yaml:"disable-batch-writes"`
	BatchSize                     *int     `yaml:"batch-size"`
	MaxAsyncBatches               *int     `yaml:"max-async-batches"`
	WarmUp                        *int     `yaml:"warm-up"`
	ExtraTTL                      *int64   `yaml:"extra-ttl"`
	IgnoreRecordError             *bool    `yaml:"ignore-record-error"`
	Uniq                          *bool    `yaml:"unique"`
	Replace                       *bool    `yaml:"replace"`
	NoGeneration                  *bool    `yaml:"no-generation"`
	RetryBaseInterval             *int64   `yaml:"retry-base-interval"`
	RetryMultiplier               *float64 `yaml:"retry-multiplier"`
	RetryMaxAttempts              *uint    `yaml:"retry-max-attempts"`
	ValidateOnly                  *bool    `yaml:"validate"`
	InfoTimeout                   *int64   `yaml:"info-timeout"`
	InfoMaxRetries                *uint    `yaml:"info-max-retries"`
	InfoRetriesMultiplier         *float64 `yaml:"info-retry-multiplier"`
	InfoRetryIntervalMilliseconds *int64   `yaml:"info-retry-interval"`
	ApplyMetadataLast             *bool    `yaml:"apply-metadata-last"`
	StdBufferSize                 *int     `yaml:"std-buffer"`
}

func defaultRestoreConfig() RestoreConfig {
	return RestoreConfig{
		Directory:                     new(models.DefaultCommonDirectory),
		Namespace:                     new(models.DefaultCommonNamespace),
		SetList:                       []string{},
		BinList:                       []string{},
		NoRecords:                     new(models.DefaultCommonNoRecords),
		NoIndexes:                     new(models.DefaultCommonNoIndexes),
		NoUDFs:                        new(models.DefaultCommonNoUDFs),
		RecordsPerSecond:              new(models.DefaultCommonRecordsPerSecond),
		SocketTimeout:                 new(models.DefaultCommonSocketTimeout),
		InfoTimeout:                   new(models.DefaultCommonInfoTimeout),
		InfoMaxRetries:                new(models.DefaultCommonInfoMaxRetries),
		InfoRetriesMultiplier:         new(models.DefaultCommonInfoRetriesMultiplier),
		InfoRetryIntervalMilliseconds: new(models.DefaultCommonInfoRetryInterval),
		Bandwidth:                     new(models.DefaultCommonBandwidth),
		StdBufferSize:                 new(models.DefaultCommonStdBufferSize),
		TotalTimeout:                  new(models.DefaultRestoreTotalTimeout),
		Parallel:                      new(models.DefaultRestoreParallel),
		InputFile:                     new(models.DefaultRestoreInputFile),
		DirectoryList:                 []string{},
		ParentDirectory:               new(models.DefaultRestoreParentDirectory),
		DisableBatchWrites:            new(models.DefaultRestoreDisableBatchWrites),
		BatchSize:                     new(models.DefaultRestoreBatchSize),
		MaxAsyncBatches:               new(models.DefaultRestoreMaxAsyncBatches),
		WarmUp:                        new(models.DefaultRestoreWarmUp),
		ExtraTTL:                      new(models.DefaultRestoreExtraTTL),
		IgnoreRecordError:             new(models.DefaultRestoreIgnoreRecordError),
		Uniq:                          new(models.DefaultRestoreUniq),
		Replace:                       new(models.DefaultRestoreReplace),
		NoGeneration:                  new(models.DefaultRestoreNoGeneration),
		RetryBaseInterval:             new(models.DefaultRestoreRetryBaseInterval),
		RetryMultiplier:               new(models.DefaultRestoreRetryMultiplier),
		RetryMaxAttempts:              new(models.DefaultRestoreRetryMaxAttempts),
		ValidateOnly:                  new(models.DefaultRestoreValidateOnly),
		ApplyMetadataLast:             new(models.DefaultRestoreApplyMetadataLast),
	}
}
