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

// Backup is used to map yaml config.
type Backup struct {
	App         App          `yaml:"app"`
	Cluster     Cluster      `yaml:"cluster"`
	Backup      BackupConfig `yaml:"backup"`
	Compression Compression  `yaml:"compression"`
	Encryption  Encryption   `yaml:"encryption"`
	SecretAgent SecretAgent  `yaml:"secret-agent"`
	Aws         struct {
		S3 AwsS3 `yaml:"s3"`
	} `yaml:"aws"`
	Gcp struct {
		Storage GcpStorage `yaml:"storage"`
	} `yaml:"gcp"`
	Azure struct {
		Blob AzureBlob `yaml:"blob"`
	} `yaml:"azure"`
	Local struct {
		Disk Local `yaml:"disk"`
	} `yaml:"local"`
}

// DefaultBackup returns a Backup with default values.
func DefaultBackup() *Backup {
	return &Backup{
		App:         defaultApp(),
		Cluster:     defaultCluster(),
		Backup:      defaultBackupConfig(),
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
		Local: struct {
			Disk Local `yaml:"disk"`
		}{Disk: defaultLocal()},
	}
}

func (b *Backup) ToModelBackup() *models.Backup {
	if b == nil {
		return nil
	}

	return &models.Backup{
		//nolint:dupl // Mappings looks the same for common values.
		Common: models.Common{
			Directory:                     derefString(b.Backup.Directory),
			Namespace:                     derefString(b.Backup.Namespace),
			SetList:                       strings.Join(b.Backup.SetList, ","),
			BinList:                       strings.Join(b.Backup.BinList, ","),
			Parallel:                      derefInt(b.Backup.Parallel),
			NoRecords:                     derefBool(b.Backup.NoRecords),
			NoIndexes:                     derefBool(b.Backup.NoIndexes),
			NoUDFs:                        derefBool(b.Backup.NoUDFs),
			RecordsPerSecond:              derefInt(b.Backup.RecordsPerSecond),
			TotalTimeout:                  derefInt64(b.Backup.TotalTimeout),
			SocketTimeout:                 derefInt64(b.Backup.SocketTimeout),
			Bandwidth:                     derefInt64(b.Backup.Bandwidth),
			InfoTimeout:                   derefInt64(b.Backup.InfoTimeout),
			InfoMaxRetries:                derefUint(b.Backup.InfoMaxRetries),
			InfoRetriesMultiplier:         derefFloat64(b.Backup.InfoRetriesMultiplier),
			InfoRetryIntervalMilliseconds: derefInt64(b.Backup.InfoRetryIntervalMilliseconds),
			StdBufferSize:                 derefInt(b.Backup.StdBufferSize),
		},
		MaxRetries:          derefInt(b.Backup.MaxRetries),
		OutputFile:          derefString(b.Backup.OutputFile),
		RemoveFiles:         derefBool(b.Backup.RemoveFiles),
		ModifiedBefore:      derefString(b.Backup.ModifiedBefore),
		ModifiedAfter:       derefString(b.Backup.ModifiedAfter),
		FileLimit:           derefUint64(b.Backup.FileLimit),
		AfterDigest:         derefString(b.Backup.AfterDigest),
		MaxRecords:          derefInt64(b.Backup.MaxRecords),
		NoBins:              derefBool(b.Backup.NoBins),
		SleepBetweenRetries: derefInt(b.Backup.SleepBetweenRetries),
		FilterExpression:    derefString(b.Backup.FilterExpression),
		RemoveArtifacts:     derefBool(b.Backup.RemoveArtifacts),
		Compact:             derefBool(b.Backup.Compact),
		NodeList:            strings.Join(b.Backup.NodeList, ","),
		NoTTLOnly:           derefBool(b.Backup.NoTTLOnly),
		PreferRacks:         strings.Join(b.Backup.PreferRacks, ","),
		PartitionList:       strings.Join(b.Backup.PartitionList, ","),
		Estimate:            derefBool(b.Backup.Estimate),
		EstimateSamples:     derefInt64(b.Backup.EstimateSamples),
		StateFileDst:        derefString(b.Backup.StateFileDst),
		Continue:            derefString(b.Backup.Continue),
		ScanPageSize:        derefInt64(b.Backup.ScanPageSize),
		OutputFilePrefix:    derefString(b.Backup.OutputFilePrefix),
		RackList:            strings.Join(b.Backup.RackList, ","),
	}
}

type BackupConfig struct {
	Directory                     *string  `yaml:"directory"`
	Namespace                     *string  `yaml:"namespace"`
	SetList                       []string `yaml:"set-list"`
	BinList                       []string `yaml:"bin-list"`
	Parallel                      *int     `yaml:"parallel"`
	NoRecords                     *bool    `yaml:"no-records"`
	NoIndexes                     *bool    `yaml:"no-indexes"`
	NoUDFs                        *bool    `yaml:"no-udfs"`
	RecordsPerSecond              *int     `yaml:"records-per-second"`
	MaxRetries                    *int     `yaml:"max-retries"`
	TotalTimeout                  *int64   `yaml:"total-timeout"`
	SocketTimeout                 *int64   `yaml:"socket-timeout"`
	Bandwidth                     *int64   `yaml:"bandwidth"`
	OutputFile                    *string  `yaml:"output-file"`
	RemoveFiles                   *bool    `yaml:"remove-files"`
	ModifiedBefore                *string  `yaml:"modified-before"`
	ModifiedAfter                 *string  `yaml:"modified-after"`
	FileLimit                     *uint64  `yaml:"file-limit"`
	AfterDigest                   *string  `yaml:"after-digest"`
	MaxRecords                    *int64   `yaml:"max-records"`
	NoBins                        *bool    `yaml:"no-bins"`
	SleepBetweenRetries           *int     `yaml:"sleep-between-retries"`
	FilterExpression              *string  `yaml:"filter-exp"`
	RemoveArtifacts               *bool    `yaml:"remove-artifacts"`
	Compact                       *bool    `yaml:"compact"`
	NodeList                      []string `yaml:"node-list"`
	NoTTLOnly                     *bool    `yaml:"no-ttl-only"`
	PreferRacks                   []string `yaml:"prefer-racks"`
	PartitionList                 []string `yaml:"partition-list"`
	Estimate                      *bool    `yaml:"estimate"`
	EstimateSamples               *int64   `yaml:"estimate-samples"`
	StateFileDst                  *string  `yaml:"state-file-dst"`
	Continue                      *string  `yaml:"continue"`
	ScanPageSize                  *int64   `yaml:"scan-page-size"`
	OutputFilePrefix              *string  `yaml:"output-file-prefix"`
	RackList                      []string `yaml:"rack-list"`
	InfoTimeout                   *int64   `yaml:"info-timeout"`
	InfoMaxRetries                *uint    `yaml:"info-max-retries"`
	InfoRetriesMultiplier         *float64 `yaml:"info-retry-multiplier"`
	InfoRetryIntervalMilliseconds *int64   `yaml:"info-retry-interval"`
	StdBufferSize                 *int     `yaml:"std-buffer"`
}

func defaultBackupConfig() BackupConfig {
	return BackupConfig{
		Directory:                     new(models.DefaultCommonDirectory),
		Namespace:                     new(models.DefaultCommonNamespace),
		SetList:                       []string{},
		BinList:                       []string{},
		NoRecords:                     new(models.DefaultCommonNoRecords),
		NoIndexes:                     new(models.DefaultCommonNoIndexes),
		NoUDFs:                        new(models.DefaultCommonNoUDFs),
		RecordsPerSecond:              new(models.DefaultCommonRecordsPerSecond),
		MaxRetries:                    new(models.DefaultBackupMaxRetries),
		SocketTimeout:                 new(models.DefaultCommonSocketTimeout),
		InfoTimeout:                   new(models.DefaultCommonInfoTimeout),
		InfoMaxRetries:                new(models.DefaultCommonInfoMaxRetries),
		InfoRetriesMultiplier:         new(models.DefaultCommonInfoRetriesMultiplier),
		InfoRetryIntervalMilliseconds: new(models.DefaultCommonInfoRetryInterval),
		Bandwidth:                     new(models.DefaultCommonBandwidth),
		StdBufferSize:                 new(models.DefaultCommonStdBufferSize),
		OutputFile:                    new(models.DefaultBackupOutputFile),
		RemoveFiles:                   new(models.DefaultBackupRemoveFiles),
		ModifiedBefore:                new(models.DefaultBackupModifiedBefore),
		ModifiedAfter:                 new(models.DefaultBackupModifiedAfter),
		FileLimit:                     new(models.DefaultBackupFileLimit),
		AfterDigest:                   new(models.DefaultBackupAfterDigest),
		MaxRecords:                    new(models.DefaultBackupMaxRecords),
		NoBins:                        new(models.DefaultBackupNoBins),
		SleepBetweenRetries:           new(models.DefaultBackupSleepBetweenRetries),
		FilterExpression:              new(models.DefaultBackupFilterExpression),
		RemoveArtifacts:               new(models.DefaultBackupRemoveArtifacts),
		Compact:                       new(models.DefaultBackupCompact),
		NodeList:                      []string{},
		NoTTLOnly:                     new(models.DefaultBackupNoTTLOnly),
		PreferRacks:                   []string{},
		PartitionList:                 []string{},
		Estimate:                      new(models.DefaultBackupEstimate),
		EstimateSamples:               new(models.DefaultBackupEstimateSamples),
		StateFileDst:                  new(models.DefaultBackupStateFileDst),
		Continue:                      new(models.DefaultBackupContinue),
		ScanPageSize:                  new(models.DefaultBackupScanPageSize),
		OutputFilePrefix:              new(models.DefaultBackupOutputFilePrefix),
		RackList:                      []string{},
		TotalTimeout:                  new(models.DefaultBackupTotalTimeout),
		Parallel:                      new(models.DefaultBackupParallel),
	}
}
