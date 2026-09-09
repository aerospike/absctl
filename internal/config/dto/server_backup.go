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

package dto

import (
	"context"
	"strings"

	"github.com/aerospike/absctl/internal/models"
)

// ServerBackup is used to map the yaml config of the snapshot-backup command
// tree. A single file describes every subcommand: the section matching the
// invoked subcommand is read, the rest are ignored.
type ServerBackup struct {
	App         App                        `yaml:"app"`
	Cluster     Cluster                    `yaml:"cluster"`
	Backup      ServerBackupConfig         `yaml:"backup"`
	List        ServerBackupListConfig     `yaml:"list"`
	Validate    ServerBackupValidateConfig `yaml:"validate"`
	Progress    ServerBackupProgressConfig `yaml:"progress"`
	SecretAgent SecretAgent                `yaml:"secret-agent"`
	Aws         struct {
		S3 ObjectStorageS3 `yaml:"s3"`
	} `yaml:"aws"`
}

// DefaultServerBackup returns a ServerBackup with default values.
func DefaultServerBackup() *ServerBackup {
	return &ServerBackup{
		App:         defaultApp(),
		Cluster:     defaultCluster(),
		Backup:      defaultServerBackupConfig(),
		List:        defaultServerBackupListConfig(),
		Validate:    defaultServerBackupValidateConfig(),
		Progress:    defaultServerBackupProgressConfig(),
		SecretAgent: defaultSecretAgent(),
		Aws: struct {
			S3 ObjectStorageS3 `yaml:"s3"`
		}{S3: defaultObjectStorageS3()},
	}
}

// LoadSecrets resolves any secrets: prefixed values in the ServerBackup DTO
// using the configured secret agent. Must be called before DTO-to-model conversion
// so that CertFlag.Set() receives file paths or raw content, not secret references.
func (b *ServerBackup) LoadSecrets(ctx context.Context) error {
	saCfg := b.SecretAgent.ToModelSecretAgent().Config()
	if saCfg == nil {
		return nil
	}

	fields := collectServerSecretableFields(&b.Cluster, &b.Aws.S3)

	return resolveSecretFields(ctx, saCfg, fields...)
}

// ServerBackupConfig maps the "backup" section, used by "snapshot-backup start".
type ServerBackupConfig struct {
	Namespace          *string  `yaml:"namespace"`
	StorageType        *string  `yaml:"object-storage-type"`
	ModifiedAfter      *string  `yaml:"modified-after"`
	ModifiedBefore     *string  `yaml:"modified-before"`
	SetList            []string `yaml:"set-list"`
	NoIndexes          *bool    `yaml:"no-indexes"`
	NoUDFs             *bool    `yaml:"no-udfs"`
	EnableChangeStream *bool    `yaml:"enable-change-stream"`
}

func defaultServerBackupConfig() ServerBackupConfig {
	return ServerBackupConfig{
		Namespace:          new(models.DefaultCommonNamespace),
		StorageType:        new(models.DefaultServerBackupObjectStorageType),
		ModifiedAfter:      new(models.DefaultBackupModifiedAfter),
		ModifiedBefore:     new(models.DefaultBackupModifiedBefore),
		SetList:            []string{},
		NoIndexes:          new(models.DefaultCommonNoIndexes),
		NoUDFs:             new(models.DefaultCommonNoUDFs),
		EnableChangeStream: new(models.DefaultBackupEnableChangeStream),
	}
}

// ToModelServerBackup maps the "backup" section onto its model.
func (b *ServerBackup) ToModelServerBackup() *models.ServerBackup {
	if b == nil {
		return nil
	}

	return &models.ServerBackup{
		Namespace:          derefString(b.Backup.Namespace),
		StorageType:        derefString(b.Backup.StorageType),
		ModifiedAfter:      derefString(b.Backup.ModifiedAfter),
		ModifiedBefore:     derefString(b.Backup.ModifiedBefore),
		SetList:            strings.Join(b.Backup.SetList, ","),
		NoIndexes:          derefBool(b.Backup.NoIndexes),
		NoUDFs:             derefBool(b.Backup.NoUDFs),
		EnableChangeStream: derefBool(b.Backup.EnableChangeStream),
	}
}

// ServerBackupListConfig maps the "list" section, used by "snapshot-backup list".
type ServerBackupListConfig struct {
	Path *string `yaml:"path"`
}

func defaultServerBackupListConfig() ServerBackupListConfig {
	return ServerBackupListConfig{
		Path: new(models.DefaultServerBackupPath),
	}
}

// ToModelServerBackupList maps the "list" section onto its model.
func (b *ServerBackup) ToModelServerBackupList() *models.ServerBackupList {
	if b == nil {
		return nil
	}

	return &models.ServerBackupList{
		Path: derefString(b.List.Path),
	}
}

// ServerBackupValidateConfig maps the "validate" section, used by
// "snapshot-backup validate".
type ServerBackupValidateConfig struct {
	JobID      *string `yaml:"backup-id"`
	SampleSize *int    `yaml:"sample-size"`
}

func defaultServerBackupValidateConfig() ServerBackupValidateConfig {
	return ServerBackupValidateConfig{
		JobID:      new(models.DefaultServerBackupJobID),
		SampleSize: new(models.DefaultServerBackupValidateSampleSize),
	}
}

// ToModelServerBackupValidate maps the "validate" section onto its model.
func (b *ServerBackup) ToModelServerBackupValidate() *models.ServerBackupValidate {
	if b == nil {
		return nil
	}

	return &models.ServerBackupValidate{
		JobID:      derefString(b.Validate.JobID),
		SampleSize: derefInt(b.Validate.SampleSize),
	}
}

// ServerBackupProgressConfig maps the "progress" section, used by
// "snapshot-backup progress".
type ServerBackupProgressConfig struct {
	JobID     *string `yaml:"backup-id"`
	Watch     *bool   `yaml:"watch"`
	WatchPoll *int64  `yaml:"watch-poll"`
}

func defaultServerBackupProgressConfig() ServerBackupProgressConfig {
	return ServerBackupProgressConfig{
		JobID:     new(models.DefaultServerBackupJobID),
		Watch:     new(models.DefaultServerBackupProgressWatch),
		WatchPoll: new(models.DefaultServerBackupProgressWatchPoll),
	}
}

// ToModelServerBackupProgress maps the "progress" section onto its model.
func (b *ServerBackup) ToModelServerBackupProgress() *models.ServerBackupProgress {
	if b == nil {
		return nil
	}

	return &models.ServerBackupProgress{
		JobID:     derefString(b.Progress.JobID),
		Watch:     derefBool(b.Progress.Watch),
		WatchPoll: derefInt64(b.Progress.WatchPoll),
	}
}
