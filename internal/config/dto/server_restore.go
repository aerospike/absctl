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

	"github.com/aerospike/absctl/internal/models"
)

// ServerRestore is used to map the yaml config of the snapshot-restore command
// tree. A single file describes every subcommand: the section matching the
// invoked subcommand is read, the rest are ignored.
type ServerRestore struct {
	App         App                         `yaml:"app"`
	Cluster     Cluster                     `yaml:"cluster"`
	Restore     ServerRestoreConfig         `yaml:"restore"`
	Prepare     ServerRestorePrepareConfig  `yaml:"prepare"`
	Progress    ServerRestoreProgressConfig `yaml:"progress"`
	SecretAgent SecretAgent                 `yaml:"secret-agent"`
	Aws         struct {
		S3 ObjectStorageS3 `yaml:"s3"`
	} `yaml:"aws"`
}

// DefaultServerRestore returns a ServerRestore with default values.
func DefaultServerRestore() *ServerRestore {
	return &ServerRestore{
		App:         defaultApp(),
		Cluster:     defaultCluster(),
		Restore:     defaultServerRestoreConfig(),
		Prepare:     defaultServerRestorePrepareConfig(),
		Progress:    defaultServerRestoreProgressConfig(),
		SecretAgent: defaultSecretAgent(),
		Aws: struct {
			S3 ObjectStorageS3 `yaml:"s3"`
		}{S3: defaultObjectStorageS3()},
	}
}

// LoadSecrets resolves any secrets: prefixed values in the ServerRestore DTO
// using the configured secret agent. Must be called before DTO-to-model conversion
// so that CertFlag.Set() receives file paths or raw content, not secret references.
func (r *ServerRestore) LoadSecrets(ctx context.Context) error {
	saCfg := r.SecretAgent.ToModelSecretAgent().Config()
	if saCfg == nil {
		return nil
	}

	fields := collectServerSecretableFields(&r.Cluster, &r.Aws.S3)

	return resolveSecretFields(ctx, saCfg, fields...)
}

// ServerRestoreConfig maps the "restore" section, used by
// "snapshot-restore start".
type ServerRestoreConfig struct {
	Namespace    *string `yaml:"namespace"`
	StorageType  *string `yaml:"object-storage-type"`
	JobID        *string `yaml:"backup-id"`
	Path         *string `yaml:"path"`
	FuzzyRestore *bool   `yaml:"fuzzy-restore"`
}

func defaultServerRestoreConfig() ServerRestoreConfig {
	return ServerRestoreConfig{
		Namespace:    new(models.DefaultCommonNamespace),
		StorageType:  new(models.DefaultServerBackupObjectStorageType),
		JobID:        new(models.DefaultServerBackupJobID),
		Path:         new(models.DefaultServerBackupPath),
		FuzzyRestore: new(models.DefaultServerRestoreFuzzyRestore),
	}
}

// ToModelServerRestore maps the "restore" section onto its model.
func (r *ServerRestore) ToModelServerRestore() *models.ServerRestore {
	if r == nil {
		return nil
	}

	return &models.ServerRestore{
		Namespace:    derefString(r.Restore.Namespace),
		StorageType:  derefString(r.Restore.StorageType),
		JobID:        derefString(r.Restore.JobID),
		Path:         derefString(r.Restore.Path),
		FuzzyRestore: derefBool(r.Restore.FuzzyRestore),
	}
}

// ServerRestorePrepareConfig maps the "prepare" section, used by
// "snapshot-restore prepare".
type ServerRestorePrepareConfig struct {
	Namespace *string `yaml:"namespace"`
	JobID     *string `yaml:"backup-id"`
}

func defaultServerRestorePrepareConfig() ServerRestorePrepareConfig {
	return ServerRestorePrepareConfig{
		Namespace: new(models.DefaultCommonNamespace),
		JobID:     new(models.DefaultServerBackupJobID),
	}
}

// ToModelServerRestorePrepare maps the "prepare" section onto its model.
func (r *ServerRestore) ToModelServerRestorePrepare() *models.ServerRestorePrepare {
	if r == nil {
		return nil
	}

	return &models.ServerRestorePrepare{
		Namespace: derefString(r.Prepare.Namespace),
		JobID:     derefString(r.Prepare.JobID),
	}
}

// ServerRestoreProgressConfig maps the "progress" section, used by
// "snapshot-restore progress".
type ServerRestoreProgressConfig struct {
	Namespace *string `yaml:"namespace"`
}

func defaultServerRestoreProgressConfig() ServerRestoreProgressConfig {
	return ServerRestoreProgressConfig{
		Namespace: new(models.DefaultCommonNamespace),
	}
}

// ToModelServerRestoreProgress maps the "progress" section onto its model.
func (r *ServerRestore) ToModelServerRestoreProgress() *models.ServerRestoreProgress {
	if r == nil {
		return nil
	}

	return &models.ServerRestoreProgress{
		Namespace: derefString(r.Progress.Namespace),
	}
}
