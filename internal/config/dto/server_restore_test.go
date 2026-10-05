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
	"testing"

	"github.com/aerospike/absctl/internal/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultServerRestore(t *testing.T) {
	restore := DefaultServerRestore()

	require.NotNil(t, restore)
	require.NotNil(t, restore.App)
	require.NotNil(t, restore.Cluster)
	require.NotNil(t, restore.Restore)
	require.NotNil(t, restore.Prepare)
	require.NotNil(t, restore.Progress)
	require.NotNil(t, restore.Abort)
	require.NotNil(t, restore.SecretAgent)
	require.NotNil(t, restore.Aws.S3)
}

func TestDefaultServerRestoreSections(t *testing.T) {
	restore := defaultServerRestoreConfig()

	assert.Equal(t, models.DefaultCommonNamespace, derefString(restore.Namespace))
	assert.Equal(t, models.DefaultServerBackupObjectStorageType, derefString(restore.StorageType))
	assert.Equal(t, models.DefaultServerBackupJobID, derefString(restore.BackupID))
	assert.Equal(t, models.DefaultServerRestoreJobID, derefString(restore.JobID))
	assert.Equal(t, models.DefaultServerBackupPath, derefString(restore.Path))
	assert.Empty(t, restore.SetList)
	assert.Equal(t, models.DefaultServerFilterExp, derefString(restore.FilterExp))
	assert.Equal(t, models.DefaultCommonNoIndexes, derefBool(restore.NoIndexes))
	assert.Equal(t, models.DefaultCommonNoUDFs, derefBool(restore.NoUDFs))
	assert.Equal(t, models.DefaultServerRestoreFuzzyRestore, derefBool(restore.FuzzyRestore))
	assert.Equal(t, models.DefaultServerRestoreAllowUnhosted, derefBool(restore.AllowUnhosted))
	assert.Equal(t, models.DefaultServerRestoreParallel, derefInt(restore.Parallel))
	assert.Equal(t, models.DefaultServerRestoreRecordsPerSecond, derefInt(restore.RecordsPerSecond))
	assert.Equal(t, models.DefaultServerRestoreMaxInflight, derefInt(restore.MaxInflight))
	assert.Equal(t, models.DefaultServerRestoreRetryBaseInterval, derefInt(restore.RetryBaseInterval))
	assert.InDelta(t, models.DefaultServerRestoreRetryMultiplier, derefFloat64(restore.RetryMultiplier), 0)
	assert.Equal(t, models.DefaultServerRestoreRetryMaxAttempts, derefInt(restore.RetryMaxAttempts))
	assert.Equal(t, models.DefaultServerRestoreIgnoreRecordError, derefBool(restore.IgnoreRecordError))

	prepare := defaultServerRestorePrepareConfig()
	assert.Equal(t, models.DefaultCommonNamespace, derefString(prepare.Namespace))
	assert.Equal(t, models.DefaultServerRestoreJobID, derefString(prepare.JobID))
	assert.Equal(t, models.DefaultServerRestoreHydrateReplica, derefBool(prepare.HydrateReplica))

	progress := defaultServerRestoreProgressConfig()
	assert.Equal(t, models.DefaultCommonNamespace, derefString(progress.Namespace))

	abort := defaultServerRestoreAbortConfig()
	assert.Equal(t, models.DefaultCommonNamespace, derefString(abort.Namespace))
	assert.Equal(t, models.DefaultServerRestoreJobID, derefString(abort.JobID))
}

func TestServerRestoreToModels(t *testing.T) {
	t.Parallel()

	namespace := "ns1"
	storageType := "aws-s3"
	backupID := "bkp-1"
	jobID := "rst-1"
	path := "some/prefix"
	filterExp := "kwGTUQKkYmluMQE="
	enabled := true
	parallel := 16
	recordsPerSecond := 1000
	maxInflight := 500
	retryBaseInterval := 2000
	retryMultiplier := 1.5
	retryMaxAttempts := 3
	hydrateReplica := false
	prepareNamespace := "ns2"
	progressNamespace := "ns3"
	abortNamespace := "ns4"

	restore := &ServerRestore{
		Restore: ServerRestoreConfig{
			Namespace:         &namespace,
			StorageType:       &storageType,
			BackupID:          &backupID,
			JobID:             &jobID,
			Path:              &path,
			SetList:           []string{"set1", "set2"},
			FilterExp:         &filterExp,
			NoIndexes:         &enabled,
			NoUDFs:            &enabled,
			FuzzyRestore:      &enabled,
			AllowUnhosted:     &enabled,
			Parallel:          &parallel,
			RecordsPerSecond:  &recordsPerSecond,
			MaxInflight:       &maxInflight,
			RetryBaseInterval: &retryBaseInterval,
			RetryMultiplier:   &retryMultiplier,
			RetryMaxAttempts:  &retryMaxAttempts,
			IgnoreRecordError: &enabled,
		},
		Prepare: ServerRestorePrepareConfig{
			Namespace:      &prepareNamespace,
			JobID:          &jobID,
			HydrateReplica: &hydrateReplica,
		},
		Progress: ServerRestoreProgressConfig{Namespace: &progressNamespace},
		Abort:    ServerRestoreAbortConfig{Namespace: &abortNamespace, JobID: &jobID},
	}

	start := restore.ToModelServerRestore()
	require.NotNil(t, start)
	assert.Equal(t, &models.ServerRestore{
		Namespace:   namespace,
		StorageType: storageType,
		BackupID:    backupID,
		JobID:       jobID,
		Path:        path,
		// The model carries a comma separated list, the YAML a sequence.
		SetList:           "set1,set2",
		FilterExp:         filterExp,
		NoIndexes:         true,
		NoUDFs:            true,
		FuzzyRestore:      true,
		AllowUnhosted:     true,
		Parallel:          parallel,
		RecordsPerSecond:  recordsPerSecond,
		MaxInflight:       maxInflight,
		RetryBaseInterval: retryBaseInterval,
		RetryMultiplier:   retryMultiplier,
		RetryMaxAttempts:  retryMaxAttempts,
		IgnoreRecordError: true,
	}, start)

	prepare := restore.ToModelServerRestorePrepare()
	require.NotNil(t, prepare)
	assert.Equal(t, prepareNamespace, prepare.Namespace)
	assert.Equal(t, jobID, prepare.JobID)
	assert.False(t, prepare.HydrateReplica)

	progress := restore.ToModelServerRestoreProgress()
	require.NotNil(t, progress)
	assert.Equal(t, progressNamespace, progress.Namespace)

	abort := restore.ToModelServerRestoreAbort()
	require.NotNil(t, abort)
	assert.Equal(t, abortNamespace, abort.Namespace)
	assert.Equal(t, jobID, abort.JobID)
}

func TestServerRestoreToModelsNil(t *testing.T) {
	t.Parallel()

	var restore *ServerRestore

	assert.Nil(t, restore.ToModelServerRestore())
	assert.Nil(t, restore.ToModelServerRestorePrepare())
	assert.Nil(t, restore.ToModelServerRestoreProgress())
	assert.Nil(t, restore.ToModelServerRestoreAbort())
}

func TestServerRestoreToModelsNullFallsBackToDefaults(t *testing.T) {
	t.Parallel()

	restore := DefaultServerRestore()
	restore.Restore.Parallel = nil
	restore.Restore.MaxInflight = nil
	restore.Restore.RetryBaseInterval = nil
	restore.Restore.RetryMultiplier = nil
	restore.Restore.RetryMaxAttempts = nil
	restore.Prepare.HydrateReplica = nil

	start := restore.ToModelServerRestore()
	require.NotNil(t, start)
	assert.Equal(t, models.DefaultServerRestoreParallel, start.Parallel)
	assert.Equal(t, models.DefaultServerRestoreMaxInflight, start.MaxInflight)
	assert.Equal(t, models.DefaultServerRestoreRetryBaseInterval, start.RetryBaseInterval)
	assert.InDelta(t, models.DefaultServerRestoreRetryMultiplier, start.RetryMultiplier, 0)
	assert.Equal(t, models.DefaultServerRestoreRetryMaxAttempts, start.RetryMaxAttempts)

	prepare := restore.ToModelServerRestorePrepare()
	require.NotNil(t, prepare)
	assert.Equal(t, models.DefaultServerRestoreHydrateReplica, prepare.HydrateReplica)
}

func TestServerRestoreLoadSecretsNoAgent(t *testing.T) {
	t.Parallel()

	restore := DefaultServerRestore()
	reference := "secrets:resource:key"
	restore.Cluster.Password = &reference

	require.NoError(t, restore.LoadSecrets(t.Context()))
	assert.Equal(t, "secrets:resource:key", derefString(restore.Cluster.Password))
}
