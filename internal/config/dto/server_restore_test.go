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
	require.NotNil(t, restore.Progress)
	require.NotNil(t, restore.Abort)
	require.NotNil(t, restore.Open)
	require.NotNil(t, restore.ClearStorage)
	require.NotNil(t, restore.SecretAgent)
	require.NotNil(t, restore.Aws.S3)
}

func TestDefaultServerRestoreSections(t *testing.T) {
	restore := defaultServerRestoreConfig()

	assert.Equal(t, models.DefaultCommonNamespace, derefString(restore.Namespace))
	assert.Empty(t, restore.BackupIDs)
	assert.Equal(t, models.DefaultServerBackupPath, derefString(restore.Path))
	assert.Empty(t, restore.SetList)
	assert.Equal(t, models.DefaultServerFilterExp, derefString(restore.FilterExp))
	assert.Equal(t, models.DefaultCommonNoIndexes, derefBool(restore.NoIndexes))
	assert.Equal(t, models.DefaultCommonNoUDFs, derefBool(restore.NoUDFs))
	assert.Equal(t, models.DefaultServerRestorePolicy, derefString(restore.Policy))
	assert.Equal(t, models.DefaultServerRestoreParallelWrite, derefInt(restore.ParallelWrite))
	assert.Equal(t, models.DefaultServerRestoreRecordsPerSecond, derefInt(restore.RecordsPerSecond))
	assert.Equal(t, models.DefaultServerRestoreParallelRead, derefInt(restore.ParallelRead))
	assert.Equal(t, models.DefaultServerRestoreIncrementalPath, derefString(restore.IncrementalPath))
	assert.Equal(t, models.DefaultServerRestoreAsync, derefBool(restore.Async))
	assert.Equal(t, models.DefaultServerRestoreExtraTTL, derefInt(restore.ExtraTTL))

	// The settings of a single policy have no default, for the server to apply its own.
	assert.Nil(t, restore.HydrateReplica)
	assert.Nil(t, restore.MaxInflight)
	assert.Nil(t, restore.RetryBaseInterval)
	assert.Nil(t, restore.RetryMultiplier)
	assert.Nil(t, restore.RetryMaxAttempts)
	assert.Nil(t, restore.IgnoreRecordError)
	assert.Nil(t, restore.ApplyMetadataLast)

	progress := defaultServerRestoreProgressConfig()
	assert.Equal(t, models.DefaultCommonNamespace, derefString(progress.Namespace))
	assert.Equal(t, models.DefaultServerRestoreJobID, derefString(progress.JobID))
	assert.Equal(t, models.DefaultServerRestoreProgressWatch, derefBool(progress.Watch))

	abort := defaultServerRestoreAbortConfig()
	assert.Equal(t, models.DefaultCommonNamespace, derefString(abort.Namespace))
	assert.Equal(t, models.DefaultServerRestoreJobID, derefString(abort.JobID))
	assert.Equal(t, models.DefaultServerRestoreAsync, derefBool(abort.Async))

	open := defaultServerRestoreOpenConfig()
	assert.Equal(t, models.DefaultCommonNamespace, derefString(open.Namespace))
	assert.Equal(t, models.DefaultServerRestoreJobID, derefString(open.JobID))
	assert.Equal(t, models.DefaultServerRestoreAsync, derefBool(open.Async))

	clearStorage := defaultServerRestoreClearConfig()
	assert.Equal(t, models.DefaultCommonNamespace, derefString(clearStorage.Namespace))
	assert.Equal(t, models.DefaultServerRestoreJobID, derefString(clearStorage.JobID))
	assert.Equal(t, models.DefaultServerRestoreAsync, derefBool(clearStorage.Async))
}

func TestServerRestoreToModels(t *testing.T) {
	t.Parallel()

	namespace := "ns1"
	jobID := "rst-1"
	path := "some/prefix"
	filterExp := "kwGTUQKkYmluMQE="
	enabled := true
	// The flags that differ from enabled catch a field mapped from its neighbor.
	disabled := false
	parallelWrite := 16
	recordsPerSecond := 1000
	maxInflight := 500
	retryBaseInterval := 2000
	retryMultiplier := 1.5
	retryMaxAttempts := 3
	parallelRead := 32
	hydrateReplica := false
	progressNamespace := "ns3"
	abortNamespace := "ns4"
	openNamespace := "ns5"
	clearNamespace := "ns6"
	incrementalPath := "incr/prefix"
	extraTTL := 3600
	policy := "fuzzy"

	restore := &ServerRestore{
		Restore: ServerRestoreConfig{
			Namespace:         &namespace,
			BackupIDs:         []string{"bkp-1", "bkp-2"},
			Path:              &path,
			SetList:           []string{"set1", "set2"},
			FilterExp:         &filterExp,
			NoIndexes:         &disabled,
			NoUDFs:            &enabled,
			Policy:            &policy,
			ParallelWrite:     &parallelWrite,
			RecordsPerSecond:  &recordsPerSecond,
			MaxInflight:       &maxInflight,
			RetryBaseInterval: &retryBaseInterval,
			RetryMultiplier:   &retryMultiplier,
			RetryMaxAttempts:  &retryMaxAttempts,
			IgnoreRecordError: &enabled,
			ApplyMetadataLast: &disabled,
			IncrementalPath:   &incrementalPath,
			Async:             &enabled,
			ExtraTTL:          &extraTTL,
			HydrateReplica:    &hydrateReplica,

			ParallelRead: &parallelRead,
		},
		Progress:     ServerRestoreProgressConfig{Namespace: &progressNamespace, JobID: &jobID, Watch: &enabled},
		Abort:        ServerRestoreAbortConfig{Namespace: &abortNamespace, JobID: &jobID, Async: &enabled},
		Open:         ServerRestoreOpenConfig{Namespace: &openNamespace, JobID: &jobID, Async: &enabled},
		ClearStorage: ServerRestoreClearConfig{Namespace: &clearNamespace, JobID: &jobID, Async: &enabled},
	}

	start := restore.ToModelServerRestore()
	require.NotNil(t, start)
	assert.Equal(t, &models.ServerRestore{
		Namespace: namespace,
		Path:      path,
		// The model carries a comma separated list, the YAML a sequence.
		BackupIDs:         "bkp-1,bkp-2",
		SetList:           "set1,set2",
		FilterExp:         filterExp,
		NoIndexes:         false,
		NoUDFs:            true,
		Policy:            policy,
		ParallelWrite:     parallelWrite,
		RecordsPerSecond:  recordsPerSecond,
		MaxInflight:       &maxInflight,
		RetryBaseInterval: &retryBaseInterval,
		RetryMultiplier:   &retryMultiplier,
		RetryMaxAttempts:  &retryMaxAttempts,
		IgnoreRecordError: &enabled,
		ApplyMetadataLast: &disabled,
		IncrementalPath:   incrementalPath,
		Async:             true,
		ExtraTTL:          extraTTL,
		HydrateReplica:    &hydrateReplica,

		ParallelRead: parallelRead,
	}, start)

	// The model gets copies of the optional settings, so it shares no memory with the DTO.
	assert.NotSame(t, restore.Restore.HydrateReplica, start.HydrateReplica)
	assert.NotSame(t, restore.Restore.MaxInflight, start.MaxInflight)
	assert.NotSame(t, restore.Restore.RetryBaseInterval, start.RetryBaseInterval)
	assert.NotSame(t, restore.Restore.RetryMultiplier, start.RetryMultiplier)
	assert.NotSame(t, restore.Restore.RetryMaxAttempts, start.RetryMaxAttempts)
	assert.NotSame(t, restore.Restore.IgnoreRecordError, start.IgnoreRecordError)
	assert.NotSame(t, restore.Restore.ApplyMetadataLast, start.ApplyMetadataLast)

	progress := restore.ToModelServerRestoreProgress()
	require.NotNil(t, progress)
	assert.Equal(t, &models.ServerRestoreProgress{Namespace: progressNamespace, JobID: jobID, Watch: true}, progress)

	abort := restore.ToModelServerRestoreAbort()
	require.NotNil(t, abort)
	assert.Equal(t, abortNamespace, abort.Namespace)
	assert.Equal(t, jobID, abort.JobID)
	assert.True(t, abort.Async)

	assert.Equal(t, &models.ServerRestoreOpen{Namespace: openNamespace, JobID: jobID, Async: true},
		restore.ToModelServerRestoreOpen())
	assert.Equal(t, &models.ServerRestoreClearStorage{Namespace: clearNamespace, JobID: jobID, Async: true},
		restore.ToModelServerRestoreClearStorage())
}

func TestServerRestoreToModelsNil(t *testing.T) {
	t.Parallel()

	var restore *ServerRestore

	assert.Nil(t, restore.ToModelServerRestore())
	assert.Nil(t, restore.ToModelServerRestoreProgress())
	assert.Nil(t, restore.ToModelServerRestoreAbort())
	assert.Nil(t, restore.ToModelServerRestoreOpen())
	assert.Nil(t, restore.ToModelServerRestoreClearStorage())
}

func TestServerRestoreToModelsNullFallsBackToDefaults(t *testing.T) {
	t.Parallel()

	restore := DefaultServerRestore()
	restore.Restore.ParallelWrite = nil
	restore.Restore.ParallelRead = nil

	start := restore.ToModelServerRestore()
	require.NotNil(t, start)
	assert.Equal(t, models.DefaultServerRestoreParallelWrite, start.ParallelWrite)
	assert.Equal(t, models.DefaultServerRestoreParallelRead, start.ParallelRead)

	// The settings of a single policy stay unset, for the server to apply its defaults.
	assert.Nil(t, start.HydrateReplica)
	assert.Nil(t, start.MaxInflight)
	assert.Nil(t, start.RetryBaseInterval)
	assert.Nil(t, start.RetryMultiplier)
	assert.Nil(t, start.RetryMaxAttempts)
	assert.Nil(t, start.IgnoreRecordError)
	assert.Nil(t, start.ApplyMetadataLast)
}

func TestServerRestoreLoadSecretsNoAgent(t *testing.T) {
	t.Parallel()

	restore := DefaultServerRestore()
	reference := "secrets:resource:key"
	restore.Cluster.Password = &reference

	require.NoError(t, restore.LoadSecrets(t.Context()))
	assert.Equal(t, "secrets:resource:key", derefString(restore.Cluster.Password))
}

func TestSnapshotRestoreS3ToModelSnapshotS3(t *testing.T) {
	t.Parallel()

	bucket := "my-bucket"
	region := "eu-central-1"
	profile := "default"
	endpoint := "http://127.0.0.1:9000"
	accessKey := "access"
	secretKey := "secret"

	s3 := &SnapshotRestoreS3{
		BucketName:       &bucket,
		Region:           &region,
		Profile:          &profile,
		EndpointOverride: &endpoint,
		AccessKeyID:      &accessKey,
		SecretAccessKey:  &secretKey,
	}

	// The restore schema has no local S3 client settings, so they keep their defaults.
	want := &models.SnapshotS3{
		BucketName:       bucket,
		Region:           region,
		Profile:          profile,
		Endpoint:         endpoint,
		AccessKeyID:      accessKey,
		SecretAccessKey:  secretKey,
		RetryMaxAttempts: models.DefaultS3RetryMaxAttempts,
		RetryMaxBackoff:  models.DefaultS3RetryMaxBackoff,
		MaxConnsPerHost:  models.DefaultCloudMaxConnsPerHost,
		RequestTimeout:   models.DefaultCloudRequestTimeout,
	}

	assert.Equal(t, want, s3.ToModelSnapshotS3())

	var nilS3 *SnapshotRestoreS3
	assert.Nil(t, nilS3.ToModelSnapshotS3())
}
