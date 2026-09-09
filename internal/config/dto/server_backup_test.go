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

func TestDefaultServerBackup(t *testing.T) {
	backup := DefaultServerBackup()

	require.NotNil(t, backup)
	require.NotNil(t, backup.App)
	require.NotNil(t, backup.Cluster)
	require.NotNil(t, backup.Backup)
	require.NotNil(t, backup.List)
	require.NotNil(t, backup.Validate)
	require.NotNil(t, backup.Progress)
	require.NotNil(t, backup.SecretAgent)
	require.NotNil(t, backup.Aws.S3)
}

func TestDefaultServerBackupSections(t *testing.T) {
	backup := defaultServerBackupConfig()

	assert.Equal(t, models.DefaultCommonNamespace, derefString(backup.Namespace))
	assert.Equal(t, models.DefaultServerBackupObjectStorageType, derefString(backup.StorageType))
	assert.Equal(t, models.DefaultBackupModifiedAfter, derefString(backup.ModifiedAfter))
	assert.Equal(t, models.DefaultBackupModifiedBefore, derefString(backup.ModifiedBefore))
	assert.Empty(t, backup.SetList)
	assert.Equal(t, models.DefaultCommonNoIndexes, derefBool(backup.NoIndexes))
	assert.Equal(t, models.DefaultCommonNoUDFs, derefBool(backup.NoUDFs))
	assert.Equal(t, models.DefaultBackupEnableChangeStream, derefBool(backup.EnableChangeStream))

	list := defaultServerBackupListConfig()
	assert.Equal(t, models.DefaultServerBackupPath, derefString(list.Path))

	validate := defaultServerBackupValidateConfig()
	assert.Equal(t, models.DefaultServerBackupJobID, derefString(validate.JobID))
	assert.Equal(t, models.DefaultServerBackupValidateSampleSize, derefInt(validate.SampleSize))

	progress := defaultServerBackupProgressConfig()
	assert.Equal(t, models.DefaultServerBackupJobID, derefString(progress.JobID))
	assert.Equal(t, models.DefaultServerBackupProgressWatch, derefBool(progress.Watch))
	assert.Equal(t, models.DefaultServerBackupProgressWatchPoll, derefInt64(progress.WatchPoll))
}

func TestServerBackupToModels(t *testing.T) {
	t.Parallel()

	namespace := "ns1"
	storageType := "aws-s3"
	modifiedAfter := "2026-01-01_00:00:00"
	modifiedBefore := "2026-02-01_00:00:00"
	noIndexes := true
	noUDFs := true
	changeStream := true
	path := "some/prefix"
	jobID := "bkp-1"
	sampleSize := 500
	watch := true
	watchPoll := int64(2000)

	backup := &ServerBackup{
		Backup: ServerBackupConfig{
			Namespace:          &namespace,
			StorageType:        &storageType,
			ModifiedAfter:      &modifiedAfter,
			ModifiedBefore:     &modifiedBefore,
			SetList:            []string{"set1", "set2"},
			NoIndexes:          &noIndexes,
			NoUDFs:             &noUDFs,
			EnableChangeStream: &changeStream,
		},
		List:     ServerBackupListConfig{Path: &path},
		Validate: ServerBackupValidateConfig{JobID: &jobID, SampleSize: &sampleSize},
		Progress: ServerBackupProgressConfig{JobID: &jobID, Watch: &watch, WatchPoll: &watchPoll},
	}

	start := backup.ToModelServerBackup()
	require.NotNil(t, start)
	assert.Equal(t, namespace, start.Namespace)
	assert.Equal(t, storageType, start.StorageType)
	assert.Equal(t, modifiedAfter, start.ModifiedAfter)
	assert.Equal(t, modifiedBefore, start.ModifiedBefore)
	// The model carries a comma separated list, the YAML a sequence.
	assert.Equal(t, "set1,set2", start.SetList)
	assert.True(t, start.NoIndexes)
	assert.True(t, start.NoUDFs)
	assert.True(t, start.EnableChangeStream)

	list := backup.ToModelServerBackupList()
	require.NotNil(t, list)
	assert.Equal(t, path, list.Path)

	validate := backup.ToModelServerBackupValidate()
	require.NotNil(t, validate)
	assert.Equal(t, jobID, validate.JobID)
	assert.Equal(t, sampleSize, validate.SampleSize)

	progress := backup.ToModelServerBackupProgress()
	require.NotNil(t, progress)
	assert.Equal(t, jobID, progress.JobID)
	assert.True(t, progress.Watch)
	assert.Equal(t, watchPoll, progress.WatchPoll)
}

func TestServerBackupToModelsNil(t *testing.T) {
	t.Parallel()

	var backup *ServerBackup

	assert.Nil(t, backup.ToModelServerBackup())
	assert.Nil(t, backup.ToModelServerBackupList())
	assert.Nil(t, backup.ToModelServerBackupValidate())
	assert.Nil(t, backup.ToModelServerBackupProgress())
}

func TestServerBackupLoadSecretsNoAgent(t *testing.T) {
	t.Parallel()

	// Without a configured secret agent the values are left untouched.
	backup := DefaultServerBackup()
	reference := "secrets:resource:key"
	backup.Aws.S3.BucketName = &reference

	require.NoError(t, backup.LoadSecrets(t.Context()))
	assert.Equal(t, "secrets:resource:key", derefString(backup.Aws.S3.BucketName))
}

func TestDefaultObjectStorageS3(t *testing.T) {
	t.Parallel()

	s3 := defaultObjectStorageS3()

	assert.Equal(t, models.DefaultS3BucketName, derefString(s3.BucketName))
	assert.Equal(t, models.DefaultS3Region, derefString(s3.Region))
	assert.Equal(t, models.DefaultS3Profile, derefString(s3.Profile))
	assert.Equal(t, models.DefaultS3Endpoint, derefString(s3.EndpointOverride))
	assert.Equal(t, models.DefaultS3AccessKeyID, derefString(s3.AccessKeyID))
	assert.Equal(t, models.DefaultS3SecretAccessKey, derefString(s3.SecretAccessKey))
}

func TestObjectStorageS3ToModelAwsS3(t *testing.T) {
	t.Parallel()

	bucket := "my-bucket"
	region := "eu-central-1"
	profile := "default"
	endpoint := "http://127.0.0.1:9000"
	accessKey := "access"
	secretKey := "secret"

	s3 := &ObjectStorageS3{
		BucketName:       &bucket,
		Region:           &region,
		Profile:          &profile,
		EndpointOverride: &endpoint,
		AccessKeyID:      &accessKey,
		SecretAccessKey:  &secretKey,
	}

	got := s3.ToModelAwsS3()
	require.NotNil(t, got)

	assert.Equal(t, bucket, got.BucketName)
	assert.Equal(t, region, got.Region)
	assert.Equal(t, profile, got.Profile)
	assert.Equal(t, endpoint, got.Endpoint)
	assert.Equal(t, accessKey, got.AccessKeyID)
	assert.Equal(t, secretKey, got.SecretAccessKey)

	// The client-side knobs have no flag on the snapshot commands, so they are
	// filled with the defaults that keep models.AwsS3 validation happy.
	assert.Equal(t, models.DefaultS3RestorePollDuration, got.RestorePollDuration)
	assert.Equal(t, models.DefaultS3RetryMaxAttempts, got.RetryMaxAttempts)
	assert.Equal(t, models.DefaultS3RetryMaxBackoff, got.RetryMaxBackoff)
	assert.Equal(t, models.DefaultS3ChunkSize, got.ChunkSize)
	assert.Equal(t, models.DefaultCloudRetryReadBackoff, got.RetryReadBackoff)
	assert.InEpsilon(t, models.DefaultCloudRetryReadMultiplier, got.RetryReadMultiplier, 0.0)
	assert.Equal(t, models.DefaultCloudRetryReadMaxAttempts, got.RetryReadMaxAttempts)

	var nilS3 *ObjectStorageS3
	assert.Nil(t, nilS3.ToModelAwsS3())
}
