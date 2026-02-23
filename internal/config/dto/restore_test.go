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
	"testing"

	"github.com/aerospike/absctl/internal/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultRestore(t *testing.T) {
	restore := DefaultRestore()

	require.NotNil(t, restore)
	require.NotNil(t, restore.App)
	require.NotNil(t, restore.Cluster)
	require.NotNil(t, restore.Restore)
	require.NotNil(t, restore.Compression)
	require.NotNil(t, restore.Encryption)
	require.NotNil(t, restore.SecretAgent)
	require.NotNil(t, restore.Aws.S3)
	require.NotNil(t, restore.Gcp.Storage)
	require.NotNil(t, restore.Azure.Blob)
}

func TestDefaultRestoreConfig(t *testing.T) {
	config := defaultRestoreConfig()

	assert.Equal(t, models.DefaultCommonDirectory, derefString(config.Directory))
	assert.Equal(t, models.DefaultCommonNamespace, derefString(config.Namespace))
	assert.Empty(t, config.SetList)
	assert.Empty(t, config.BinList)
	assert.Equal(t, models.DefaultRestoreParallel, derefInt(config.Parallel))
	assert.Equal(t, models.DefaultCommonNoRecords, derefBool(config.NoRecords))
	assert.Equal(t, models.DefaultCommonNoIndexes, derefBool(config.NoIndexes))
	assert.Equal(t, models.DefaultCommonNoUDFs, derefBool(config.NoUDFs))
	assert.Equal(t, models.DefaultCommonRecordsPerSecond, derefInt(config.RecordsPerSecond))
	assert.Equal(t, models.DefaultCommonSocketTimeout, derefInt64(config.SocketTimeout))
	assert.Equal(t, models.DefaultCommonInfoTimeout, derefInt64(config.InfoTimeout))
	assert.Equal(t, models.DefaultCommonInfoMaxRetries, derefUint(config.InfoMaxRetries))
	assert.InEpsilon(t, models.DefaultCommonInfoRetriesMultiplier, derefFloat64(config.InfoRetriesMultiplier), 0.0)
	assert.Equal(t, models.DefaultCommonInfoRetryInterval, derefInt64(config.InfoRetryIntervalMilliseconds))
	assert.Equal(t, models.DefaultCommonBandwidth, derefInt64(config.Bandwidth))
	assert.Equal(t, models.DefaultCommonStdBufferSize, derefInt(config.StdBufferSize))
	assert.Equal(t, models.DefaultRestoreTotalTimeout, derefInt64(config.TotalTimeout))
	assert.Equal(t, models.DefaultRestoreInputFile, derefString(config.InputFile))
	assert.Empty(t, config.DirectoryList)
	assert.Equal(t, models.DefaultRestoreParentDirectory, derefString(config.ParentDirectory))
	assert.Equal(t, models.DefaultRestoreDisableBatchWrites, derefBool(config.DisableBatchWrites))
	assert.Equal(t, models.DefaultRestoreBatchSize, derefInt(config.BatchSize))
	assert.Equal(t, models.DefaultRestoreMaxAsyncBatches, derefInt(config.MaxAsyncBatches))
	assert.Equal(t, models.DefaultRestoreWarmUp, derefInt(config.WarmUp))
	assert.Equal(t, models.DefaultRestoreExtraTTL, derefInt64(config.ExtraTTL))
	assert.Equal(t, models.DefaultRestoreIgnoreRecordError, derefBool(config.IgnoreRecordError))
	assert.Equal(t, models.DefaultRestoreUniq, derefBool(config.Uniq))
	assert.Equal(t, models.DefaultRestoreReplace, derefBool(config.Replace))
	assert.Equal(t, models.DefaultRestoreNoGeneration, derefBool(config.NoGeneration))
	assert.Equal(t, models.DefaultRestoreRetryBaseInterval, derefInt64(config.RetryBaseInterval))
	assert.InEpsilon(t, models.DefaultRestoreRetryMultiplier, derefFloat64(config.RetryMultiplier), 0.0)
	assert.Equal(t, models.DefaultRestoreRetryMaxAttempts, derefUint(config.RetryMaxAttempts))
	assert.Equal(t, models.DefaultRestoreValidateOnly, derefBool(config.ValidateOnly))
	assert.Equal(t, models.DefaultRestoreApplyMetadataLast, derefBool(config.ApplyMetadataLast))
}

func TestRestoreConfig_ToModelRestore(t *testing.T) {
	config := RestoreConfig{
		Directory:                     new("/restore"),
		Namespace:                     new("test"),
		SetList:                       []string{"set1", "set2"},
		BinList:                       []string{"bin1", "bin2"},
		Parallel:                      new(8),
		NoRecords:                     new(false),
		NoIndexes:                     new(true),
		NoUDFs:                        new(false),
		RecordsPerSecond:              new(1000),
		TotalTimeout:                  new(int64(30000)),
		SocketTimeout:                 new(int64(10000)),
		Bandwidth:                     new(int64(50000000)),
		InfoTimeout:                   new(int64(5000)),
		InfoMaxRetries:                new(uint(3)),
		InfoRetriesMultiplier:         new(1.5),
		InfoRetryIntervalMilliseconds: new(int64(1000)),
		StdBufferSize:                 new(4096),
		InputFile:                     new("input.asb"),
		DirectoryList:                 []string{"dir1", "dir2"},
		ParentDirectory:               new("/parent"),
		DisableBatchWrites:            new(true),
		BatchSize:                     new(100),
		MaxAsyncBatches:               new(32),
		WarmUp:                        new(10),
		ExtraTTL:                      new(int64(3600)),
		IgnoreRecordError:             new(true),
		Uniq:                          new(true),
		Replace:                       new(false),
		NoGeneration:                  new(true),
		RetryBaseInterval:             new(int64(500)),
		RetryMultiplier:               new(2.0),
		RetryMaxAttempts:              new(uint(10)),
		ValidateOnly:                  new(false),
		ApplyMetadataLast:             new(true),
	}

	restore := &Restore{Restore: config}
	model := restore.ToModelRestore()

	require.NotNil(t, model)

	assert.Equal(t, "/restore", model.Directory)
	assert.Equal(t, "test", model.Namespace)
	assert.Equal(t, "set1,set2", model.Sets)
	assert.Equal(t, "bin1,bin2", model.BinList)
	assert.Equal(t, 8, model.Parallel)
	assert.False(t, model.NoRecords)
	assert.True(t, model.NoIndexes)
	assert.False(t, model.NoUDFs)
	assert.Equal(t, 1000, model.RecordsPerSecond)
	assert.Equal(t, int64(30000), model.TotalTimeout)
	assert.Equal(t, int64(10000), model.SocketTimeout)
	assert.Equal(t, int64(50000000), model.Bandwidth)
	assert.Equal(t, int64(5000), model.InfoTimeout)
	assert.Equal(t, uint(3), model.InfoMaxRetries)
	assert.InEpsilon(t, 1.5, model.InfoRetriesMultiplier, 0.0)
	assert.Equal(t, int64(1000), model.InfoRetryIntervalMilliseconds)
	assert.Equal(t, 4096, model.StdBufferSize)
	assert.Equal(t, "input.asb", model.InputFile)
	assert.Equal(t, "dir1,dir2", model.DirectoryList)
	assert.Equal(t, "/parent", model.ParentDirectory)
	assert.True(t, model.DisableBatchWrites)
	assert.Equal(t, 100, model.BatchSize)
	assert.Equal(t, 32, model.MaxAsyncBatches)
	assert.Equal(t, 10, model.WarmUp)
	assert.Equal(t, int64(3600), model.ExtraTTL)
	assert.True(t, model.IgnoreRecordError)
	assert.True(t, model.Uniq)
	assert.False(t, model.Replace)
	assert.True(t, model.NoGeneration)
	assert.Equal(t, int64(500), model.RetryBaseInterval)
	assert.InEpsilon(t, 2.0, model.RetryMultiplier, 0.0)
	assert.Equal(t, uint(10), model.RetryMaxAttempts)
	assert.False(t, model.ValidateOnly)
	assert.True(t, model.ApplyMetadataLast)
}

func TestRestore_ToModelRestore_NilHandling(t *testing.T) {
	t.Run("nil restore", func(t *testing.T) {
		var r *Restore
		assert.Nil(t, r.ToModelRestore())
	})
}

func TestRestore_ToModelRestore_EmptyLists(t *testing.T) {
	config := RestoreConfig{
		SetList:       []string{},
		BinList:       []string{},
		DirectoryList: []string{},
	}

	restore := &Restore{Restore: config}
	model := restore.ToModelRestore()

	require.NotNil(t, model)
	assert.Empty(t, model.Sets)
	assert.Empty(t, model.BinList)
	assert.Empty(t, model.DirectoryList)
}

func TestRestore_ToModelRestore_DefaultToModel(t *testing.T) {
	restore := DefaultRestore()
	model := restore.ToModelRestore()

	require.NotNil(t, model)

	assert.Equal(t, models.DefaultCommonDirectory, model.Directory)
	assert.Equal(t, models.DefaultCommonNamespace, model.Namespace)
	assert.Equal(t, models.DefaultRestoreParallel, model.Parallel)
	assert.Equal(t, models.DefaultCommonNoRecords, model.NoRecords)
	assert.Equal(t, models.DefaultCommonNoIndexes, model.NoIndexes)
	assert.Equal(t, models.DefaultCommonNoUDFs, model.NoUDFs)
	assert.Equal(t, models.DefaultCommonRecordsPerSecond, model.RecordsPerSecond)
	assert.Equal(t, models.DefaultRestoreTotalTimeout, model.TotalTimeout)
	assert.Equal(t, models.DefaultCommonSocketTimeout, model.SocketTimeout)
	assert.Equal(t, models.DefaultCommonBandwidth, model.Bandwidth)
	assert.Equal(t, models.DefaultCommonInfoTimeout, model.InfoTimeout)
	assert.Equal(t, models.DefaultCommonInfoMaxRetries, model.InfoMaxRetries)
	assert.InEpsilon(t, models.DefaultCommonInfoRetriesMultiplier, model.InfoRetriesMultiplier, 0.0)
	assert.Equal(t, models.DefaultCommonInfoRetryInterval, model.InfoRetryIntervalMilliseconds)
	assert.Equal(t, models.DefaultCommonStdBufferSize, model.StdBufferSize)
	assert.Equal(t, models.DefaultRestoreInputFile, model.InputFile)
	assert.Equal(t, models.DefaultRestoreParentDirectory, model.ParentDirectory)
	assert.Equal(t, models.DefaultRestoreDisableBatchWrites, model.DisableBatchWrites)
	assert.Equal(t, models.DefaultRestoreBatchSize, model.BatchSize)
	assert.Equal(t, models.DefaultRestoreMaxAsyncBatches, model.MaxAsyncBatches)
	assert.Equal(t, models.DefaultRestoreWarmUp, model.WarmUp)
	assert.Equal(t, models.DefaultRestoreExtraTTL, model.ExtraTTL)
	assert.Equal(t, models.DefaultRestoreIgnoreRecordError, model.IgnoreRecordError)
	assert.Equal(t, models.DefaultRestoreUniq, model.Uniq)
	assert.Equal(t, models.DefaultRestoreReplace, model.Replace)
	assert.Equal(t, models.DefaultRestoreNoGeneration, model.NoGeneration)
	assert.Equal(t, models.DefaultRestoreRetryBaseInterval, model.RetryBaseInterval)
	assert.InEpsilon(t, models.DefaultRestoreRetryMultiplier, model.RetryMultiplier, 0.0)
	assert.Equal(t, models.DefaultRestoreRetryMaxAttempts, model.RetryMaxAttempts)
	assert.Equal(t, models.DefaultRestoreValidateOnly, model.ValidateOnly)
	assert.Equal(t, models.DefaultRestoreApplyMetadataLast, model.ApplyMetadataLast)
}
