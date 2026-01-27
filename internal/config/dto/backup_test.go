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

func TestDefaultBackup(t *testing.T) {
	backup := DefaultBackup()

	require.NotNil(t, backup)
	require.NotNil(t, backup.App)
	require.NotNil(t, backup.Cluster)
	require.NotNil(t, backup.Backup)
	require.NotNil(t, backup.Compression)
	require.NotNil(t, backup.Encryption)
	require.NotNil(t, backup.SecretAgent)
	require.NotNil(t, backup.Aws.S3)
	require.NotNil(t, backup.Gcp.Storage)
	require.NotNil(t, backup.Azure.Blob)
	require.NotNil(t, backup.Local.Disk)
}

func TestDefaultBackupConfig(t *testing.T) {
	config := defaultBackupConfig()

	assert.Equal(t, models.DefaultCommonDirectory, derefString(config.Directory))
	assert.Equal(t, models.DefaultCommonNamespace, derefString(config.Namespace))
	assert.Empty(t, config.SetList)
	assert.Empty(t, config.BinList)
	assert.Equal(t, models.DefaultBackupParallel, derefInt(config.Parallel))
	assert.Equal(t, models.DefaultCommonNoRecords, derefBool(config.NoRecords))
	assert.Equal(t, models.DefaultCommonNoIndexes, derefBool(config.NoIndexes))
	assert.Equal(t, models.DefaultCommonNoUDFs, derefBool(config.NoUDFs))
	assert.Equal(t, models.DefaultCommonRecordsPerSecond, derefInt(config.RecordsPerSecond))
	assert.Equal(t, models.DefaultBackupMaxRetries, derefInt(config.MaxRetries))
	assert.Equal(t, int64(models.DefaultCommonSocketTimeout), derefInt64(config.SocketTimeout))
	assert.Equal(t, int64(models.DefaultCommonInfoTimeout), derefInt64(config.InfoTimeout))
	assert.Equal(t, uint(models.DefaultCommonInfoMaxRetries), derefUint(config.InfoMaxRetries))
	assert.Equal(t, models.DefaultCommonInfoRetriesMultiplier, derefFloat64(config.InfoRetriesMultiplier))
	assert.Equal(t, int64(models.DefaultCommonInfoRetryInterval), derefInt64(config.InfoRetryIntervalMilliseconds))
	assert.Equal(t, int64(models.DefaultCommonBandwidth), derefInt64(config.Bandwidth))
	assert.Equal(t, models.DefaultCommonStdBufferSize, derefInt(config.StdBufferSize))
	assert.Equal(t, models.DefaultBackupOutputFile, derefString(config.OutputFile))
	assert.Equal(t, models.DefaultBackupRemoveFiles, derefBool(config.RemoveFiles))
	assert.Equal(t, models.DefaultBackupModifiedBefore, derefString(config.ModifiedBefore))
	assert.Equal(t, models.DefaultBackupModifiedAfter, derefString(config.ModifiedAfter))
	assert.Equal(t, uint64(models.DefaultBackupFileLimit), derefUint64(config.FileLimit))
	assert.Equal(t, models.DefaultBackupAfterDigest, derefString(config.AfterDigest))
	assert.Equal(t, int64(models.DefaultBackupMaxRecords), derefInt64(config.MaxRecords))
	assert.Equal(t, models.DefaultBackupNoBins, derefBool(config.NoBins))
	assert.Equal(t, models.DefaultBackupSleepBetweenRetries, derefInt(config.SleepBetweenRetries))
	assert.Equal(t, models.DefaultBackupFilterExpression, derefString(config.FilterExpression))
	assert.Equal(t, models.DefaultBackupRemoveArtifacts, derefBool(config.RemoveArtifacts))
	assert.Equal(t, models.DefaultBackupCompact, derefBool(config.Compact))
	assert.Empty(t, config.NodeList)
	assert.Equal(t, models.DefaultBackupNoTTLOnly, derefBool(config.NoTTLOnly))
	assert.Empty(t, config.PreferRacks)
	assert.Empty(t, config.PartitionList)
	assert.Equal(t, models.DefaultBackupEstimate, derefBool(config.Estimate))
	assert.Equal(t, int64(models.DefaultBackupEstimateSamples), derefInt64(config.EstimateSamples))
	assert.Equal(t, models.DefaultBackupStateFileDst, derefString(config.StateFileDst))
	assert.Equal(t, models.DefaultBackupContinue, derefString(config.Continue))
	assert.Equal(t, int64(models.DefaultBackupScanPageSize), derefInt64(config.ScanPageSize))
	assert.Equal(t, models.DefaultBackupOutputFilePrefix, derefString(config.OutputFilePrefix))
	assert.Empty(t, config.RackList)
	assert.Equal(t, int64(models.DefaultBackupTotalTimeout), derefInt64(config.TotalTimeout))
}

func TestBackupConfig_ToModelBackup(t *testing.T) {
	config := BackupConfig{
		Directory:                     stringPtr("/backup"),
		Namespace:                     stringPtr("test"),
		SetList:                       []string{"set1", "set2"},
		BinList:                       []string{"bin1", "bin2"},
		Parallel:                      intPtr(8),
		NoRecords:                     boolPtr(false),
		NoIndexes:                     boolPtr(true),
		NoUDFs:                        boolPtr(false),
		RecordsPerSecond:              intPtr(1000),
		MaxRetries:                    intPtr(5),
		TotalTimeout:                  int64Ptr(30000),
		SocketTimeout:                 int64Ptr(10000),
		Bandwidth:                     int64Ptr(50000000),
		InfoTimeout:                   int64Ptr(5000),
		InfoMaxRetries:                uintPtr(3),
		InfoRetriesMultiplier:         float64Ptr(1.5),
		InfoRetryIntervalMilliseconds: int64Ptr(1000),
		StdBufferSize:                 intPtr(4096),
		OutputFile:                    stringPtr("output.asb"),
		RemoveFiles:                   boolPtr(true),
		ModifiedBefore:                stringPtr("2024-01-01"),
		ModifiedAfter:                 stringPtr("2023-01-01"),
		FileLimit:                     uint64Ptr(100),
		AfterDigest:                   stringPtr("digest123"),
		MaxRecords:                    int64Ptr(1000000),
		NoBins:                        boolPtr(false),
		SleepBetweenRetries:           intPtr(2000),
		FilterExpression:              stringPtr("exp > 100"),
		RemoveArtifacts:               boolPtr(true),
		Compact:                       boolPtr(true),
		NodeList:                      []string{"node1", "node2"},
		NoTTLOnly:                     boolPtr(true),
		PreferRacks:                   []string{"rack1"},
		PartitionList:                 []string{"0", "1", "2"},
		Estimate:                      boolPtr(false),
		EstimateSamples:               int64Ptr(5000),
		StateFileDst:                  stringPtr("/state"),
		Continue:                      stringPtr("/cont"),
		ScanPageSize:                  int64Ptr(2500),
		OutputFilePrefix:              stringPtr("prefix-"),
		RackList:                      []string{"rack-a"},
	}

	backup := &Backup{Backup: config}
	model := backup.ToModelBackup()

	require.NotNil(t, model)

	assert.Equal(t, "/backup", model.Directory)
	assert.Equal(t, "test", model.Namespace)
	assert.Equal(t, "set1,set2", model.SetList)
	assert.Equal(t, "bin1,bin2", model.BinList)
	assert.Equal(t, 8, model.Parallel)
	assert.False(t, model.NoRecords)
	assert.True(t, model.NoIndexes)
	assert.False(t, model.NoUDFs)
	assert.Equal(t, 1000, model.RecordsPerSecond)
	assert.Equal(t, 5, model.MaxRetries)
	assert.Equal(t, int64(30000), model.TotalTimeout)
	assert.Equal(t, int64(10000), model.SocketTimeout)
	assert.Equal(t, int64(50000000), model.Bandwidth)
	assert.Equal(t, int64(5000), model.InfoTimeout)
	assert.Equal(t, uint(3), model.InfoMaxRetries)
	assert.Equal(t, 1.5, model.InfoRetriesMultiplier)
	assert.Equal(t, int64(1000), model.InfoRetryIntervalMilliseconds)
	assert.Equal(t, 4096, model.StdBufferSize)
	assert.Equal(t, "output.asb", model.OutputFile)
	assert.True(t, model.RemoveFiles)
	assert.Equal(t, "2024-01-01", model.ModifiedBefore)
	assert.Equal(t, "2023-01-01", model.ModifiedAfter)
	assert.Equal(t, uint64(100), model.FileLimit)
	assert.Equal(t, "digest123", model.AfterDigest)
	assert.Equal(t, int64(1000000), model.MaxRecords)
	assert.False(t, model.NoBins)
	assert.Equal(t, 2000, model.SleepBetweenRetries)
	assert.Equal(t, "exp > 100", model.FilterExpression)
	assert.True(t, model.RemoveArtifacts)
	assert.True(t, model.Compact)
	assert.Equal(t, "node1,node2", model.NodeList)
	assert.True(t, model.NoTTLOnly)
	assert.Equal(t, "rack1", model.PreferRacks)
	assert.Equal(t, "0,1,2", model.PartitionList)
	assert.False(t, model.Estimate)
	assert.Equal(t, int64(5000), model.EstimateSamples)
	assert.Equal(t, "/state", model.StateFileDst)
	assert.Equal(t, "/cont", model.Continue)
	assert.Equal(t, int64(2500), model.ScanPageSize)
	assert.Equal(t, "prefix-", model.OutputFilePrefix)
	assert.Equal(t, "rack-a", model.RackList)
}

func TestBackup_ToModelBackup_NilHandling(t *testing.T) {
	t.Run("nil backup", func(t *testing.T) {
		var b *Backup
		assert.Nil(t, b.ToModelBackup())
	})
}

func TestBackup_ToModelBackup_EmptyLists(t *testing.T) {
	config := BackupConfig{
		SetList:       []string{},
		BinList:       []string{},
		NodeList:      []string{},
		PreferRacks:   []string{},
		PartitionList: []string{},
		RackList:      []string{},
	}

	backup := &Backup{Backup: config}
	model := backup.ToModelBackup()

	require.NotNil(t, model)
	assert.Equal(t, "", model.SetList)
	assert.Equal(t, "", model.BinList)
	assert.Equal(t, "", model.NodeList)
	assert.Equal(t, "", model.PreferRacks)
	assert.Equal(t, "", model.PartitionList)
	assert.Equal(t, "", model.RackList)
}

func TestBackup_ToModelBackup_DefaultToModel(t *testing.T) {
	backup := DefaultBackup()
	model := backup.ToModelBackup()

	require.NotNil(t, model)

	assert.Equal(t, models.DefaultCommonDirectory, model.Directory)
	assert.Equal(t, models.DefaultCommonNamespace, model.Namespace)
	assert.Equal(t, models.DefaultBackupParallel, model.Parallel)
	assert.Equal(t, models.DefaultCommonNoRecords, model.NoRecords)
	assert.Equal(t, models.DefaultCommonNoIndexes, model.NoIndexes)
	assert.Equal(t, models.DefaultCommonNoUDFs, model.NoUDFs)
	assert.Equal(t, models.DefaultCommonRecordsPerSecond, model.RecordsPerSecond)
	assert.Equal(t, models.DefaultBackupMaxRetries, model.MaxRetries)
	assert.Equal(t, int64(models.DefaultBackupTotalTimeout), model.TotalTimeout)
	assert.Equal(t, int64(models.DefaultCommonSocketTimeout), model.SocketTimeout)
	assert.Equal(t, int64(models.DefaultCommonBandwidth), model.Bandwidth)
	assert.Equal(t, int64(models.DefaultCommonInfoTimeout), model.InfoTimeout)
	assert.Equal(t, uint(models.DefaultCommonInfoMaxRetries), model.InfoMaxRetries)
	assert.Equal(t, models.DefaultCommonInfoRetriesMultiplier, model.InfoRetriesMultiplier)
	assert.Equal(t, int64(models.DefaultCommonInfoRetryInterval), model.InfoRetryIntervalMilliseconds)
	assert.Equal(t, models.DefaultCommonStdBufferSize, model.StdBufferSize)
	assert.Equal(t, models.DefaultBackupOutputFile, model.OutputFile)
	assert.Equal(t, models.DefaultBackupRemoveFiles, model.RemoveFiles)
	assert.Equal(t, models.DefaultBackupModifiedBefore, model.ModifiedBefore)
	assert.Equal(t, models.DefaultBackupModifiedAfter, model.ModifiedAfter)
	assert.Equal(t, uint64(models.DefaultBackupFileLimit), model.FileLimit)
	assert.Equal(t, models.DefaultBackupAfterDigest, model.AfterDigest)
	assert.Equal(t, int64(models.DefaultBackupMaxRecords), model.MaxRecords)
	assert.Equal(t, models.DefaultBackupNoBins, model.NoBins)
	assert.Equal(t, models.DefaultBackupSleepBetweenRetries, model.SleepBetweenRetries)
	assert.Equal(t, models.DefaultBackupFilterExpression, model.FilterExpression)
	assert.Equal(t, models.DefaultBackupRemoveArtifacts, model.RemoveArtifacts)
	assert.Equal(t, models.DefaultBackupCompact, model.Compact)
	assert.Equal(t, models.DefaultBackupNoTTLOnly, model.NoTTLOnly)
	assert.Equal(t, models.DefaultBackupEstimate, model.Estimate)
	assert.Equal(t, int64(models.DefaultBackupEstimateSamples), model.EstimateSamples)
	assert.Equal(t, models.DefaultBackupStateFileDst, model.StateFileDst)
	assert.Equal(t, models.DefaultBackupContinue, model.Continue)
	assert.Equal(t, int64(models.DefaultBackupScanPageSize), model.ScanPageSize)
	assert.Equal(t, models.DefaultBackupOutputFilePrefix, model.OutputFilePrefix)
}
