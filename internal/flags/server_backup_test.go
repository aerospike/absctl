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

package flags

import (
	"testing"

	"github.com/aerospike/absctl/internal/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testBackupJobID     = "backup-job-1"
	testBackupKeyPrefix = "backups/daily"
)

func TestServerBackup_NewFlagSet(t *testing.T) {
	t.Parallel()

	backup := NewServerBackup()
	flagSet := backup.NewFlagSet()

	args := []string{
		"--namespace", "test-ns",
		"--object-storage-type", "aws-s3",
		"--modified-after", "2023-09-02_12:00:00",
		"--modified-before", "2023-09-03_12:00:00",
	}

	err := flagSet.Parse(args)
	require.NoError(t, err)

	result := backup.GetServerBackup()

	assert.Equal(t, "test-ns", result.Namespace)
	assert.Equal(t, "aws-s3", result.StorageType)
	assert.Equal(t, "2023-09-02_12:00:00", result.ModifiedAfter)
	assert.Equal(t, "2023-09-03_12:00:00", result.ModifiedBefore)
}

func TestServerBackup_NewFlagSet_ShortFlags(t *testing.T) {
	t.Parallel()

	backup := NewServerBackup()
	flagSet := backup.NewFlagSet()

	args := []string{
		"-a", "2023-09-02_12:00:00",
		"-b", "2023-09-03_12:00:00",
	}

	err := flagSet.Parse(args)
	require.NoError(t, err)

	result := backup.GetServerBackup()

	assert.Equal(t, "2023-09-02_12:00:00", result.ModifiedAfter)
	assert.Equal(t, "2023-09-03_12:00:00", result.ModifiedBefore)
}

func TestServerBackup_NewFlagSet_DefaultValues(t *testing.T) {
	t.Parallel()

	backup := NewServerBackup()
	flagSet := backup.NewFlagSet()

	err := flagSet.Parse([]string{})
	require.NoError(t, err)

	result := backup.GetServerBackup()

	assert.Equal(t, models.DefaultCommonNamespace, result.Namespace)
	assert.Equal(t, models.DefaultServerBackupObjectStorageType, result.StorageType)
	assert.Equal(t, models.DefaultBackupModifiedAfter, result.ModifiedAfter)
	assert.Equal(t, models.DefaultBackupModifiedBefore, result.ModifiedBefore)
	assert.Equal(t, models.DefaultServerBackupPath, result.Path)
	assert.Equal(t, models.DefaultCommonSetList, result.SetList)
	assert.Equal(t, models.DefaultCommonBinList, result.BinList)
	assert.Equal(t, models.DefaultServerFilterExp, result.FilterExp)
	assert.Equal(t, models.DefaultCommonNoIndexes, result.NoIndexes)
	assert.Equal(t, models.DefaultCommonNoUDFs, result.NoUDFs)
	assert.Equal(t, models.DefaultServerBackupAsync, result.Async)
}

func TestServerBackup_NewFlagSet_RequestFields(t *testing.T) {
	t.Parallel()

	const (
		testNamespace = "test-ns"
		testPath      = "backups/daily"
		testSetList   = "set1,set2"
		testBinList   = "bin1,bin2"
		testFilterExp = "kwGTUQKkYmluMQE="
	)

	want := &models.ServerBackup{
		Namespace: testNamespace,
		Path:      testPath,
		SetList:   testSetList,
		BinList:   testBinList,
		FilterExp: testFilterExp,
		NoIndexes: true,
	}

	tests := []struct {
		name string
		args []string
	}{
		{
			name: "long flags",
			args: []string{
				"--namespace", testNamespace,
				"--path", testPath,
				"--set-list", testSetList,
				"--bin-list", testBinList,
				"--filter-exp", testFilterExp,
				"--no-indexes",
			},
		},
		{
			// The short flags match the ones of the scan backup, so that the same
			// muscle memory works for both.
			name: "short flags",
			args: []string{
				"-n", testNamespace,
				"--path", testPath,
				"-s", testSetList,
				"-B", testBinList,
				"-f", testFilterExp,
				"-I",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backup := NewServerBackup()
			require.NoError(t, backup.NewFlagSet().Parse(tt.args))

			assert.Equal(t, want, backup.GetServerBackup())
		})
	}
}

func TestServerBackup_NewFlagSet_EnableChangeStreamRemoved(t *testing.T) {
	t.Parallel()

	err := NewServerBackup().NewFlagSet().Parse([]string{"--enable-change-stream"})
	require.Error(t, err)
}

func TestServerBackup_NewFlagSet_Async(t *testing.T) {
	t.Parallel()

	backup := NewServerBackup()
	flagSet := backup.NewFlagSet()

	err := flagSet.Parse([]string{"--async"})
	require.NoError(t, err)

	assert.True(t, backup.GetServerBackup().Async)
}

func TestServerBackupList_NewFlagSet(t *testing.T) {
	t.Parallel()

	list := NewServerBackupList()
	flagSet := list.NewFlagSet()

	err := flagSet.Parse([]string{"--path", "/backups"})
	require.NoError(t, err)

	result := list.GetServerBackupList()

	assert.Equal(t, "/backups", result.Path)
}

func TestServerBackupList_NewFlagSet_DefaultValues(t *testing.T) {
	t.Parallel()

	list := NewServerBackupList()
	flagSet := list.NewFlagSet()

	err := flagSet.Parse([]string{})
	require.NoError(t, err)

	result := list.GetServerBackupList()

	assert.Equal(t, models.DefaultServerBackupPath, result.Path)
}

func TestServerBackupValidate_NewFlagSet(t *testing.T) {
	t.Parallel()

	validate := NewServerBackupValidate()
	flagSet := validate.NewFlagSet()

	args := []string{
		"--sample-size", "5000",
		"--backup-id", testBackupJobID,
		"--path", testBackupKeyPrefix,
	}

	err := flagSet.Parse(args)
	require.NoError(t, err)

	result := validate.GetServerBackupValidate()

	assert.Equal(t, 5000, result.SampleSize)
	assert.Equal(t, testBackupJobID, result.JobID)
	assert.Equal(t, testBackupKeyPrefix, result.Path)
}

func TestServerBackupValidate_NewFlagSet_DefaultValues(t *testing.T) {
	t.Parallel()

	validate := NewServerBackupValidate()
	flagSet := validate.NewFlagSet()

	err := flagSet.Parse([]string{})
	require.NoError(t, err)

	result := validate.GetServerBackupValidate()

	assert.Equal(t, models.DefaultServerBackupValidateSampleSize, result.SampleSize)
	assert.Equal(t, models.DefaultServerBackupJobID, result.JobID)
	assert.Equal(t, models.DefaultServerBackupPath, result.Path)
}

func TestServerBackupProgress_NewFlagSet(t *testing.T) {
	t.Parallel()

	progress := NewServerBackupProgress()
	flagSet := progress.NewFlagSet()

	args := []string{
		"--backup-id", testBackupJobID,
		"--path", testBackupKeyPrefix,
		"--watch",
	}

	err := flagSet.Parse(args)
	require.NoError(t, err)

	result := progress.GetServerBackupProgress()

	assert.Equal(t, testBackupJobID, result.JobID)
	assert.Equal(t, testBackupKeyPrefix, result.Path)
	assert.True(t, result.Watch)
}

func TestServerBackupAbort_NewFlagSet(t *testing.T) {
	t.Parallel()

	abort := NewServerBackupAbort()
	flagSet := abort.NewFlagSet()

	err := flagSet.Parse([]string{"--backup-id", testBackupJobID})
	require.NoError(t, err)

	result := abort.GetServerBackupAbort()

	assert.Equal(t, testBackupJobID, result.JobID)
}

func TestServerBackupAbort_NewFlagSet_DefaultValues(t *testing.T) {
	t.Parallel()

	abort := NewServerBackupAbort()
	flagSet := abort.NewFlagSet()

	err := flagSet.Parse([]string{})
	require.NoError(t, err)

	result := abort.GetServerBackupAbort()

	assert.Equal(t, models.DefaultServerBackupJobID, result.JobID)
}

func TestServerBackupProgress_NewFlagSet_DefaultValues(t *testing.T) {
	t.Parallel()

	progress := NewServerBackupProgress()
	flagSet := progress.NewFlagSet()

	err := flagSet.Parse([]string{})
	require.NoError(t, err)

	result := progress.GetServerBackupProgress()

	assert.Equal(t, models.DefaultServerBackupJobID, result.JobID)
	assert.Equal(t, models.DefaultServerBackupPath, result.Path)
	assert.Equal(t, models.DefaultServerBackupProgressWatch, result.Watch)
}
