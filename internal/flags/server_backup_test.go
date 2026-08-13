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

package flags

import (
	"testing"

	"github.com/aerospike/absctl/internal/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

	assert.Empty(t, result.Namespace)
	assert.Empty(t, result.StorageType)
	assert.Equal(t, models.DefaultBackupModifiedAfter, result.ModifiedAfter)
	assert.Equal(t, models.DefaultBackupModifiedBefore, result.ModifiedBefore)
}

func TestServerBackupList_NewFlagSet(t *testing.T) {
	t.Parallel()

	list := NewServerBackupList()
	flagSet := list.NewFlagSet()

	err := flagSet.Parse([]string{"--path", "/backups"})
	require.NoError(t, err)

	result := list.GetServerBackupList()

	assert.Equal(t, "/backups", result.ListPath)
}

func TestServerBackupList_NewFlagSet_DefaultValues(t *testing.T) {
	t.Parallel()

	list := NewServerBackupList()
	flagSet := list.NewFlagSet()

	err := flagSet.Parse([]string{})
	require.NoError(t, err)

	result := list.GetServerBackupList()

	assert.Equal(t, "/", result.ListPath)
}

func TestServerBackupValidate_NewFlagSet(t *testing.T) {
	t.Parallel()

	validate := NewServerBackupValidate()
	flagSet := validate.NewFlagSet()

	args := []string{
		"--sample-size", "5000",
		"--backup-id", "backup-job-1",
	}

	err := flagSet.Parse(args)
	require.NoError(t, err)

	result := validate.GetServerBackupValidate()

	assert.Equal(t, 5000, result.SampleSize)
	assert.Equal(t, "backup-job-1", result.JobID)
}

func TestServerBackupValidate_NewFlagSet_DefaultValues(t *testing.T) {
	t.Parallel()

	validate := NewServerBackupValidate()
	flagSet := validate.NewFlagSet()

	err := flagSet.Parse([]string{})
	require.NoError(t, err)

	result := validate.GetServerBackupValidate()

	assert.Equal(t, 10000, result.SampleSize)
	assert.Empty(t, result.JobID)
}
