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
	testRestoreNamespace = "test-ns"
	testRestoreBackupID  = "backup-job-1"
	testRestoreJobID     = "restore-job-1"
	flagRestoreNamespace = "--namespace"
	flagRestoreJobID     = "--job-id"
)

func TestServerRestore_NewFlagSet(t *testing.T) {
	t.Parallel()

	const (
		testStorage           = "aws-s3"
		testPath              = "backups/daily"
		testSetList           = "set1,set2"
		testFilterExp         = "kwGTUQKkYmluMQE="
		testParallel          = "16"
		testRecordsPerSecond  = "1000"
		testMaxInflight       = "500"
		testRetryBaseInterval = "2000"
		testRetryMultiplier   = "1.5"
		testRetryMaxAttempts  = "3"
	)

	want := &models.ServerRestore{
		Namespace:         testRestoreNamespace,
		StorageType:       testStorage,
		BackupID:          testRestoreBackupID,
		JobID:             testRestoreJobID,
		Path:              testPath,
		SetList:           testSetList,
		FilterExp:         testFilterExp,
		NoIndexes:         true,
		NoUDFs:            true,
		FuzzyRestore:      true,
		AllowUnhosted:     true,
		Parallel:          16,
		RecordsPerSecond:  1000,
		MaxInflight:       500,
		RetryBaseInterval: 2000,
		RetryMultiplier:   1.5,
		RetryMaxAttempts:  3,
		IgnoreRecordError: true,
	}

	commonArgs := []string{
		"--object-storage-type", testStorage,
		"--backup-id", testRestoreBackupID,
		flagRestoreJobID, testRestoreJobID,
		"--path", testPath,
		"--filter-exp", testFilterExp,
		"--no-udfs",
		"--fuzzy-restore",
		"--allow-unhosted",
		"--max-inflight", testMaxInflight,
		"--retry-base-interval", testRetryBaseInterval,
		"--retry-multiplier", testRetryMultiplier,
		"--retry-max-attempts", testRetryMaxAttempts,
		"--ignore-record-error",
	}

	tests := []struct {
		name string
		args []string
	}{
		{
			name: "long flags",
			args: append([]string{
				flagRestoreNamespace, testRestoreNamespace,
				"--set-list", testSetList,
				"--no-indexes",
				"--parallel", testParallel,
				"--records-per-second", testRecordsPerSecond,
			}, commonArgs...),
		},
		{
			name: "short flags",
			args: append([]string{
				"-n", testRestoreNamespace,
				"-s", testSetList,
				"-I",
				"-w", testParallel,
				"-L", testRecordsPerSecond,
			}, commonArgs...),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			restore := NewServerRestore()
			require.NoError(t, restore.NewFlagSet().Parse(tt.args))

			assert.Equal(t, want, restore.GetServerRestore())
		})
	}
}

func TestServerRestore_NewFlagSet_DefaultValues(t *testing.T) {
	t.Parallel()

	restore := NewServerRestore()
	flagSet := restore.NewFlagSet()

	err := flagSet.Parse([]string{})
	require.NoError(t, err)

	result := restore.GetServerRestore()

	assert.Equal(t, models.DefaultCommonNamespace, result.Namespace)
	assert.Equal(t, models.DefaultServerBackupObjectStorageType, result.StorageType)
	assert.Equal(t, models.DefaultServerBackupJobID, result.BackupID)
	assert.Equal(t, models.DefaultServerRestoreJobID, result.JobID)
	assert.Equal(t, models.DefaultServerBackupPath, result.Path)
	assert.Equal(t, models.DefaultCommonSetList, result.SetList)
	assert.Equal(t, models.DefaultServerFilterExp, result.FilterExp)
	assert.Equal(t, models.DefaultCommonNoIndexes, result.NoIndexes)
	assert.Equal(t, models.DefaultCommonNoUDFs, result.NoUDFs)
	assert.Equal(t, models.DefaultServerRestoreFuzzyRestore, result.FuzzyRestore)
	assert.Equal(t, models.DefaultServerRestoreAllowUnhosted, result.AllowUnhosted)
	assert.Equal(t, models.DefaultServerRestoreParallel, result.Parallel)
	assert.Equal(t, models.DefaultServerRestoreRecordsPerSecond, result.RecordsPerSecond)
	assert.Equal(t, models.DefaultServerRestoreMaxInflight, result.MaxInflight)
	assert.Equal(t, models.DefaultServerRestoreRetryBaseInterval, result.RetryBaseInterval)
	assert.InDelta(t, models.DefaultServerRestoreRetryMultiplier, result.RetryMultiplier, 0)
	assert.Equal(t, models.DefaultServerRestoreRetryMaxAttempts, result.RetryMaxAttempts)
	assert.Equal(t, models.DefaultServerRestoreIgnoreRecordError, result.IgnoreRecordError)
}

func TestServerRestorePrepare_NewFlagSet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name               string
		args               []string
		wantNamespace      string
		wantJobID          string
		wantHydrateReplica bool
	}{
		{
			name:               "no arguments keeps the defaults",
			args:               []string{},
			wantNamespace:      models.DefaultCommonNamespace,
			wantJobID:          models.DefaultServerRestoreJobID,
			wantHydrateReplica: models.DefaultServerRestoreHydrateReplica,
		},
		{
			name:               "long flags",
			args:               []string{flagRestoreNamespace, testRestoreNamespace, flagRestoreJobID, testRestoreJobID},
			wantNamespace:      testRestoreNamespace,
			wantJobID:          testRestoreJobID,
			wantHydrateReplica: models.DefaultServerRestoreHydrateReplica,
		},
		{
			name:               "short namespace flag",
			args:               []string{"-n", testRestoreNamespace},
			wantNamespace:      testRestoreNamespace,
			wantJobID:          models.DefaultServerRestoreJobID,
			wantHydrateReplica: models.DefaultServerRestoreHydrateReplica,
		},
		{
			name:               "master only",
			args:               []string{"--hydrate-replica=false"},
			wantNamespace:      models.DefaultCommonNamespace,
			wantJobID:          models.DefaultServerRestoreJobID,
			wantHydrateReplica: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			prepare := NewServerRestorePrepare()
			require.NoError(t, prepare.NewFlagSet().Parse(tt.args))

			result := prepare.GetServerRestorePrepare()

			assert.Equal(t, tt.wantNamespace, result.Namespace)
			assert.Equal(t, tt.wantJobID, result.JobID)
			assert.Equal(t, tt.wantHydrateReplica, result.HydrateReplica)
		})
	}
}

func TestServerRestoreAbort_NewFlagSet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		args          []string
		wantNamespace string
		wantJobID     string
	}{
		{
			name:          "no arguments keeps the defaults",
			args:          []string{},
			wantNamespace: models.DefaultCommonNamespace,
			wantJobID:     models.DefaultServerRestoreJobID,
		},
		{
			// A restore job is named by both, so both flags have to reach the model.
			name:          "namespace and job id",
			args:          []string{flagRestoreNamespace, testRestoreNamespace, flagRestoreJobID, testRestoreJobID},
			wantNamespace: testRestoreNamespace,
			wantJobID:     testRestoreJobID,
		},
		{
			name:          "only namespace",
			args:          []string{"-n", testRestoreNamespace},
			wantNamespace: testRestoreNamespace,
			wantJobID:     models.DefaultServerRestoreJobID,
		},
		{
			name:          "only job id",
			args:          []string{flagRestoreJobID, testRestoreJobID},
			wantNamespace: models.DefaultCommonNamespace,
			wantJobID:     testRestoreJobID,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			abort := NewServerRestoreAbort()
			flagSet := abort.NewFlagSet()

			require.NoError(t, flagSet.Parse(tt.args))

			result := abort.GetServerRestoreAbort()

			assert.Equal(t, tt.wantNamespace, result.Namespace)
			assert.Equal(t, tt.wantJobID, result.JobID)
		})
	}
}

func TestServerRestore_NewFlagSet_BackupIDRemoved(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		parse func([]string) error
	}{
		{
			name:  "prepare",
			parse: NewServerRestorePrepare().NewFlagSet().Parse,
		},
		{
			name:  "abort",
			parse: NewServerRestoreAbort().NewFlagSet().Parse,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			require.Error(t, tt.parse([]string{"--backup-id", testRestoreBackupID}))
		})
	}
}
