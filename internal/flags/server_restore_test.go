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
	// testRestoreBackupIDs holds several ids, so that the flag is shown to carry the
	// list through to the model verbatim.
	testRestoreBackupIDs = "backup-job-1,backup-job-2"
	testRestoreJobID     = "restore-job-1"
	flagRestoreNamespace = "--namespace"
	flagRestoreJobID     = "--job-id"
	flagAsync            = "--async"
)

func TestServerRestore_NewFlagSet(t *testing.T) {
	t.Parallel()

	const (
		testPath              = "backups/daily"
		testSetList           = "set1,set2"
		testFilterExp         = "kwGTUQKkYmluMQE="
		testParallelWrite     = "16"
		testRecordsPerSecond  = "1000"
		testMaxInflight       = "500"
		testRetryBaseInterval = "2000"
		testRetryMultiplier   = "1.5"
		testRetryMaxAttempts  = "3"
		testParallelRead      = "32"
		testIncrementalPath   = "backups/incr"
		testExtraTTL          = "3600"
		testPolicy            = "fuzzy"
	)

	want := &models.ServerRestore{
		Namespace:         testRestoreNamespace,
		BackupIDs:         testRestoreBackupIDs,
		JobID:             testRestoreJobID,
		Path:              testPath,
		SetList:           testSetList,
		FilterExp:         testFilterExp,
		NoIndexes:         true,
		NoUDFs:            true,
		Policy:            testPolicy,
		AllowUnhosted:     true,
		ParallelWrite:     16,
		RecordsPerSecond:  1000,
		MaxInflight:       500,
		RetryBaseInterval: 2000,
		RetryMultiplier:   1.5,
		RetryMaxAttempts:  3,
		IgnoreRecordError: true,
		ApplyMetadataLast: true,
		IncrementalPath:   testIncrementalPath,
		Async:             true,
		ExtraTTL:          3600,

		ParallelRead: 32,
	}

	commonArgs := []string{
		"--backup-ids", testRestoreBackupIDs,
		flagRestoreJobID, testRestoreJobID,
		"--path", testPath,
		"--filter-exp", testFilterExp,
		"--no-udfs",
		"--policy", testPolicy,
		"--allow-unhosted",
		"--max-inflight", testMaxInflight,
		"--retry-base-interval", testRetryBaseInterval,
		"--retry-multiplier", testRetryMultiplier,
		"--retry-max-attempts", testRetryMaxAttempts,
		"--ignore-record-error",
		"--parallel-read", testParallelRead,
		"--apply-metadata-last",
		"--incremental-path", testIncrementalPath,
		flagAsync,
		"--extra-ttl", testExtraTTL,
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
				"--parallel-write", testParallelWrite,
				"--records-per-second", testRecordsPerSecond,
			}, commonArgs...),
		},
		{
			name: "short flags",
			args: append([]string{
				"-n", testRestoreNamespace,
				"-s", testSetList,
				"-I",
				"-w", testParallelWrite,
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
	assert.Equal(t, models.DefaultServerBackupJobID, result.BackupIDs)
	assert.Equal(t, models.DefaultServerRestoreJobID, result.JobID)
	assert.Equal(t, models.DefaultServerBackupPath, result.Path)
	assert.Equal(t, models.DefaultCommonSetList, result.SetList)
	assert.Equal(t, models.DefaultServerFilterExp, result.FilterExp)
	assert.Equal(t, models.DefaultCommonNoIndexes, result.NoIndexes)
	assert.Equal(t, models.DefaultCommonNoUDFs, result.NoUDFs)
	assert.Equal(t, models.DefaultServerRestorePolicy, result.Policy)
	assert.Equal(t, models.DefaultServerRestoreAllowUnhosted, result.AllowUnhosted)
	assert.Equal(t, models.DefaultServerRestoreParallelWrite, result.ParallelWrite)
	assert.Equal(t, models.DefaultServerRestoreRecordsPerSecond, result.RecordsPerSecond)
	assert.Equal(t, models.DefaultServerRestoreMaxInflight, result.MaxInflight)
	assert.Equal(t, models.DefaultServerRestoreRetryBaseInterval, result.RetryBaseInterval)
	assert.InDelta(t, models.DefaultServerRestoreRetryMultiplier, result.RetryMultiplier, 0)
	assert.Equal(t, models.DefaultServerRestoreRetryMaxAttempts, result.RetryMaxAttempts)
	assert.Equal(t, models.DefaultServerRestoreIgnoreRecordError, result.IgnoreRecordError)
	assert.Equal(t, models.DefaultServerRestoreParallelRead, result.ParallelRead)
	assert.Equal(t, models.DefaultServerRestoreApplyMetadataLast, result.ApplyMetadataLast)
	assert.Equal(t, models.DefaultServerRestoreIncrementalPath, result.IncrementalPath)
	assert.Equal(t, models.DefaultServerRestoreAsync, result.Async)
	assert.Equal(t, models.DefaultServerRestoreExtraTTL, result.ExtraTTL)
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
			name:  "start",
			parse: NewServerRestore().NewFlagSet().Parse,
		},
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

			require.Error(t, tt.parse([]string{"--backup-id", testRestoreBackupIDs}))
		})
	}
}

// TestServerRestoreJob_NewFlagSet covers the commands that name a restore job and take a
// single switch on top of it: --watch for progress, --async for prepare, abort, open and
// clear-storage.
func TestServerRestoreJob_NewFlagSet(t *testing.T) {
	t.Parallel()

	// jobFlags is what the commands share, read back from the model of each command.
	type jobFlags struct {
		namespace string
		jobID     string
		toggle    bool
	}

	commands := []struct {
		name       string
		toggleFlag string
		parse      func(args []string) (jobFlags, error)
	}{
		{
			name:       "progress",
			toggleFlag: "--watch",
			parse: func(args []string) (jobFlags, error) {
				f := NewServerRestoreProgress()
				err := f.NewFlagSet().Parse(args)
				m := f.GetServerRestoreProgress()

				return jobFlags{namespace: m.Namespace, jobID: m.JobID, toggle: m.Watch}, err
			},
		},
		{
			name:       "prepare",
			toggleFlag: flagAsync,
			parse: func(args []string) (jobFlags, error) {
				f := NewServerRestorePrepare()
				err := f.NewFlagSet().Parse(args)
				m := f.GetServerRestorePrepare()

				return jobFlags{namespace: m.Namespace, jobID: m.JobID, toggle: m.Async}, err
			},
		},
		{
			name:       "abort",
			toggleFlag: flagAsync,
			parse: func(args []string) (jobFlags, error) {
				f := NewServerRestoreAbort()
				err := f.NewFlagSet().Parse(args)
				m := f.GetServerRestoreAbort()

				return jobFlags{namespace: m.Namespace, jobID: m.JobID, toggle: m.Async}, err
			},
		},
		{
			name:       "open",
			toggleFlag: flagAsync,
			parse: func(args []string) (jobFlags, error) {
				f := NewServerRestoreOpen()
				err := f.NewFlagSet().Parse(args)
				m := f.GetServerRestoreOpen()

				return jobFlags{namespace: m.Namespace, jobID: m.JobID, toggle: m.Async}, err
			},
		},
		{
			name:       "clear storage",
			toggleFlag: flagAsync,
			parse: func(args []string) (jobFlags, error) {
				f := NewServerRestoreClearStorage()
				err := f.NewFlagSet().Parse(args)
				m := f.GetServerRestoreClearStorage()

				return jobFlags{namespace: m.Namespace, jobID: m.JobID, toggle: m.Async}, err
			},
		},
	}

	tests := []struct {
		name      string
		args      func(toggleFlag string) []string
		wantFlags jobFlags
	}{
		{
			name: "no arguments keeps the defaults",
			args: func(string) []string { return nil },
			wantFlags: jobFlags{
				namespace: models.DefaultCommonNamespace,
				jobID:     models.DefaultServerRestoreJobID,
			},
		},
		{
			name: "long flags",
			args: func(toggleFlag string) []string {
				return []string{flagRestoreNamespace, testRestoreNamespace, flagRestoreJobID, testRestoreJobID, toggleFlag}
			},
			wantFlags: jobFlags{namespace: testRestoreNamespace, jobID: testRestoreJobID, toggle: true},
		},
		{
			name: "short namespace flag",
			args: func(string) []string { return []string{"-n", testRestoreNamespace} },
			wantFlags: jobFlags{
				namespace: testRestoreNamespace,
				jobID:     models.DefaultServerRestoreJobID,
			},
		},
	}

	for _, cmd := range commands {
		for _, tt := range tests {
			t.Run(cmd.name+"/"+tt.name, func(t *testing.T) {
				t.Parallel()

				got, err := cmd.parse(tt.args(cmd.toggleFlag))
				require.NoError(t, err)

				assert.Equal(t, tt.wantFlags, got)
			})
		}
	}
}
