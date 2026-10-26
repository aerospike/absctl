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

package models

import (
	"math"
	"testing"

	infomodels "github.com/aerospike/backup-go/pkg/asinfo/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testServerRestoreJobID = "restore-job-1"
	testServerSecondJobID  = "261025T130000-cd34"
	// testServerBackupIDs is the list form a multi-backup restore is given.
	testServerBackupIDs = testServerJobID + "," + testServerSecondJobID
	errMsgJobIDRequired = "job-id is required"
	errMsgRequiresFuzzy = "requires policy fuzzy"
	// errMsgAllowUnhostedRequiresFuzzy is reported for allow-unhosted without the fuzzy policy.
	errMsgAllowUnhostedRequiresFuzzy = "allow-unhosted " + errMsgRequiresFuzzy
	// errMsgRestorePolicy is the common part of the unknown restore policy errors.
	errMsgRestorePolicy = `policy must be strict or fuzzy, got "`
	// errMsgParallelRead is the common part of the parallel read range errors.
	errMsgParallelRead = "parallel-read must be between 1 and 128, got "
)

// validServerRestore returns a cold restore with every setting at its default, as the
// flags and the YAML config produce it.
func validServerRestore() *ServerRestore {
	return &ServerRestore{
		Namespace:         testServerNamespace,
		BackupIDs:         testServerJobID,
		ParallelRead:      DefaultServerRestoreParallelRead,
		AllowUnhosted:     DefaultServerRestoreAllowUnhosted,
		ParallelWrite:     DefaultServerRestoreParallelWrite,
		RecordsPerSecond:  DefaultServerRestoreRecordsPerSecond,
		MaxInflight:       DefaultServerRestoreMaxInflight,
		RetryBaseInterval: DefaultServerRestoreRetryBaseInterval,
		RetryMultiplier:   DefaultServerRestoreRetryMultiplier,
		RetryMaxAttempts:  DefaultServerRestoreRetryMaxAttempts,
		IgnoreRecordError: DefaultServerRestoreIgnoreRecordError,
	}
}

func validServerFuzzyRestore() *ServerRestore {
	restore := validServerRestore()
	restore.Policy = string(infomodels.RestorePolicyFuzzy)

	return restore
}

func validServerRestorePrepare() *ServerRestorePrepare {
	return &ServerRestorePrepare{
		Namespace:      testServerNamespace,
		JobID:          testServerRestoreJobID,
		HydrateReplica: DefaultServerRestoreHydrateReplica,
	}
}

func validServerRestoreAbort() *ServerRestoreAbort {
	return &ServerRestoreAbort{
		Namespace: testServerNamespace,
		JobID:     testServerRestoreJobID,
	}
}

func TestServerRestore_RestoreJobID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		giveJobID     string
		giveBackupIDs string
		want          string
	}{
		{
			name:          "job id set",
			giveJobID:     testServerRestoreJobID,
			giveBackupIDs: testServerJobID,
			want:          testServerRestoreJobID,
		},
		{
			name:          "job id defaults to backup id",
			giveBackupIDs: testServerJobID,
			want:          testServerJobID,
		},
		{
			// Validate rejects the empty job id of a multi-backup restore, so only the
			// explicit id can reach the request here.
			name:          "job id set with several backup ids",
			giveJobID:     testServerRestoreJobID,
			giveBackupIDs: testServerBackupIDs,
			want:          testServerRestoreJobID,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			restore := &ServerRestore{JobID: tt.giveJobID, BackupIDs: tt.giveBackupIDs}

			assert.Equal(t, tt.want, restore.RestoreJobID())
		})
	}
}

func TestServerRestore_BackupIDList(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		giveBackupIDs string
		want          []string
	}{
		{
			name:          "single id",
			giveBackupIDs: testServerJobID,
			want:          []string{testServerJobID},
		},
		{
			name:          "several ids",
			giveBackupIDs: testServerBackupIDs,
			want:          []string{testServerJobID, testServerSecondJobID},
		},
		{
			name: "empty",
			want: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			restore := &ServerRestore{BackupIDs: tt.giveBackupIDs}

			assert.Equal(t, tt.want, restore.BackupIDList())
		})
	}
}

func TestServerRestore_ValidateRestoreMode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		restore    func() *ServerRestore
		wantErrMsg string
	}{
		{
			name:    "valid fuzzy restore with defaults",
			restore: validServerFuzzyRestore,
		},
		{
			name: "valid fuzzy restore at the limits",
			restore: func() *ServerRestore {
				restore := validServerFuzzyRestore()
				restore.AllowUnhosted = true
				restore.ParallelWrite = maxServerRestoreParallelWrite
				restore.MaxInflight = maxServerRestoreMaxInflight
				restore.RetryMultiplier = minServerRestoreRetryMultiplier
				restore.RetryMaxAttempts = 1
				restore.IgnoreRecordError = true
				return restore
			},
		},
		{
			name: "valid cold restore with filter expression",
			restore: func() *ServerRestore {
				restore := validServerRestore()
				restore.FilterExp = testServerFilterExp
				return restore
			},
		},
		{
			name: "invalid filter expression",
			restore: func() *ServerRestore {
				restore := validServerRestore()
				restore.FilterExp = testInvalidFilterExp
				return restore
			},
			wantErrMsg: errMsgInvalidFilterExp,
		},
		{
			name: "valid path",
			restore: func() *ServerRestore {
				restore := validServerRestore()
				restore.Path = testServerPath
				return restore
			},
		},
		{
			name: "path with parent segment",
			restore: func() *ServerRestore {
				restore := validServerRestore()
				restore.Path = testParentPath
				return restore
			},
			wantErrMsg: errMsgParentPath,
		},
		{
			name: "fuzzy settings passed with their defaults on a cold restore",
			restore: func() *ServerRestore {
				restore := validServerRestore()
				restore.ParallelWrite = DefaultServerRestoreParallelWrite
				restore.RetryMultiplier = DefaultServerRestoreRetryMultiplier
				return restore
			},
		},
		{
			name: "filter expression with fuzzy restore",
			restore: func() *ServerRestore {
				restore := validServerFuzzyRestore()
				restore.FilterExp = testServerFilterExp
				return restore
			},
			wantErrMsg: "filter-exp is not supported with policy fuzzy",
		},
		{
			name: "several backup ids with fuzzy restore",
			restore: func() *ServerRestore {
				restore := validServerFuzzyRestore()
				restore.JobID = testServerRestoreJobID
				restore.BackupIDs = testServerBackupIDs
				return restore
			},
			wantErrMsg: "policy fuzzy restores a single backup, got 2 backup ids",
		},
		{
			name: "valid strict restore with several backup ids and filter expression",
			restore: func() *ServerRestore {
				restore := validServerRestore()
				restore.Policy = string(infomodels.RestorePolicyStrict)
				restore.JobID = testServerRestoreJobID
				restore.BackupIDs = testServerBackupIDs
				restore.FilterExp = testServerFilterExp
				return restore
			},
		},
		{
			name: "allow unhosted with strict policy",
			restore: func() *ServerRestore {
				restore := validServerRestore()
				restore.Policy = string(infomodels.RestorePolicyStrict)
				restore.AllowUnhosted = true
				return restore
			},
			wantErrMsg: errMsgAllowUnhostedRequiresFuzzy,
		},
		{
			name: "unknown policy",
			restore: func() *ServerRestore {
				restore := validServerRestore()
				restore.Policy = testUnknownPolicy
				return restore
			},
			wantErrMsg: errMsgRestorePolicy + testUnknownPolicy + `"`,
		},
		{
			// The backup policy best-effort is not a restore policy.
			name: "backup only policy",
			restore: func() *ServerRestore {
				restore := validServerRestore()
				restore.Policy = string(infomodels.BackupPolicyBestEffort)
				return restore
			},
			wantErrMsg: errMsgRestorePolicy + string(infomodels.BackupPolicyBestEffort),
		},
		{
			name: "parallel write below range",
			restore: func() *ServerRestore {
				restore := validServerFuzzyRestore()
				restore.ParallelWrite = minServerRestoreParallelWrite - 1
				return restore
			},
			wantErrMsg: "parallel-write must be between 1 and 128, got 0",
		},
		{
			name: "parallel write above range",
			restore: func() *ServerRestore {
				restore := validServerFuzzyRestore()
				restore.ParallelWrite = maxServerRestoreParallelWrite + 1
				return restore
			},
			wantErrMsg: "parallel-write must be between 1 and 128, got 129",
		},
		{
			name: "negative records per second",
			restore: func() *ServerRestore {
				restore := validServerFuzzyRestore()
				restore.RecordsPerSecond = -1
				return restore
			},
			wantErrMsg: "records-per-second must be between 0 and 4294967295, got -1",
		},
		{
			name: "max inflight below range",
			restore: func() *ServerRestore {
				restore := validServerFuzzyRestore()
				restore.MaxInflight = 0
				return restore
			},
			wantErrMsg: "max-inflight must be between 1 and 100000, got 0",
		},
		{
			name: "max inflight above range",
			restore: func() *ServerRestore {
				restore := validServerFuzzyRestore()
				restore.MaxInflight = maxServerRestoreMaxInflight + 1
				return restore
			},
			wantErrMsg: "max-inflight must be between 1 and 100000, got 100001",
		},
		{
			// Zero retries a failed record at once.
			name: "zero retry base interval",
			restore: func() *ServerRestore {
				restore := validServerFuzzyRestore()
				restore.RetryBaseInterval = 0
				return restore
			},
		},
		{
			name: "negative retry base interval",
			restore: func() *ServerRestore {
				restore := validServerFuzzyRestore()
				restore.RetryBaseInterval = -1
				return restore
			},
			wantErrMsg: "retry-base-interval must be between 0 and 4294967295, got -1",
		},
		{
			name: "apply metadata last with fuzzy restore",
			restore: func() *ServerRestore {
				restore := validServerFuzzyRestore()
				restore.ApplyMetadataLast = true
				return restore
			},
		},
		{
			// The server refuses apply-metadata-last for a restore with no metadata.
			name: "apply metadata last with neither indexes nor udfs",
			restore: func() *ServerRestore {
				restore := validServerFuzzyRestore()
				restore.ApplyMetadataLast = true
				restore.NoIndexes = true
				restore.NoUDFs = true
				return restore
			},
			wantErrMsg: "apply-metadata-last has nothing to apply",
		},
		{
			name: "apply metadata last with indexes only",
			restore: func() *ServerRestore {
				restore := validServerFuzzyRestore()
				restore.ApplyMetadataLast = true
				restore.NoUDFs = true
				return restore
			},
		},
		{
			name: "retry multiplier below one",
			restore: func() *ServerRestore {
				restore := validServerFuzzyRestore()
				restore.RetryMultiplier = 0.5
				return restore
			},
			wantErrMsg: "retry-multiplier must be at least 1.0, got 0.5",
		},
		{
			name: "zero retry max attempts",
			restore: func() *ServerRestore {
				restore := validServerFuzzyRestore()
				restore.RetryMaxAttempts = 0
				return restore
			},
			wantErrMsg: "retry-max-attempts must be positive",
		},
		{
			name: "allow unhosted without fuzzy policy",
			restore: func() *ServerRestore {
				restore := validServerRestore()
				restore.AllowUnhosted = true
				return restore
			},
			wantErrMsg: errMsgAllowUnhostedRequiresFuzzy,
		},
		{
			// The worker threads and the rate limit apply to both kinds of restore.
			name: "parallel write and records per second on a cold restore",
			restore: func() *ServerRestore {
				restore := validServerRestore()
				restore.ParallelWrite = 16
				restore.RecordsPerSecond = 100
				return restore
			},
		},
		{
			name: "parallel write out of range on a cold restore",
			restore: func() *ServerRestore {
				restore := validServerRestore()
				restore.ParallelWrite = maxServerRestoreParallelWrite + 1
				return restore
			},
			wantErrMsg: "parallel-write must be between 1 and 128, got 129",
		},
		{
			name: "records per second above range",
			restore: func() *ServerRestore {
				restore := validServerRestore()
				restore.RecordsPerSecond = math.MaxUint32 + 1
				return restore
			},
			wantErrMsg: "records-per-second must be between 0 and 4294967295, got 4294967296",
		},
		{
			name: "extra ttl at the limit",
			restore: func() *ServerRestore {
				restore := validServerRestore()
				restore.ExtraTTL = maxServerRestoreExtraTTL
				return restore
			},
		},
		{
			name: "negative extra ttl",
			restore: func() *ServerRestore {
				restore := validServerRestore()
				restore.ExtraTTL = -1
				return restore
			},
			wantErrMsg: "extra-ttl must be between 0 and 315360000, got -1",
		},
		{
			name: "extra ttl above range",
			restore: func() *ServerRestore {
				restore := validServerRestore()
				restore.ExtraTTL = maxServerRestoreExtraTTL + 1
				return restore
			},
			wantErrMsg: "extra-ttl must be between 0 and 315360000, got 315360001",
		},
		{
			name: "incremental path with several backups",
			restore: func() *ServerRestore {
				restore := validServerRestore()
				restore.BackupIDs = testServerBackupIDs
				restore.JobID = testServerRestoreJobID
				restore.IncrementalPath = testServerPath
				return restore
			},
		},
		{
			// The first backup is always read from under the path, so an incremental path
			// means nothing to a restore of a single backup.
			name: "incremental path with a single backup",
			restore: func() *ServerRestore {
				restore := validServerRestore()
				restore.IncrementalPath = testServerPath
				return restore
			},
			wantErrMsg: "incremental-path requires more than one backup id",
		},
		{
			name: "incremental path with parent segment",
			restore: func() *ServerRestore {
				restore := validServerRestore()
				restore.BackupIDs = testServerBackupIDs
				restore.JobID = testServerRestoreJobID
				restore.IncrementalPath = testParentPath
				return restore
			},
			wantErrMsg: errMsgParentPath,
		},
		{
			name: "apply metadata last without fuzzy policy",
			restore: func() *ServerRestore {
				restore := validServerRestore()
				restore.ApplyMetadataLast = true
				return restore
			},
			wantErrMsg: "apply-metadata-last " + errMsgRequiresFuzzy,
		},
		{
			name: "max inflight without fuzzy policy",
			restore: func() *ServerRestore {
				restore := validServerRestore()
				restore.MaxInflight = 100
				return restore
			},
			wantErrMsg: "max-inflight " + errMsgRequiresFuzzy,
		},
		{
			name: "retry base interval without fuzzy policy",
			restore: func() *ServerRestore {
				restore := validServerRestore()
				restore.RetryBaseInterval = 100
				return restore
			},
			wantErrMsg: "retry-base-interval " + errMsgRequiresFuzzy,
		},
		{
			name: "retry multiplier without fuzzy policy",
			restore: func() *ServerRestore {
				restore := validServerRestore()
				restore.RetryMultiplier = 2
				return restore
			},
			wantErrMsg: "retry-multiplier " + errMsgRequiresFuzzy,
		},
		{
			name: "retry max attempts without fuzzy policy",
			restore: func() *ServerRestore {
				restore := validServerRestore()
				restore.RetryMaxAttempts = 1
				return restore
			},
			wantErrMsg: "retry-max-attempts " + errMsgRequiresFuzzy,
		},
		{
			name: "ignore record error without fuzzy policy",
			restore: func() *ServerRestore {
				restore := validServerRestore()
				restore.IgnoreRecordError = true
				return restore
			},
			wantErrMsg: "ignore-record-error " + errMsgRequiresFuzzy,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.restore().Validate()

			if tt.wantErrMsg != "" {
				require.ErrorContains(t, err, tt.wantErrMsg)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestServerRestore_Validate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		restore    func() *ServerRestore
		wantErr    bool
		wantErrMsg string
	}{
		{
			name:    "valid restore",
			restore: validServerRestore,
			wantErr: false,
		},
		{
			name: "nil restore",
			restore: func() *ServerRestore {
				return nil
			},
			wantErr: false,
		},
		{
			name: "missing namespace",
			restore: func() *ServerRestore {
				restore := validServerRestore()
				restore.Namespace = ""
				return restore
			},
			wantErr:    true,
			wantErrMsg: "namespace is required",
		},
		{
			name: "missing backup ids",
			restore: func() *ServerRestore {
				restore := validServerRestore()
				restore.BackupIDs = ""
				return restore
			},
			wantErr:    true,
			wantErrMsg: "backup-ids is required",
		},
		{
			name: "empty id inside the backup id list",
			restore: func() *ServerRestore {
				restore := validServerRestore()
				restore.JobID = testServerRestoreJobID
				restore.BackupIDs = testServerJobID + ",," + testServerSecondJobID
				return restore
			},
			wantErr:    true,
			wantErrMsg: "backup-ids must not contain an empty id",
		},
		{
			name: "backup id of another form in the backup id list",
			restore: func() *ServerRestore {
				restore := validServerRestore()
				restore.JobID = testServerRestoreJobID
				restore.BackupIDs = testServerJobID + "," + testNotBackupID
				return restore
			},
			wantErr:    true,
			wantErrMsg: errMsgNotBackupID,
		},
		{
			name: "trailing separator in the backup id list",
			restore: func() *ServerRestore {
				restore := validServerRestore()
				restore.JobID = testServerRestoreJobID
				restore.BackupIDs = testServerBackupIDs + ","
				return restore
			},
			wantErr:    true,
			wantErrMsg: "backup-ids must not contain an empty id",
		},
		{
			// The job id cannot fall back to a list of backup ids.
			name: "several backup ids without a job id",
			restore: func() *ServerRestore {
				restore := validServerRestore()
				restore.BackupIDs = testServerBackupIDs
				return restore
			},
			wantErr:    true,
			wantErrMsg: "job-id is required when more than one backup id is given",
		},
		{
			name: "several backup ids with a job id",
			restore: func() *ServerRestore {
				restore := validServerRestore()
				restore.JobID = testServerRestoreJobID
				restore.BackupIDs = testServerBackupIDs
				return restore
			},
			wantErr: false,
		},
		{
			name: "explicit job id",
			restore: func() *ServerRestore {
				restore := validServerRestore()
				restore.JobID = testServerRestoreJobID
				return restore
			},
			wantErr: false,
		},
		{
			name: "parallel read at the lower limit on a cold restore",
			restore: func() *ServerRestore {
				restore := validServerRestore()
				restore.ParallelRead = minServerRestoreParallelRead
				return restore
			},
			wantErr: false,
		},
		{
			name: "parallel read at the upper limit on a fuzzy restore",
			restore: func() *ServerRestore {
				restore := validServerFuzzyRestore()
				restore.ParallelRead = maxServerRestoreParallelRead
				return restore
			},
			wantErr: false,
		},
		{
			name: "parallel read below range",
			restore: func() *ServerRestore {
				restore := validServerRestore()
				restore.ParallelRead = minServerRestoreParallelRead - 1
				return restore
			},
			wantErr:    true,
			wantErrMsg: errMsgParallelRead + "0",
		},
		{
			name: "negative parallel read",
			restore: func() *ServerRestore {
				restore := validServerRestore()
				restore.ParallelRead = -1
				return restore
			},
			wantErr:    true,
			wantErrMsg: errMsgParallelRead + "-1",
		},
		{
			name: "parallel read above range on a fuzzy restore",
			restore: func() *ServerRestore {
				restore := validServerFuzzyRestore()
				restore.ParallelRead = maxServerRestoreParallelRead + 1
				return restore
			},
			wantErr:    true,
			wantErrMsg: errMsgParallelRead + "129",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.restore().Validate()

			if tt.wantErr {
				require.Error(t, err)
				if tt.wantErrMsg != "" {
					assert.Contains(t, err.Error(), tt.wantErrMsg)
				}
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestServerRestoreAbort_Validate(t *testing.T) {
	t.Parallel()

	const errMsgNamespaceRequired = "namespace is required"

	tests := []struct {
		name       string
		abort      func() *ServerRestoreAbort
		wantErr    bool
		wantErrMsg string
	}{
		{
			name:    "valid abort",
			abort:   validServerRestoreAbort,
			wantErr: false,
		},
		{
			name: "nil abort",
			abort: func() *ServerRestoreAbort {
				return nil
			},
			wantErr: false,
		},
		{
			name: "missing job id",
			abort: func() *ServerRestoreAbort {
				abort := validServerRestoreAbort()
				abort.JobID = ""
				return abort
			},
			wantErr:    true,
			wantErrMsg: errMsgJobIDRequired,
		},
		{
			name: "missing namespace",
			abort: func() *ServerRestoreAbort {
				abort := validServerRestoreAbort()
				abort.Namespace = ""
				return abort
			},
			wantErr:    true,
			wantErrMsg: errMsgNamespaceRequired,
		},
		{
			// Both fields name the job, and the missing id is reported first.
			name: "missing namespace and job id",
			abort: func() *ServerRestoreAbort {
				return &ServerRestoreAbort{}
			},
			wantErr:    true,
			wantErrMsg: errMsgJobIDRequired,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.abort().Validate()

			if tt.wantErr {
				require.Error(t, err)
				if tt.wantErrMsg != "" {
					assert.Contains(t, err.Error(), tt.wantErrMsg)
				}
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestServerRestorePrepare_Validate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		prepare    func() *ServerRestorePrepare
		wantErr    bool
		wantErrMsg string
	}{
		{
			name:    "valid prepare",
			prepare: validServerRestorePrepare,
			wantErr: false,
		},
		{
			name: "nil prepare",
			prepare: func() *ServerRestorePrepare {
				return nil
			},
			wantErr: false,
		},
		{
			name: "missing namespace",
			prepare: func() *ServerRestorePrepare {
				prepare := validServerRestorePrepare()
				prepare.Namespace = ""
				return prepare
			},
			wantErr:    true,
			wantErrMsg: "namespace is required",
		},
		{
			name: "missing job id",
			prepare: func() *ServerRestorePrepare {
				prepare := validServerRestorePrepare()
				prepare.JobID = ""
				return prepare
			},
			wantErr:    true,
			wantErrMsg: errMsgJobIDRequired,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.prepare().Validate()

			if tt.wantErr {
				require.Error(t, err)
				if tt.wantErrMsg != "" {
					assert.Contains(t, err.Error(), tt.wantErrMsg)
				}
				return
			}
			require.NoError(t, err)
		})
	}
}

// TestServerRestoreJob_Validate covers the commands that name a running restore job,
// which all check the job the same way.
func TestServerRestoreJob_Validate(t *testing.T) {
	t.Parallel()

	const errMsgNamespaceRequired = "namespace is required"

	type validator interface{ Validate() error }

	commands := []struct {
		name string
		make func(namespace, jobID string) validator
		zero validator
	}{
		{
			name: "progress",
			make: func(namespace, jobID string) validator {
				return &ServerRestoreProgress{Namespace: namespace, JobID: jobID, Watch: true}
			},
			zero: (*ServerRestoreProgress)(nil),
		},
		{
			name: "open",
			make: func(namespace, jobID string) validator {
				return &ServerRestoreOpen{Namespace: namespace, JobID: jobID, Async: true}
			},
			zero: (*ServerRestoreOpen)(nil),
		},
		{
			name: "clear storage",
			make: func(namespace, jobID string) validator {
				return &ServerRestoreClearStorage{Namespace: namespace, JobID: jobID, Async: true}
			},
			zero: (*ServerRestoreClearStorage)(nil),
		},
	}

	tests := []struct {
		name       string
		namespace  string
		jobID      string
		wantErrMsg string
	}{
		{name: "valid", namespace: testServerNamespace, jobID: testServerRestoreJobID},
		{name: "missing job id", namespace: testServerNamespace, wantErrMsg: errMsgJobIDRequired},
		{name: "missing namespace", jobID: testServerRestoreJobID, wantErrMsg: errMsgNamespaceRequired},
		// Both fields name the job, and the missing id is reported first.
		{name: "missing namespace and job id", wantErrMsg: errMsgJobIDRequired},
	}

	for _, cmd := range commands {
		t.Run(cmd.name+"/nil", func(t *testing.T) {
			t.Parallel()

			require.NoError(t, cmd.zero.Validate())
		})

		for _, tt := range tests {
			t.Run(cmd.name+"/"+tt.name, func(t *testing.T) {
				t.Parallel()

				err := cmd.make(tt.namespace, tt.jobID).Validate()

				if tt.wantErrMsg != "" {
					require.ErrorContains(t, err, tt.wantErrMsg)
					return
				}
				require.NoError(t, err)
			})
		}
	}
}
