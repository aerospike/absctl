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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testServerRestoreJobID = "restore-job-1"
	errMsgJobIDRequired    = "job-id is required"
	errMsgRequiresFuzzy    = "requires fuzzy-restore"
)

// validServerRestore returns a cold restore with every setting at its default, as the
// flags and the YAML config produce it.
func validServerRestore() *ServerRestore {
	return &ServerRestore{
		Namespace:         testServerNamespace,
		StorageType:       testServerStorage,
		BackupID:          testServerJobID,
		AllowUnhosted:     DefaultServerRestoreAllowUnhosted,
		Parallel:          DefaultServerRestoreParallel,
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
	restore.FuzzyRestore = true

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
		name         string
		giveJobID    string
		giveBackupID string
		want         string
	}{
		{
			name:         "job id set",
			giveJobID:    testServerRestoreJobID,
			giveBackupID: testServerJobID,
			want:         testServerRestoreJobID,
		},
		{
			name:         "job id defaults to backup id",
			giveBackupID: testServerJobID,
			want:         testServerJobID,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			restore := &ServerRestore{JobID: tt.giveJobID, BackupID: tt.giveBackupID}

			assert.Equal(t, tt.want, restore.RestoreJobID())
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
				restore.Parallel = maxServerRestoreParallel
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
				restore.Parallel = DefaultServerRestoreParallel
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
			wantErrMsg: "filter-exp is not supported with fuzzy-restore",
		},
		{
			name: "parallel below range",
			restore: func() *ServerRestore {
				restore := validServerFuzzyRestore()
				restore.Parallel = minServerRestoreParallel - 1
				return restore
			},
			wantErrMsg: "parallel must be between 1 and 128, got 0",
		},
		{
			name: "parallel above range",
			restore: func() *ServerRestore {
				restore := validServerFuzzyRestore()
				restore.Parallel = maxServerRestoreParallel + 1
				return restore
			},
			wantErrMsg: "parallel must be between 1 and 128, got 129",
		},
		{
			name: "negative records per second",
			restore: func() *ServerRestore {
				restore := validServerFuzzyRestore()
				restore.RecordsPerSecond = -1
				return restore
			},
			wantErrMsg: "records-per-second must be non-negative",
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
			name: "zero retry base interval",
			restore: func() *ServerRestore {
				restore := validServerFuzzyRestore()
				restore.RetryBaseInterval = 0
				return restore
			},
			wantErrMsg: "retry-base-interval must be positive",
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
			name: "allow unhosted without fuzzy restore",
			restore: func() *ServerRestore {
				restore := validServerRestore()
				restore.AllowUnhosted = true
				return restore
			},
			wantErrMsg: "allow-unhosted " + errMsgRequiresFuzzy,
		},
		{
			name: "parallel without fuzzy restore",
			restore: func() *ServerRestore {
				restore := validServerRestore()
				restore.Parallel = 16
				return restore
			},
			wantErrMsg: "parallel " + errMsgRequiresFuzzy,
		},
		{
			name: "records per second without fuzzy restore",
			restore: func() *ServerRestore {
				restore := validServerRestore()
				restore.RecordsPerSecond = 100
				return restore
			},
			wantErrMsg: "records-per-second " + errMsgRequiresFuzzy,
		},
		{
			name: "max inflight without fuzzy restore",
			restore: func() *ServerRestore {
				restore := validServerRestore()
				restore.MaxInflight = 100
				return restore
			},
			wantErrMsg: "max-inflight " + errMsgRequiresFuzzy,
		},
		{
			name: "retry base interval without fuzzy restore",
			restore: func() *ServerRestore {
				restore := validServerRestore()
				restore.RetryBaseInterval = 100
				return restore
			},
			wantErrMsg: "retry-base-interval " + errMsgRequiresFuzzy,
		},
		{
			name: "retry multiplier without fuzzy restore",
			restore: func() *ServerRestore {
				restore := validServerRestore()
				restore.RetryMultiplier = 2
				return restore
			},
			wantErrMsg: "retry-multiplier " + errMsgRequiresFuzzy,
		},
		{
			name: "retry max attempts without fuzzy restore",
			restore: func() *ServerRestore {
				restore := validServerRestore()
				restore.RetryMaxAttempts = 1
				return restore
			},
			wantErrMsg: "retry-max-attempts " + errMsgRequiresFuzzy,
		},
		{
			name: "ignore record error without fuzzy restore",
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
			name: "missing storage type",
			restore: func() *ServerRestore {
				restore := validServerRestore()
				restore.StorageType = ""
				return restore
			},
			wantErr:    true,
			wantErrMsg: "storage-type is required",
		},
		{
			name: "unsupported storage type",
			restore: func() *ServerRestore {
				restore := validServerRestore()
				restore.StorageType = testUnsupportedStorage
				return restore
			},
			wantErr:    true,
			wantErrMsg: "unsupported storage-type",
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
			name: "missing backup id",
			restore: func() *ServerRestore {
				restore := validServerRestore()
				restore.BackupID = ""
				return restore
			},
			wantErr:    true,
			wantErrMsg: "backup-id is required",
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
