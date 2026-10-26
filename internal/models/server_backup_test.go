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
	testServerNamespace = "test-ns"
	testServerJobID     = "261025T120000-ab12"
	// testNotBackupID is an id that does not have the form of a backup id.
	testNotBackupID        = "backup-job-1"
	errMsgNotBackupID      = "is not a backup id"
	testServerListPath     = "/backups"
	testServerFilterExp    = "kwGTUQKkYmluMQE="
	testInvalidFilterExp   = "not-base64!"
	errMsgInvalidFilterExp = "invalid filter-exp: must be a base64-encoded filter expression"
	testServerPath         = "backups/daily"
	testParentPath         = "backups/../other"
	errMsgParentPath       = `invalid path "backups/../other": must not contain '..'`
	testUnknownPolicy      = "eventual"
)

func validServerBackup() *ServerBackup {
	return &ServerBackup{
		Namespace: testServerNamespace,
	}
}

func TestServerBackup_Validate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		backup     func() *ServerBackup
		wantErr    bool
		wantErrMsg string
	}{
		{
			name:    "valid backup",
			backup:  validServerBackup,
			wantErr: false,
		},
		{
			name: "nil backup",
			backup: func() *ServerBackup {
				return nil
			},
			wantErr: false,
		},
		{
			name: "missing namespace",
			backup: func() *ServerBackup {
				backup := validServerBackup()
				backup.Namespace = ""
				return backup
			},
			wantErr:    true,
			wantErrMsg: "namespace is required",
		},
		{
			name: "invalid modified after",
			backup: func() *ServerBackup {
				backup := validServerBackup()
				backup.ModifiedAfter = "not-a-date"
				return backup
			},
			wantErr:    true,
			wantErrMsg: "failed to parse modified after",
		},
		{
			name: "invalid modified before",
			backup: func() *ServerBackup {
				backup := validServerBackup()
				backup.ModifiedBefore = "not-a-date"
				return backup
			},
			wantErr:    true,
			wantErrMsg: "failed to parse modified before",
		},
		{
			name: "modified before not after modified after",
			backup: func() *ServerBackup {
				backup := validServerBackup()
				backup.ModifiedAfter = "2024-12-31"
				backup.ModifiedBefore = "2024-01-01"
				return backup
			},
			wantErr:    true,
			wantErrMsg: "modified-before must be strictly greater than modified-after",
		},
		{
			name: "valid modified after and before",
			backup: func() *ServerBackup {
				backup := validServerBackup()
				backup.ModifiedAfter = "2024-01-01"
				backup.ModifiedBefore = "2024-12-31"
				return backup
			},
			wantErr: false,
		},
		{
			name: "valid filter expression",
			backup: func() *ServerBackup {
				backup := validServerBackup()
				backup.FilterExp = testServerFilterExp
				return backup
			},
			wantErr: false,
		},
		{
			name: "invalid filter expression",
			backup: func() *ServerBackup {
				backup := validServerBackup()
				backup.FilterExp = testInvalidFilterExp
				return backup
			},
			wantErr:    true,
			wantErrMsg: errMsgInvalidFilterExp,
		},
		{
			name: "valid path",
			backup: func() *ServerBackup {
				backup := validServerBackup()
				backup.Path = testServerPath
				return backup
			},
			wantErr: false,
		},
		{
			name: "path with parent segment",
			backup: func() *ServerBackup {
				backup := validServerBackup()
				backup.Path = testParentPath
				return backup
			},
			wantErr:    true,
			wantErrMsg: errMsgParentPath,
		},
		{
			name: "job settings at the limits",
			backup: func() *ServerBackup {
				backup := validServerBackup()
				backup.RecordsPerSecond = math.MaxUint32
				backup.ParallelRead = maxServerBackupParallel
				backup.ParallelWrite = maxServerBackupParallel
				backup.S3Throughput = maxServerBackupS3Throughput
				return backup
			},
		},
		{
			// The server checks the rate against its own number of scan threads.
			name: "records per second without parallel read",
			backup: func() *ServerBackup {
				backup := validServerBackup()
				backup.RecordsPerSecond = 1
				return backup
			},
		},
		{
			name: "records per second at the minimum for parallel read",
			backup: func() *ServerBackup {
				backup := validServerBackup()
				backup.ParallelRead = 4
				backup.RecordsPerSecond = 4 * minServerBackupRPSPerReader
				return backup
			},
		},
		{
			name: "records per second below the minimum for parallel read",
			backup: func() *ServerBackup {
				backup := validServerBackup()
				backup.ParallelRead = 4
				backup.RecordsPerSecond = 4*minServerBackupRPSPerReader - 1
				return backup
			},
			wantErr:    true,
			wantErrMsg: "records-per-second must be 0 or at least 20 for 4 parallel-read, got 19",
		},
		{
			name: "negative records per second",
			backup: func() *ServerBackup {
				backup := validServerBackup()
				backup.RecordsPerSecond = -1
				return backup
			},
			wantErr:    true,
			wantErrMsg: "records-per-second must be between 0 and 4294967295, got -1",
		},
		{
			name: "records per second above range",
			backup: func() *ServerBackup {
				backup := validServerBackup()
				backup.RecordsPerSecond = math.MaxUint32 + 1
				return backup
			},
			wantErr:    true,
			wantErrMsg: "records-per-second must be between 0 and 4294967295, got 4294967296",
		},
		{
			name: "parallel read above range",
			backup: func() *ServerBackup {
				backup := validServerBackup()
				backup.ParallelRead = maxServerBackupParallel + 1
				return backup
			},
			wantErr:    true,
			wantErrMsg: "parallel-read must be between 0 and 128, got 129",
		},
		{
			name: "negative parallel read",
			backup: func() *ServerBackup {
				backup := validServerBackup()
				backup.ParallelRead = -1
				return backup
			},
			wantErr:    true,
			wantErrMsg: "parallel-read must be between 0 and 128, got -1",
		},
		{
			name: "parallel write above range",
			backup: func() *ServerBackup {
				backup := validServerBackup()
				backup.ParallelWrite = maxServerBackupParallel + 1
				return backup
			},
			wantErr:    true,
			wantErrMsg: "parallel-write must be between 0 and 128, got 129",
		},
		{
			name: "s3 throughput above range",
			backup: func() *ServerBackup {
				backup := validServerBackup()
				backup.S3Throughput = maxServerBackupS3Throughput + 1
				return backup
			},
			wantErr:    true,
			wantErrMsg: "s3-throughput must be between 0 and 400, got 401",
		},
		{
			name: "negative s3 throughput",
			backup: func() *ServerBackup {
				backup := validServerBackup()
				backup.S3Throughput = -1
				return backup
			},
			wantErr:    true,
			wantErrMsg: "s3-throughput must be between 0 and 400, got -1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.backup().Validate()

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

func TestServerBackup_ValidatePolicy(t *testing.T) {
	t.Parallel()

	const (
		testUpperCasePolicy = "STRICT"
		// errMsgBackupPolicy is the common part of the unknown backup policy errors.
		errMsgBackupPolicy = `policy must be strict, best-effort or fuzzy, got "`
	)

	tests := []struct {
		name       string
		givePolicy string
		wantErrMsg string
	}{
		{name: "empty uses the server default"},
		{name: "strict", givePolicy: string(infomodels.BackupPolicyStrict)},
		{name: "best effort", givePolicy: string(infomodels.BackupPolicyBestEffort)},
		{name: "fuzzy", givePolicy: string(infomodels.BackupPolicyFuzzy)},
		{
			name:       "unknown",
			givePolicy: testUnknownPolicy,
			wantErrMsg: errMsgBackupPolicy + testUnknownPolicy,
		},
		{
			// Policies are matched exactly, as the server does.
			name:       "upper case",
			givePolicy: testUpperCasePolicy,
			wantErrMsg: errMsgBackupPolicy + testUpperCasePolicy,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backup := validServerBackup()
			backup.Policy = tt.givePolicy

			err := backup.Validate()

			if tt.wantErrMsg != "" {
				require.ErrorContains(t, err, tt.wantErrMsg)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestServerBackupList_Validate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		list       *ServerBackupList
		wantErr    bool
		wantErrMsg string
	}{
		{
			name: "valid list path",
			list: &ServerBackupList{
				Path: testServerListPath,
			},
			wantErr: false,
		},
		{
			name:    "nil list",
			list:    nil,
			wantErr: false,
		},
		{
			name: "path with parent segment",
			list: &ServerBackupList{
				Path: testParentPath,
			},
			wantErr:    true,
			wantErrMsg: errMsgParentPath,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.list.Validate()

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

func TestServerBackupProgress_Validate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		progress   *ServerBackupProgress
		wantErr    bool
		wantErrMsg string
	}{
		{
			name: "valid progress",
			progress: &ServerBackupProgress{
				JobID: testServerJobID,
			},
			wantErr: false,
		},
		{
			name:     "nil progress",
			progress: nil,
			wantErr:  false,
		},
		{
			name:       "missing backup id",
			progress:   &ServerBackupProgress{},
			wantErr:    true,
			wantErrMsg: "backup-id is required",
		},
		{
			name:       "backup id of another form",
			progress:   &ServerBackupProgress{JobID: testNotBackupID},
			wantErr:    true,
			wantErrMsg: errMsgNotBackupID,
		},
		{
			name: "watch enabled",
			progress: &ServerBackupProgress{
				JobID: testServerJobID,
				Watch: true,
			},
			wantErr: false,
		},
		{
			name: "valid path",
			progress: &ServerBackupProgress{
				JobID: testServerJobID,
				Path:  testServerPath,
			},
			wantErr: false,
		},
		{
			name: "path with parent segment",
			progress: &ServerBackupProgress{
				JobID: testServerJobID,
				Path:  testParentPath,
			},
			wantErr:    true,
			wantErrMsg: errMsgParentPath,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.progress.Validate()

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

func TestServerBackupAbort_Validate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		abort      *ServerBackupAbort
		wantErr    bool
		wantErrMsg string
	}{
		{
			name: "valid backup id",
			abort: &ServerBackupAbort{
				JobID: testServerJobID,
			},
			wantErr: false,
		},
		{
			name:    "nil abort",
			abort:   nil,
			wantErr: false,
		},
		{
			name:       "missing backup id",
			abort:      &ServerBackupAbort{},
			wantErr:    true,
			wantErrMsg: "backup-id is required",
		},
		{
			name:       "backup id of another form",
			abort:      &ServerBackupAbort{JobID: testNotBackupID},
			wantErr:    true,
			wantErrMsg: errMsgNotBackupID,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.abort.Validate()

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

func TestServerBackupValidate_Validate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		validation *ServerBackupValidate
		wantErr    bool
		wantErrMsg string
	}{
		{
			name: "valid backup id",
			validation: &ServerBackupValidate{
				JobID: testServerJobID,
			},
			wantErr: false,
		},
		{
			name:       "nil validation",
			validation: nil,
			wantErr:    false,
		},
		{
			name:       "missing backup id",
			validation: &ServerBackupValidate{},
			wantErr:    true,
			wantErrMsg: "backup-id is required",
		},
		{
			name:       "backup id of another form",
			validation: &ServerBackupValidate{JobID: testNotBackupID},
			wantErr:    true,
			wantErrMsg: errMsgNotBackupID,
		},
		{
			name: "zero sample size validates all segments",
			validation: &ServerBackupValidate{
				JobID:      testServerJobID,
				SampleSize: 0,
			},
			wantErr: false,
		},
		{
			name: "negative sample size",
			validation: &ServerBackupValidate{
				JobID:      testServerJobID,
				SampleSize: -1,
			},
			wantErr:    true,
			wantErrMsg: "sample-size must be non-negative",
		},
		{
			name: "valid path",
			validation: &ServerBackupValidate{
				JobID: testServerJobID,
				Path:  testServerPath,
			},
			wantErr: false,
		},
		{
			name: "path with parent segment",
			validation: &ServerBackupValidate{
				JobID: testServerJobID,
				Path:  testParentPath,
			},
			wantErr:    true,
			wantErrMsg: errMsgParentPath,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.validation.Validate()

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
