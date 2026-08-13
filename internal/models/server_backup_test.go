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

package models

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testServerNamespace = "test-ns"
	testServerJobID     = "backup-job-1"
	testServerListPath  = "/backups"
	testServerStorage   = "s3"
)

func validServerBackup() *ServerBackup {
	return &ServerBackup{
		ServerCommon: ServerCommon{
			Namespace:   testServerNamespace,
			StorageType: testServerStorage,
		},
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
			name: "missing storage type",
			backup: func() *ServerBackup {
				backup := validServerBackup()
				backup.StorageType = ""
				return backup
			},
			wantErr:    true,
			wantErrMsg: "storage-type is required",
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
				ListPath: testServerListPath,
			},
			wantErr: false,
		},
		{
			name:    "nil list",
			list:    nil,
			wantErr: false,
		},
		{
			name:       "missing list path",
			list:       &ServerBackupList{},
			wantErr:    true,
			wantErrMsg: "list-path is required",
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
