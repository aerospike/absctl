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

func validServerRestore() *ServerRestore {
	return &ServerRestore{
		ServerCommon: ServerCommon{
			Namespace:   testServerNamespace,
			StorageType: testServerStorage,
		},
		JobID: testServerJobID,
	}
}

func validServerRestorePrepare() *ServerRestorePrepare {
	return &ServerRestorePrepare{
		ServerCommon: ServerCommon{
			Namespace: testServerNamespace,
		},
		JobID: testServerJobID,
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
				restore.JobID = ""
				return restore
			},
			wantErr:    true,
			wantErrMsg: "backup-id is required",
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
			name: "missing backup id",
			prepare: func() *ServerRestorePrepare {
				prepare := validServerRestorePrepare()
				prepare.JobID = ""
				return prepare
			},
			wantErr:    true,
			wantErrMsg: "backup-id is required",
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
