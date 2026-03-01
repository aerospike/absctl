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
	"time"

	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		restore *Restore
		wantErr bool
		errMsg  string
	}{
		{
			name: "Valid restore configuration with input file",
			restore: &Restore{
				InputFile: "backup.asb",
				Mode:      RestoreModeASB,
				Common: Common{
					Namespace: testNamespace,
				},
			},
			wantErr: false,
		},
		{
			name: "Valid restore configuration with directory",
			restore: &Restore{
				Mode: RestoreModeASB,
				Common: Common{
					Directory: "restore-dir",
					Namespace: "test",
				},
			},
			wantErr: false,
		},
		{
			name: "Valid restore configuration with directory list",
			restore: &Restore{
				DirectoryList:   "dir1,dir2",
				ParentDirectory: "parent",
				Mode:            RestoreModeASB,
				Common: Common{
					Namespace: "test",
				},
			},
			wantErr: false,
		},
		{
			name: "Invalid restore mode",
			restore: &Restore{
				InputFile: "backup.asb",
				Mode:      "invalid-mode",
				Common: Common{
					Namespace: "test",
				},
			},
			wantErr: true,
			errMsg:  "invalid restore mode: invalid-mode",
		},
		{
			name: "Missing input source",
			restore: &Restore{
				Mode: RestoreModeASB,
				Common: Common{
					Namespace: "test",
				},
			},
			wantErr: true,
			errMsg:  "input file or directory required",
		},
		{
			name: "Invalid restore restore - both input file and directory",
			restore: &Restore{
				InputFile: "backup.asb",
				Mode:      RestoreModeASB,
				Common: Common{
					Directory: "restore-dir",
					Namespace: "test",
				},
			},
			wantErr: true,
			errMsg:  "only one of directory and input-file may be configured at the same time",
		},
		{
			name: "Invalid common restore - missing namespace",
			restore: &Restore{
				InputFile: "backup.asb",
				Mode:      RestoreModeASB,
			},
			wantErr: true,
			errMsg:  "namespace is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.restore.Validate()
			if tt.wantErr {
				require.Error(t, err)

				if tt.errMsg != "" {
					assert.Equal(t, tt.errMsg, err.Error())
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestMapRestoreNamespace_SuccessSingleNamespace(t *testing.T) {
	t.Parallel()

	restore := &Restore{
		Common: Common{
			Namespace: "source-ns",
		},
	}

	result := restore.NamespaceConfig()
	require.NotNil(t, result, "Result should not be nil")
	assert.Equal(t, "source-ns", *result.Source, "Source should be 'source-ns'")
	assert.Equal(t, "source-ns", *result.Destination, "Destination should be the same as Source")
}

func TestMapRestoreNamespace_SuccessDifferentNamespaces(t *testing.T) {
	t.Parallel()

	restore := &Restore{
		Common: Common{
			Namespace: "source-ns,destination-ns",
		},
	}

	result := restore.NamespaceConfig()
	require.NotNil(t, result, "Result should not be nil")
	assert.Equal(t, "source-ns", *result.Source, "Source should be 'source-ns'")
	assert.Equal(t, "destination-ns", *result.Destination, "Destination should be 'destination-ns'")
}

func TestMapRestoreNamespace_InvalidNamespace(t *testing.T) {
	t.Parallel()

	restore := &Restore{
		Common: Common{
			Namespace: "source-ns,destination-ns,extra-ns",
		},
	}

	result := restore.NamespaceConfig()
	assert.Nil(t, result, "Result should be nil for invalid input")
}

func TestMapWritePolicy_Success(t *testing.T) {
	t.Parallel()

	restoreModel := &Restore{
		Replace: true,
		Uniq:    false,
		Common: Common{
			TotalTimeout:  5000,
			SocketTimeout: 1500,
		},
	}

	writePolicy := restoreModel.WritePolicy()
	assert.Equal(t, aerospike.REPLACE, writePolicy.RecordExistsAction)
	assert.Equal(t, 5000*time.Millisecond, writePolicy.TotalTimeout)
	assert.Equal(t, 1500*time.Millisecond, writePolicy.SocketTimeout)
}

func TestMapWritePolicy_ConfigurationCombinations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		restoreModel  *Restore
		commonModel   *Common
		wantAction    aerospike.RecordExistsAction
		wantGenPolicy aerospike.GenerationPolicy
	}{
		{
			name: "replace with generation",
			restoreModel: &Restore{
				Replace:      true,
				NoGeneration: false,
			},
			commonModel:   &Common{},
			wantAction:    aerospike.REPLACE,
			wantGenPolicy: aerospike.EXPECT_GEN_GT,
		},
		{
			name:          "default update with generation",
			restoreModel:  &Restore{},
			commonModel:   &Common{},
			wantAction:    aerospike.UPDATE,
			wantGenPolicy: aerospike.EXPECT_GEN_GT,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := tt.restoreModel.WritePolicy()
			assert.Equal(t, tt.wantAction, got.RecordExistsAction)
			assert.Equal(t, tt.wantGenPolicy, got.GenerationPolicy)
			assert.True(t, got.SendKey)
		})
	}
}
