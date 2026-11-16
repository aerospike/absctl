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
	"time"

	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/backup-go/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateRestore(t *testing.T) {
	t.Parallel()

	const testRestoreDir = "restore-dir"

	tests := []struct {
		name    string
		restore *Restore
		wantErr bool
		errMsg  string
	}{
		{
			name: "Valid restore configuration with input file",
			restore: &Restore{
				InputFile:        "backup.asb",
				Namespace:        testNamespace,
				RetryMultiplier:  DefaultRestoreRetryMultiplier,
				RetryMaxAttempts: DefaultRestoreRetryMaxAttempts,
			},
			wantErr: false,
		},
		{
			name: "Valid restore configuration with directory",
			restore: &Restore{
				Directory:        "restore-dir",
				Namespace:        "test",
				RetryMultiplier:  DefaultRestoreRetryMultiplier,
				RetryMaxAttempts: DefaultRestoreRetryMaxAttempts,
			},
			wantErr: false,
		},
		{
			name: "Valid restore configuration with directory list",
			restore: &Restore{
				DirectoryList:    "dir1,dir2",
				ParentDirectory:  "parent",
				Namespace:        "test",
				RetryMultiplier:  DefaultRestoreRetryMultiplier,
				RetryMaxAttempts: DefaultRestoreRetryMaxAttempts,
			},
			wantErr: false,
		},
		{
			name: "Missing input source",
			restore: &Restore{
				Namespace: "test",
			},
			wantErr: true,
			errMsg:  "input file or directory required",
		},
		{
			name: "Invalid restore restore - both input file and directory",
			restore: &Restore{
				InputFile: "backup.asb",
				Directory: "restore-dir",
				Namespace: "test",
			},
			wantErr: true,
			errMsg:  "only one of directory and input-file may be configured at the same time",
		},
		{
			name: "Invalid restore - parent directory without directory list",
			restore: &Restore{
				ParentDirectory: "parent",
				Directory:       "restore-dir",
				Namespace:       "test",
			},
			wantErr: true,
			errMsg:  "parent-directory requires directory-list to be set",
		},
		{
			name: "Invalid common restore - missing namespace",
			restore: &Restore{
				InputFile: "backup.asb",
			},
			wantErr: true,
			errMsg:  "namespace is required",
		},
		{
			name: "Replace and uniq are mutually exclusive",
			restore: &Restore{
				Directory: "restore-dir",
				Namespace: "test",
				Replace:   true,
				Uniq:      true,
			},
			wantErr: true,
			errMsg:  "replace and unique are mutually exclusive",
		},
		{
			name: "Negative retry base interval",
			restore: &Restore{
				Directory:         testRestoreDir,
				Namespace:         testNamespace,
				RetryBaseInterval: -1,
				RetryMultiplier:   DefaultRestoreRetryMultiplier,
				RetryMaxAttempts:  DefaultRestoreRetryMaxAttempts,
			},
			wantErr: true,
			errMsg:  "retry-base-interval must be non-negative",
		},
		{
			name: "Retry multiplier less than one",
			restore: &Restore{
				Directory:        testRestoreDir,
				Namespace:        testNamespace,
				RetryMultiplier:  0.5,
				RetryMaxAttempts: DefaultRestoreRetryMaxAttempts,
			},
			wantErr: true,
			errMsg:  "retry-multiplier must be greater than or equal to 1",
		},
		{
			name: "Retry max attempts less than one",
			restore: &Restore{
				Directory:       testRestoreDir,
				Namespace:       testNamespace,
				RetryMultiplier: DefaultRestoreRetryMultiplier,
			},
			wantErr: true,
			errMsg:  "retry-max-attempts must be greater than or equal to 1",
		},
		{
			name: "Retry settings are not validated in validate-only mode",
			restore: &Restore{
				Directory:       testRestoreDir,
				Namespace:       testNamespace,
				RetryMultiplier: 0.5,
				ValidateOnly:    true,
			},
			wantErr: false,
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
		Namespace: "source-ns",
	}

	result := restore.NamespaceConfig()
	require.NotNil(t, result, "Result should not be nil")
	assert.Equal(t, "source-ns", *result.Source, "Source should be 'source-ns'")
	assert.Equal(t, "source-ns", *result.Destination, "Destination should be the same as Source")
}

func TestMapRestoreNamespace_SuccessDifferentNamespaces(t *testing.T) {
	t.Parallel()

	restore := &Restore{
		Namespace: "source-ns,destination-ns",
	}

	result := restore.NamespaceConfig()
	require.NotNil(t, result, "Result should not be nil")
	assert.Equal(t, "source-ns", *result.Source, "Source should be 'source-ns'")
	assert.Equal(t, "destination-ns", *result.Destination, "Destination should be 'destination-ns'")
}

func TestMapRestoreNamespace_InvalidNamespace(t *testing.T) {
	t.Parallel()

	restore := &Restore{
		Namespace: "source-ns,destination-ns,extra-ns",
	}

	result := restore.NamespaceConfig()
	assert.Nil(t, result, "Result should be nil for invalid input")
}

func TestMapWritePolicy_Success(t *testing.T) {
	t.Parallel()

	restoreModel := &Restore{
		Replace:       true,
		Uniq:          false,
		TotalTimeout:  5000,
		SocketTimeout: 1500,
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

func TestRestore_RetryPolicies(t *testing.T) {
	t.Parallel()

	r := &Restore{
		InfoMaxRetries:                4,
		InfoRetriesMultiplier:         1.5,
		InfoRetryIntervalMilliseconds: 250,
		RetryBaseInterval:             5000,
		RetryMultiplier:               2,
		RetryMaxAttempts:              10,
	}

	tests := []struct {
		name           string
		give           func() *models.RetryPolicy
		wantBase       time.Duration
		wantMultiplier float64
		wantMaxRetries uint
	}{
		{
			name:           "write retry policy uses retry settings",
			give:           r.WriteRetryPolicy,
			wantBase:       5 * time.Second,
			wantMultiplier: 2,
			wantMaxRetries: 10,
		},
		{
			name:           "info retry policy uses info retry settings",
			give:           r.InfoRetryPolicy,
			wantBase:       250 * time.Millisecond,
			wantMultiplier: 1.5,
			wantMaxRetries: 4,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := tt.give()
			require.NotNil(t, got)
			assert.Equal(t, tt.wantBase, got.BaseTimeout)
			assert.InDelta(t, tt.wantMultiplier, got.Multiplier, 0.0001)
			assert.Equal(t, tt.wantMaxRetries, got.MaxRetries)
		})
	}
}
