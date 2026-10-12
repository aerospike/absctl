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

func TestServerRestore_NewFlagSet(t *testing.T) {
	t.Parallel()

	restore := NewServerRestore()
	flagSet := restore.NewFlagSet()

	args := []string{
		"--namespace", "test-ns",
		"--object-storage-type", "aws-s3",
		"--backup-id", "backup-job-1",
	}

	err := flagSet.Parse(args)
	require.NoError(t, err)

	result := restore.GetServerRestore()

	assert.Equal(t, "test-ns", result.Namespace)
	assert.Equal(t, "aws-s3", result.StorageType)
	assert.Equal(t, "backup-job-1", result.JobID)
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
	assert.Equal(t, models.DefaultServerBackupJobID, result.JobID)
	assert.Equal(t, models.DefaultServerBackupPath, result.Path)
	assert.Equal(t, models.DefaultServerRestoreFuzzyRestore, result.FuzzyRestore)
}

func TestServerRestorePrepare_NewFlagSet(t *testing.T) {
	t.Parallel()

	prepare := NewServerRestorePrepare()
	flagSet := prepare.NewFlagSet()

	args := []string{
		"--namespace", "test-ns",
		"--backup-id", "backup-job-1",
	}

	err := flagSet.Parse(args)
	require.NoError(t, err)

	result := prepare.GetServerRestorePrepare()

	assert.Equal(t, "test-ns", result.Namespace)
	assert.Equal(t, "backup-job-1", result.JobID)
}

func TestServerRestorePrepare_NewFlagSet_DefaultValues(t *testing.T) {
	t.Parallel()

	prepare := NewServerRestorePrepare()
	flagSet := prepare.NewFlagSet()

	err := flagSet.Parse([]string{})
	require.NoError(t, err)

	result := prepare.GetServerRestorePrepare()

	assert.Equal(t, models.DefaultCommonNamespace, result.Namespace)
	assert.Equal(t, models.DefaultServerBackupJobID, result.JobID)
}

func TestServerRestoreAbort_NewFlagSet(t *testing.T) {
	t.Parallel()

	const (
		testNamespace = "test-ns"
		testJobID     = "backup-job-1"
	)

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
			wantJobID:     models.DefaultServerBackupJobID,
		},
		{
			// A restore job is named by both, so both flags have to reach the model.
			name:          "namespace and backup id",
			args:          []string{"--namespace", testNamespace, "--backup-id", testJobID},
			wantNamespace: testNamespace,
			wantJobID:     testJobID,
		},
		{
			name:          "only namespace",
			args:          []string{"--namespace", testNamespace},
			wantNamespace: testNamespace,
			wantJobID:     models.DefaultServerBackupJobID,
		},
		{
			name:          "only backup id",
			args:          []string{"--backup-id", testJobID},
			wantNamespace: models.DefaultCommonNamespace,
			wantJobID:     testJobID,
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
