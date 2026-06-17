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

package flags

import (
	"testing"

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

	assert.Empty(t, result.Namespace)
	assert.Empty(t, result.StorageType)
	assert.Empty(t, result.JobID)
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

	assert.Empty(t, result.Namespace)
	assert.Empty(t, result.JobID)
}
