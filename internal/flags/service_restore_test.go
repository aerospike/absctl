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

func TestServiceRestoreJobID_NewFlagSet(t *testing.T) {
	t.Parallel()

	f := NewServiceRestoreJobID()
	flagSet := f.NewFlagSet()

	err := flagSet.Parse([]string{"--job-id", "42"})
	require.NoError(t, err)

	assert.Equal(t, int64(42), f.JobID)
}

func TestServiceRestoreJobID_NewFlagSet_Default(t *testing.T) {
	t.Parallel()

	f := NewServiceRestoreJobID()
	flagSet := f.NewFlagSet()

	err := flagSet.Parse([]string{})
	require.NoError(t, err)

	assert.Equal(t, int64(0), f.JobID)
}

func TestServiceRestoreJobs_NewFlagSet(t *testing.T) {
	t.Parallel()

	f := NewServiceRestoreJobs()
	flagSet := f.NewFlagSet()

	args := []string{
		"--from", "1700000000000",
		"--to", "1800000000000",
		"--status", "Running,Done",
	}

	err := flagSet.Parse(args)
	require.NoError(t, err)

	assert.Equal(t, int64(1700000000000), f.From)
	assert.Equal(t, int64(1800000000000), f.To)
	assert.Equal(t, "Running,Done", f.Status)
}

func TestServiceRestoreJobs_NewFlagSet_Defaults(t *testing.T) {
	t.Parallel()

	f := NewServiceRestoreJobs()
	flagSet := f.NewFlagSet()

	err := flagSet.Parse([]string{})
	require.NoError(t, err)

	assert.Equal(t, int64(0), f.From)
	assert.Equal(t, int64(0), f.To)
	assert.Empty(t, f.Status)
}

func TestServiceRestoreRequest_NewFlagSet(t *testing.T) {
	t.Parallel()

	f := NewServiceRestoreRequest()
	flagSet := f.NewFlagSet()

	args := []string{
		"--request-file", "request.json",
		"--backup-data-path", "data/backup-1",
		"--destination-name", "dest-cluster",
		"--source-name", "src-storage",
		"--secret-agent-name", "agent-1",
	}

	err := flagSet.Parse(args)
	require.NoError(t, err)

	assert.Equal(t, "request.json", f.RequestFile)
	assert.Equal(t, "data/backup-1", f.BackupDataPath)
	assert.Equal(t, "dest-cluster", f.DestinationName)
	assert.Equal(t, "src-storage", f.SourceName)
	assert.Equal(t, "agent-1", f.SecretAgentName)
}

func TestServiceRestoreRequest_NewFlagSet_Defaults(t *testing.T) {
	t.Parallel()

	f := NewServiceRestoreRequest()
	flagSet := f.NewFlagSet()

	err := flagSet.Parse([]string{})
	require.NoError(t, err)

	assert.Empty(t, f.RequestFile)
	assert.Empty(t, f.BackupDataPath)
	assert.Empty(t, f.DestinationName)
	assert.Empty(t, f.SourceName)
	assert.Empty(t, f.SecretAgentName)
}

func TestServiceRestoreTimestamp_NewFlagSet(t *testing.T) {
	t.Parallel()

	f := NewServiceRestoreTimestamp()
	flagSet := f.NewFlagSet()

	args := []string{
		"--request-file", "ts.json",
		"--routine", "daily",
		"--time", "1700000000000",
		"--destination-name", "dest-cluster",
		"--secret-agent-name", "agent-1",
		"--disable-reordering",
	}

	err := flagSet.Parse(args)
	require.NoError(t, err)

	assert.Equal(t, "ts.json", f.RequestFile)
	assert.Equal(t, "daily", f.Routine)
	assert.Equal(t, int64(1700000000000), f.Time)
	assert.Equal(t, "dest-cluster", f.DestinationName)
	assert.Equal(t, "agent-1", f.SecretAgentName)
	assert.True(t, f.DisableReordering)
}

func TestServiceRestoreTimestamp_NewFlagSet_Defaults(t *testing.T) {
	t.Parallel()

	f := NewServiceRestoreTimestamp()
	flagSet := f.NewFlagSet()

	err := flagSet.Parse([]string{})
	require.NoError(t, err)

	assert.Empty(t, f.RequestFile)
	assert.Empty(t, f.Routine)
	assert.Equal(t, int64(0), f.Time)
	assert.Empty(t, f.DestinationName)
	assert.Empty(t, f.SecretAgentName)
	assert.False(t, f.DisableReordering)
}
