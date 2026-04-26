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

func TestServiceBackupRoutine_NewFlagSet(t *testing.T) {
	t.Parallel()

	f := NewServiceBackupRoutine()
	flagSet := f.NewFlagSet()

	err := flagSet.Parse([]string{"--name", "daily-backup"})
	require.NoError(t, err)

	assert.Equal(t, "daily-backup", f.Name)
}

func TestServiceBackupRoutine_NewFlagSet_Default(t *testing.T) {
	t.Parallel()

	f := NewServiceBackupRoutine()
	flagSet := f.NewFlagSet()

	err := flagSet.Parse([]string{})
	require.NoError(t, err)

	assert.Empty(t, f.Name)
}

func TestServiceBackupList_NewFlagSet(t *testing.T) {
	t.Parallel()

	f := NewServiceBackupList()
	flagSet := f.NewFlagSet()

	args := []string{
		"--name", "weekly-backup",
		"--from", "1700000000000",
		"--to", "1800000000000",
	}

	err := flagSet.Parse(args)
	require.NoError(t, err)

	assert.Equal(t, "weekly-backup", f.Name)
	assert.Equal(t, int64(1700000000000), f.From)
	assert.Equal(t, int64(1800000000000), f.To)
}

func TestServiceBackupList_NewFlagSet_Defaults(t *testing.T) {
	t.Parallel()

	f := NewServiceBackupList()
	flagSet := f.NewFlagSet()

	err := flagSet.Parse([]string{})
	require.NoError(t, err)

	assert.Empty(t, f.Name)
	assert.Equal(t, int64(0), f.From)
	assert.Equal(t, int64(0), f.To)
}

func TestServiceBackupTrigger_NewFlagSet(t *testing.T) {
	t.Parallel()

	f := NewServiceBackupTrigger()
	flagSet := f.NewFlagSet()

	args := []string{
		"--name", "hourly-backup",
		"--delay", "5000",
	}

	err := flagSet.Parse(args)
	require.NoError(t, err)

	assert.Equal(t, "hourly-backup", f.Name)
	assert.Equal(t, 5000, f.Delay)
}

func TestServiceBackupTrigger_NewFlagSet_Defaults(t *testing.T) {
	t.Parallel()

	f := NewServiceBackupTrigger()
	flagSet := f.NewFlagSet()

	err := flagSet.Parse([]string{})
	require.NoError(t, err)

	assert.Empty(t, f.Name)
	assert.Equal(t, 0, f.Delay)
}
