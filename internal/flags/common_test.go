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

	"github.com/aerospike/absctl/internal/models"
	"github.com/stretchr/testify/assert"
)

func TestCommon_NewFlagSet(t *testing.T) {
	t.Parallel()

	var fields models.Common

	common := NewCommon(&fields, OperationBackup)

	flagSet := common.NewFlagSet()

	args := []string{
		"--directory", "/path/to/backup",
		"--namespace", "test-namespace",
		"--set-list", "set1,set2",
		"--records-per-second", "5000",
		"--bin-list", "bin1,bin2",
		"--parallel", "10",
		"--no-records",
		"--no-indexes",
		"--no-udfs",
		"--total-timeout", "30000",
		"--socket-timeout", "15000",
		"--info-timeout", "1000",
		"--info-retry-interval", "1",
		"--info-retry-multiplier", "1",
		"--info-max-retries", "1",
		"--std-buffer", "1",
	}

	err := flagSet.Parse(args)
	assert.NoError(t, err)

	result := common.GetCommon()

	assert.Equal(t, "/path/to/backup", result.Directory, "The directory flag should be parsed correctly")
	assert.Equal(t, "test-namespace", result.Namespace, "The namespace flag should be parsed correctly")
	assert.Equal(t, "set1,set2", result.SetList, "The set list flag should be parsed correctly")
	assert.Equal(t, 5000, result.RecordsPerSecond, "The records-per-second flag should be parsed correctly")
	assert.Equal(t, "bin1,bin2", result.BinList, "The bin-list flag should be parsed correctly")
	assert.Equal(t, 10, result.Parallel, "The parallel flag should be parsed correctly")
	assert.True(t, result.NoRecords, "The no-records flag should be parsed correctly")
	assert.True(t, result.NoIndexes, "The no-indexes flag should be parsed correctly")
	assert.True(t, result.NoUDFs, "The no-udfs flag should be parsed correctly")
	assert.Equal(t, int64(30000), result.TotalTimeout, "The total-timeout flag should be parsed correctly")
	assert.Equal(t, int64(15000), result.SocketTimeout, "The socket-timeout flag should be parsed correctly")
	assert.Equal(t, int64(1000), result.InfoTimeout, "The info-timeout flag should be parsed correctly")
	assert.Equal(t, int64(1), result.InfoRetryIntervalMilliseconds, "The info-retry-interval flag should be parsed correctly")
	assert.Equal(t, float64(1), result.InfoRetriesMultiplier, "The info-retry-multiplier flag should be parsed correctly")
	assert.Equal(t, uint(1), result.InfoMaxRetries, "The info-max-retries flag should be parsed correctly")
	assert.Equal(t, 1, result.StdBufferSize, "The std-buffer flag should be parsed correctly")
}

func TestCommon_NewFlagSet_DefaultValues(t *testing.T) {
	t.Parallel()

	var fields models.Common

	common := NewCommon(&fields, OperationRestore)

	flagSet := common.NewFlagSet()

	err := flagSet.Parse([]string{})
	assert.NoError(t, err)

	result := common.GetCommon()

	// Verify default values
	assert.Equal(t, "", result.Directory, "The default value for directory should be an empty string")
	assert.Equal(t, "", result.Namespace, "The default value for namespace should be an empty string")
	assert.Equal(t, "", result.SetList, "The default value for set-list should be nil")
	assert.Equal(t, 0, result.RecordsPerSecond, "The default value for records-per-second should be 0")
	assert.Equal(t, "", result.BinList, "The default value for bin-list should be nil")
	assert.Equal(t, 0, result.Parallel, "The default value for parallel should be 0")
	assert.False(t, result.NoRecords, "The default value for no-records should be false")
	assert.False(t, result.NoIndexes, "The default value for no-indexes should be false")
	assert.False(t, result.NoUDFs, "The default value for no-udfs should be false")
	assert.Equal(t, int64(10000), result.TotalTimeout, "The default value for total-timeout should be 0")
	assert.Equal(t, int64(10000), result.SocketTimeout, "The default value for socket-timeout should be 10000")
	assert.Equal(t, int64(10000), result.InfoTimeout, "The default value for info-timeout should be 10000")
	assert.Equal(t, int64(1000), result.InfoRetryIntervalMilliseconds, "The default value for info-retry-interval should be 1000")
	assert.Equal(t, float64(1), result.InfoRetriesMultiplier, "The default value for info-retry-multiplier should be 1")
	assert.Equal(t, uint(3), result.InfoMaxRetries, "The default value for info-max-retries should be 3")
	assert.Equal(t, 4, result.StdBufferSize, "The default value for std-buffer should be 4194304")
}
