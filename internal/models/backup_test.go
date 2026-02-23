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
	"fmt"
	"testing"
	"time"

	appConfig "github.com/aerospike/absctl/internal/config"
	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/backup-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testNamespace = "test"
	testDir       = "test-dir"
	testFile      = "test-file"
)

func TestValidateBackup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		backup      *Backup
		wantErr     bool
		expectedErr string
	}{
		{
			name: "Both AfterDigest and PartitionList configured",
			backup: &Backup{
				AfterDigest:   "some-digest",
				PartitionList: "some-partition",
				Common: Common{
					Directory: testDir,
				},
			},
			wantErr:     true,
			expectedErr: "only one of after-digest or partition-list can be configured",
		},
		{
			name: "Only AfterDigest configured",
			backup: &Backup{
				AfterDigest:   "some-digest",
				PartitionList: "",
				OutputFile:    testFile,
				Common: Common{
					Namespace: testNamespace,
				},
			},
			wantErr:     false,
			expectedErr: "",
		},
		{
			name: "Only PartitionList configured",
			backup: &Backup{
				AfterDigest:   "",
				PartitionList: "some-partition",
				OutputFile:    testFile,
				Common: Common{
					Namespace: testNamespace,
				},
			},

			wantErr:     false,
			expectedErr: "",
		},
		{
			name: "Neither AfterDigest nor PartitionList configured",
			backup: &Backup{
				AfterDigest:   "",
				PartitionList: "",
				OutputFile:    testFile,
				Common: Common{
					Namespace: testNamespace,
				},
			},

			wantErr:     false,
			expectedErr: "",
		},
		{
			name: "Estimate with PartitionList",
			backup: &Backup{
				Estimate:      true,
				PartitionList: "some-partition",
			},

			wantErr:     true,
			expectedErr: "estimate with any filter is not allowed",
		},
		{
			name: "Estimate with output file",
			backup: &Backup{
				Estimate:   true,
				OutputFile: testFile,
			},

			wantErr:     true,
			expectedErr: "estimate with output-file or directory is not allowed",
		},
		{
			name: "Estimate with valid configuration",
			backup: &Backup{
				Estimate:        true,
				EstimateSamples: 100,
				Common: Common{
					Namespace: testNamespace,
				},
			},

			wantErr:     false,
			expectedErr: "",
		},
		{
			name: "Estimate with invalid samples size",
			backup: &Backup{
				Estimate:        true,
				EstimateSamples: -1,
			},

			wantErr:     true,
			expectedErr: "estimate with estimate-samples < 0 is not allowed",
		},
		{
			name: "Non-estimate with no output or directory",
			backup: &Backup{
				Estimate:   false,
				OutputFile: "",
				Common:     Common{Directory: ""},
			},
			wantErr:     true,
			expectedErr: "must specify either estimate, output-file or directory",
		},
		{
			name: "Non-estimate with output file",
			backup: &Backup{
				Estimate:   false,
				OutputFile: testFile,
				Common: Common{
					Directory: "",
					Namespace: testNamespace,
				},
			},
			wantErr:     false,
			expectedErr: "",
		},
		{
			name: "Non-estimate with directory",
			backup: &Backup{
				Estimate:   false,
				OutputFile: "",
				Common: Common{
					Directory: testDir,
					Namespace: testNamespace,
				},
			},
			wantErr:     false,
			expectedErr: "",
		},
		{
			name: "Continue with valid state file",
			backup: &Backup{
				Continue:   "state.json",
				OutputFile: testFile,
				Common: Common{
					Namespace: testNamespace,
				},
			},

			wantErr: false,
		},
		{
			name: "NodeList with parallel nodes",
			backup: &Backup{
				NodeList:   "node1,node2",
				OutputFile: testFile,
				Common: Common{
					Namespace: testNamespace,
				},
			},

			wantErr: false,
		},
		{
			name: "FilterExpression with valid expression",
			backup: &Backup{
				FilterExpression: "age > 25",
				OutputFile:       testFile,
				Common: Common{
					Namespace: testNamespace,
				},
			},

			wantErr: false,
		},
		{
			name: "Modified time filters",
			backup: &Backup{
				ModifiedAfter:  "2024-01-01",
				ModifiedBefore: "2024-12-31",
				OutputFile:     testFile,
				Common: Common{
					Namespace: testNamespace,
				},
			},

			wantErr: false,
		},
		{
			name: "NoTTLOnly flag",
			backup: &Backup{
				NoTTLOnly:  true,
				OutputFile: testFile,
				Common: Common{
					Namespace: testNamespace,
				},
			},

			wantErr: false,
		},
		{
			name: "Estimate with FilterExpression",
			backup: &Backup{
				Estimate:         true,
				FilterExpression: "age > 25",
			},

			wantErr:     true,
			expectedErr: "estimate with any filter is not allowed",
		},
		{
			name: "Estimate with ModifiedAfter",
			backup: &Backup{
				Estimate:      true,
				ModifiedAfter: "2024-01-01",
			},

			wantErr:     true,
			expectedErr: "estimate with any filter is not allowed",
		},
		{
			name: "Both directory and output file configured",
			backup: &Backup{
				OutputFile: testFile,
				Common:     Common{Directory: testDir},
			},
			wantErr:     true,
			expectedErr: "only one of output-file and directory may be configured at the same time",
		},
		{
			name: "Both node-list and rack-list configured",
			backup: &Backup{
				NodeList: "1,2",
				RackList: "3,4",
				Common:   Common{Directory: testDir},
			},
			wantErr:     true,
			expectedErr: "only one of node-list or rack-list can be configured",
		},
		{
			name: "Both continue and state-file-dst configured",
			backup: &Backup{
				Continue:     "state",
				StateFileDst: "state",
				Common:       Common{Directory: testDir},
			},
			wantErr:     true,
			expectedErr: "continue and state-file-dst are mutually exclusive",
		},
		{
			name: "Prefix and output file set",
			backup: &Backup{
				OutputFilePrefix: testFile,
				OutputFile:       testFile,
			},
			wantErr:     true,
			expectedErr: "using output-file-prefix is not allowed with output-file",
		},
		{
			name: "Continue and remove file",
			backup: &Backup{
				OutputFile:  testFile,
				Continue:    testFile,
				RemoveFiles: true,
			},
			wantErr:     true,
			expectedErr: "continue and remove-files are mutually exclusive, as remove-files will delete the backup files",
		},
		{
			name: "Max-records set with parallel > 1",
			backup: &Backup{
				OutputFile: testFile,
				MaxRecords: 10,
				Common:     Common{Parallel: 10},
			},
			wantErr:     true,
			expectedErr: "max-records must be used with parallel = 1",
		},
		{
			name: "Max-records valid",
			backup: &Backup{
				OutputFile: testFile,
				MaxRecords: 10,
				Common:     Common{Namespace: testNamespace, Parallel: 1},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.backup.Validate()
			if tt.wantErr {
				require.Error(t, err, "Expected error but got none")
				assert.Equal(t, tt.expectedErr, err.Error())
			} else {
				require.NoError(t, err, "Expected no error but got one")
			}
		})
	}
}

func TestBackup_validateSingleFilter(t *testing.T) {
	tests := []struct {
		name          string
		backup        *Backup
		expectedError bool
		errorContains string
	}{
		{
			name:          "no filters set - valid",
			backup:        &Backup{},
			expectedError: false,
		},
		{
			name: "only after-digest set - valid",
			backup: &Backup{
				AfterDigest: "some-digest",
			},
			expectedError: false,
		},
		{
			name: "only partition-list set - valid",
			backup: &Backup{
				PartitionList: "1,2,3",
			},
			expectedError: false,
		},
		{
			name: "only node-list set - valid",
			backup: &Backup{
				NodeList: "node1,node2",
			},
			expectedError: false,
		},
		{
			name: "only rack-list set - valid",
			backup: &Backup{
				RackList: "rack1,rack2",
			},
			expectedError: false,
		},
		{
			name: "after-digest and partition-list - invalid",
			backup: &Backup{
				AfterDigest:   "some-digest",
				PartitionList: "1,2,3",
			},
			expectedError: true,
			errorContains: "only one of",
		},
		{
			name: "after-digest and node-list - invalid",
			backup: &Backup{
				AfterDigest: "some-digest",
				NodeList:    "node1",
			},
			expectedError: true,
			errorContains: "only one of",
		},
		{
			name: "after-digest and rack-list - invalid",
			backup: &Backup{
				AfterDigest: "some-digest",
				RackList:    "rack1",
			},
			expectedError: true,
			errorContains: "only one of",
		},
		{
			name: "partition-list and node-list - invalid",
			backup: &Backup{
				PartitionList: "1,2,3",
				NodeList:      "node1",
			},
			expectedError: true,
			errorContains: "only one of",
		},
		{
			name: "partition-list and rack-list - invalid",
			backup: &Backup{
				PartitionList: "1,2,3",
				RackList:      "rack1",
			},
			expectedError: true,
			errorContains: "only one of",
		},
		{
			name: "node-list and rack-list - invalid",
			backup: &Backup{
				NodeList: "node1",
				RackList: "rack1",
			},
			expectedError: true,
			errorContains: "only one of",
		},
		{
			name: "three filters set - invalid",
			backup: &Backup{
				AfterDigest:   "digest",
				PartitionList: "1,2,3",
				NodeList:      "node1",
			},
			expectedError: true,
			errorContains: "only one of",
		},
		{
			name: "all four filters set - invalid",
			backup: &Backup{
				AfterDigest:   "digest",
				PartitionList: "1,2,3",
				NodeList:      "node1",
				RackList:      "rack1",
			},
			expectedError: true,
			errorContains: "only one of",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.backup.validateSingleFilter()

			if tt.expectedError {
				require.Error(t, err)
				if tt.errorContains != "" {
					assert.Contains(t, err.Error(), tt.errorContains)
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestValidateFilePrefix(t *testing.T) {
	tests := []struct {
		name      string
		prefix    string
		wantError bool
	}{
		{
			name:      "empty prefix is valid",
			prefix:    "",
			wantError: false,
		},
		{
			name:      "valid alphanumeric prefix",
			prefix:    "backup123",
			wantError: false,
		},
		{
			name:      "valid prefix with dash and underscore",
			prefix:    "backup-2024_v1",
			wantError: false,
		},
		{
			name:      "valid prefix with dot",
			prefix:    "backup.v1",
			wantError: false,
		},
		{
			name:      "invalid forward slash",
			prefix:    "backup/test",
			wantError: true,
		},
		{
			name:      "invalid backslash",
			prefix:    "backup\\test",
			wantError: true,
		},
		{
			name:      "backslash at start",
			prefix:    "\\backup",
			wantError: true,
		},
		{
			name:      "invalid colon",
			prefix:    "backup:test",
			wantError: true,
		},
		{
			name:      "invalid asterisk",
			prefix:    "backup*",
			wantError: true,
		},
		{
			name:      "invalid question mark",
			prefix:    "backup?",
			wantError: true,
		},
		{
			name:      "invalid double quote",
			prefix:    "backup\"test",
			wantError: true,
		},
		{
			name:      "invalid less than",
			prefix:    "backup<test",
			wantError: true,
		},
		{
			name:      "invalid greater than",
			prefix:    "backup>test",
			wantError: true,
		},
		{
			name:      "invalid pipe",
			prefix:    "backup|test",
			wantError: true,
		},
		{
			name:      "invalid null character",
			prefix:    "backup\x00test",
			wantError: true,
		},
		{
			name:      "invalid control character tab",
			prefix:    "backup\ttest",
			wantError: true,
		},
		{
			name:      "invalid control character newline",
			prefix:    "backup\ntest",
			wantError: true,
		},
		{
			name:      "leading whitespace",
			prefix:    " backup",
			wantError: true,
		},
		{
			name:      "trailing whitespace",
			prefix:    "backup ",
			wantError: true,
		},
		{
			name:      "valid with internal space",
			prefix:    "backup test",
			wantError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateFilePrefix(tt.prefix)
			if tt.wantError {
				require.Error(t, err, "expected error for prefix: %q", tt.prefix)
			} else {
				require.NoError(t, err, "unexpected error for prefix: %q", tt.prefix)
			}
		})
	}
}

func TestMapPartitionFilter_AfterDigest(t *testing.T) {
	t.Parallel()

	backupModel := &Backup{
		AfterDigest: "AvDsV2KuSZHZugDBftnLxGpR+88=",
		Common: Common{
			Namespace: "test-namespace",
		},
	}

	filters, err := backupModel.PartitionFilters()
	require.NoError(t, err)
	assert.NotNil(t, filters)
	assert.Len(t, filters, 1)
	assert.IsType(t, &aerospike.PartitionFilter{}, filters[0])
}

func TestMapPartitionFilter_PartitionList(t *testing.T) {
	t.Parallel()

	backupModel := &Backup{
		PartitionList: "0-1024",
		Common: Common{
			Namespace: "test-namespace",
		},
	}

	filters, err := backupModel.PartitionFilters()
	require.NoError(t, err)
	assert.NotNil(t, filters)
	assert.Len(t, filters, 1)
	assert.IsType(t, &aerospike.PartitionFilter{}, filters[0])
}

func TestMapPartitionFilter_NoFilters(t *testing.T) {
	t.Parallel()

	backupModel := &Backup{
		Common: Common{
			Namespace: "test-namespace",
		},
	}

	filters, err := backupModel.PartitionFilters()
	require.NoError(t, err)
	assert.NotNil(t, filters)
	assert.Len(t, filters, 1)
	assert.Equal(t, backup.NewPartitionFilterAll(), filters[0])
}

func TestMapScanPolicy_Success(t *testing.T) {
	t.Parallel()

	backupModel := &Backup{
		MaxRecords:          500,
		SleepBetweenRetries: 50,
		FilterExpression:    "k1EDpHRlc3Q=",
		PreferRacks:         "rack1",
		NoBins:              true,
		MaxRetries:          3,
		Common: Common{
			TotalTimeout:  10000,
			SocketTimeout: 3000,
		},
	}
	scanPolicy, err := backupModel.ScanPolicy()
	require.NoError(t, err)
	assert.Equal(t, int64(500), scanPolicy.MaxRecords)
	assert.Equal(t, 3, scanPolicy.MaxRetries)
	assert.Equal(t, 50*time.Millisecond, scanPolicy.SleepBetweenRetries)
	assert.Equal(t, 10000*time.Millisecond, scanPolicy.TotalTimeout)
	assert.Equal(t, 3000*time.Millisecond, scanPolicy.SocketTimeout)
	assert.Equal(t, aerospike.PREFER_RACK, scanPolicy.ReplicaPolicy)
	assert.False(t, scanPolicy.IncludeBinData)
}

func TestMapScanPolicy_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		backupModel *Backup
		commonModel *Common
		wantErr     bool
		errContains string
	}{
		{
			name: "invalid filter expression",
			backupModel: &Backup{
				FilterExpression: "invalid-base64",
			},
			commonModel: &Common{},
			wantErr:     true,
			errContains: "failed to parse filter expression",
		},
		{
			name:        "empty models",
			backupModel: &Backup{},
			commonModel: &Common{},
			wantErr:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := tt.backupModel.ScanPolicy()
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errContains)
				assert.Nil(t, got)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, got)
			}
		})
	}
}

func TestSplitByComma_EmptyString(t *testing.T) {
	t.Parallel()

	result := SplitByComma("")
	assert.Nil(t, result)
}

func TestSplitByComma_NonEmptyString(t *testing.T) {
	t.Parallel()

	result := SplitByComma("item1,item2,item3")
	assert.Equal(t, []string{"item1", "item2", "item3"}, result)
}

func TestRecordExistsAction(t *testing.T) {
	t.Parallel()

	assert.Equal(t, aerospike.REPLACE, recordExistsAction(true, false))
	assert.Equal(t, aerospike.CREATE_ONLY, recordExistsAction(false, true))
	assert.Equal(t, aerospike.UPDATE, recordExistsAction(false, false))
}

func TestParseLocalTimeToUTC(t *testing.T) {
	tests := []struct {
		name        string
		timeString  string
		expectedUTC string
		expectError bool
		errorText   string
	}{
		{
			name:        "Valid DateTime",
			timeString:  "2023-09-01_12:34:56",
			expectedUTC: "2023-09-01_12:34:56",
			expectError: false,
		},
		{
			name:        "Valid Date Only",
			timeString:  "2023-09-01",
			expectedUTC: "2023-09-01_00:00:00",
			expectError: false,
		},
		{
			name:        "Valid Time Only",
			timeString:  "12:34:56",
			expectedUTC: time.Now().Format("2006-01-02") + "_12:34:56",
			expectError: false,
		},
		{
			name:        "Invalid Format",
			timeString:  "invalid-format",
			expectedUTC: "",
			expectError: true,
			errorText:   "unknown time format",
		},
		{
			name:        "Invalid Date",
			timeString:  "2023-13-01_12:00:00",
			expectedUTC: "",
			expectError: true,
			errorText:   "failed to parse time",
		},
		{
			name:        "Invalid Time",
			timeString:  "2023-09-01_25:00:00",
			expectedUTC: "",
			expectError: true,
			errorText:   "failed to parse time",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parseLocalTimeToUTC(tt.timeString)

			if tt.expectError {
				require.Error(t, err)
				assert.Equal(t, time.Time{}, result)
				assert.Contains(t, err.Error(), tt.errorText)
			} else {
				require.NoError(t, err)
				location, err := time.LoadLocation("Local")
				require.NoError(t, err)
				localTime, err := time.ParseInLocation("2006-01-02_15:04:05", tt.expectedUTC, location)
				require.NoError(t, err)
				assert.Equal(t, localTime.UTC(), result)
			}
		})
	}
}

func TestValidatePartitionFilters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		partitionFilters []*aerospike.PartitionFilter
		wantErr          bool
	}{
		{
			name: "Single valid partition filter",
			partitionFilters: []*aerospike.PartitionFilter{
				{Begin: 0, Count: 1},
			},
			wantErr: false,
		},
		{
			name: "Non-overlapping partition filters",
			partitionFilters: []*aerospike.PartitionFilter{
				{Begin: 0, Count: 5},
				{Begin: 10, Count: 5},
			},
			wantErr: false,
		},
		{
			name: "Overlapping partition filters",
			partitionFilters: []*aerospike.PartitionFilter{
				{Begin: 0, Count: 10},
				{Begin: 5, Count: 10},
			},
			wantErr: true,
		},
		{
			name: "Border partition filters",
			partitionFilters: []*aerospike.PartitionFilter{
				{Begin: 0, Count: 1000},
				{Begin: 1000, Count: 3000},
			},
			wantErr: false,
		},
		{
			name: "Duplicate begin value",
			partitionFilters: []*aerospike.PartitionFilter{
				{Begin: 0, Count: 1},
				{Begin: 0, Count: 1},
			},
			wantErr: true,
		},
		{
			name: "Mixed filters with no overlap",
			partitionFilters: []*aerospike.PartitionFilter{
				{Begin: 0, Count: 1},
				{Begin: 5, Count: 5},
				{Begin: 20, Count: 1},
				{Begin: 30, Count: 10},
			},
			wantErr: false,
		},
		{
			name: "Invalid count in filter",
			partitionFilters: []*aerospike.PartitionFilter{
				{Begin: 0, Count: 0},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := validatePartitionFilters(tt.partitionFilters)
			if tt.wantErr {
				assert.Error(t, err, "Expected error but got none")
			} else {
				assert.NoError(t, err, "Expected no error but got one")
			}
		})
	}
}

func TestParseRacks(t *testing.T) {
	tests := []struct {
		name        string
		racks       string
		expected    []int
		expectError bool
		errorText   string
	}{
		{
			name:     "Single Valid Rack",
			racks:    "1",
			expected: []int{1},
		},
		{
			name:     "Multiple Valid Racks",
			racks:    "1,2,3",
			expected: []int{1, 2, 3},
		},
		{
			name:        "Invalid Rack - Non-integer",
			racks:       "1,abc,3",
			expected:    nil,
			expectError: true,
			errorText:   "failed to parse racks",
		},
		{
			name:        "Invalid Rack - Negative Value",
			racks:       "1,-2,3",
			expected:    nil,
			expectError: true,
			errorText:   "rack id -2 invalid, should be non-negative number",
		},
		{
			name:        "Invalid Rack - Exceeds MaxRack",
			racks:       fmt.Sprintf("1,%d,3", appConfig.MaxRack+1),
			expected:    nil,
			expectError: true,
			errorText: fmt.Sprintf("rack id %d invalid, should not exceed %d",
				appConfig.MaxRack+1, appConfig.MaxRack),
		},
		{
			name:     "Empty Input",
			racks:    "",
			expected: []int{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			backup := &Backup{
				RackList: tt.racks,
			}

			result, err := backup.Racks()

			if tt.expectError {
				require.Error(t, err)
				assert.Nil(t, result)
				assert.Contains(t, err.Error(), tt.errorText)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}
