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
			name: "Continue and remove file",
			backup: &Backup{
				OutputFile: testFile,
				MaxRecords: 10,
				Common:     Common{Parallel: 10},
			},
			wantErr:     true,
			expectedErr: "max-records can't be used with parallel > 1",
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
