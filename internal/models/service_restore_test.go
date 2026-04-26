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

	"github.com/stretchr/testify/require"
)

func TestRestoreJobID_Validate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		req     RestoreJobID
		wantErr bool
		errMsg  string
	}{
		{
			name:    "valid id",
			req:     RestoreJobID{JobID: 1},
			wantErr: false,
		},
		{
			name:    "large id",
			req:     RestoreJobID{JobID: 1700000000000},
			wantErr: false,
		},
		{
			name:    "zero id",
			req:     RestoreJobID{JobID: 0},
			wantErr: true,
			errMsg:  "job-id must be a positive integer",
		},
		{
			name:    "negative id",
			req:     RestoreJobID{JobID: -42},
			wantErr: true,
			errMsg:  "job-id must be a positive integer",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.req.Validate()

			if tt.wantErr {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errMsg)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestRestoreRequest_Validate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		req     RestoreRequest
		wantErr bool
		errMsg  string
	}{
		{
			name:    "request file only",
			req:     RestoreRequest{RequestFile: "request.json"},
			wantErr: false,
		},
		{
			name:    "backup data path only",
			req:     RestoreRequest{BackupDataPath: "data/backup-1"},
			wantErr: false,
		},
		{
			name: "both request file and backup data path",
			req: RestoreRequest{
				RequestFile:    "request.json",
				BackupDataPath: "data/backup-1",
			},
			wantErr: false,
		},
		{
			name:    "neither set",
			req:     RestoreRequest{},
			wantErr: true,
			errMsg:  "either --request-file or --backup-data-path must be provided",
		},
		{
			name: "destination/source/secret without backup-data-path is still invalid",
			req: RestoreRequest{
				DestinationName: "dest",
				SourceName:      "src",
				SecretAgentName: "agent",
			},
			wantErr: true,
			errMsg:  "either --request-file or --backup-data-path must be provided",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.req.Validate()

			if tt.wantErr {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errMsg)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestRestoreTimestampRequest_Validate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		req     RestoreTimestampRequest
		wantErr bool
		errMsg  string
	}{
		{
			name:    "request file only",
			req:     RestoreTimestampRequest{RequestFile: "ts.json"},
			wantErr: false,
		},
		{
			name: "routine and time set",
			req: RestoreTimestampRequest{
				Routine: "daily",
				Time:    1700000000000,
			},
			wantErr: false,
		},
		{
			name: "missing routine",
			req: RestoreTimestampRequest{
				Time: 1700000000000,
			},
			wantErr: true,
			errMsg:  "--routine is required",
		},
		{
			name: "missing time",
			req: RestoreTimestampRequest{
				Routine: "daily",
			},
			wantErr: true,
			errMsg:  "--time must be a positive epoch in milliseconds",
		},
		{
			name: "negative time",
			req: RestoreTimestampRequest{
				Routine: "daily",
				Time:    -1,
			},
			wantErr: true,
			errMsg:  "--time must be a positive epoch in milliseconds",
		},
		{
			name:    "all empty",
			req:     RestoreTimestampRequest{},
			wantErr: true,
			errMsg:  "--routine is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.req.Validate()

			if tt.wantErr {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errMsg)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
