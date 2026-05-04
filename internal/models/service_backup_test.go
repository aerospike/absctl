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

func TestBackupRoutine_Validate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		routine BackupRoutine
		wantErr bool
		errMsg  string
	}{
		{
			name:    "valid name",
			routine: BackupRoutine{Name: "daily-backup"},
			wantErr: false,
		},
		{
			name:    "empty name",
			routine: BackupRoutine{Name: ""},
			wantErr: true,
			errMsg:  "routine name is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.routine.Validate()

			if tt.wantErr {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errMsg)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestBackupTrigger_Validate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		trigger BackupTrigger
		wantErr bool
		errMsg  string
	}{
		{
			name:    "valid name no delay",
			trigger: BackupTrigger{Name: "daily"},
			wantErr: false,
		},
		{
			name:    "valid name with delay",
			trigger: BackupTrigger{Name: "daily", Delay: 1500},
			wantErr: false,
		},
		{
			name:    "empty name",
			trigger: BackupTrigger{Delay: 1000},
			wantErr: true,
			errMsg:  "routine name is required",
		},
		{
			name:    "negative delay",
			trigger: BackupTrigger{Name: "daily", Delay: -5},
			wantErr: true,
			errMsg:  "delay must be non-negative",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.trigger.Validate()

			if tt.wantErr {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errMsg)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
