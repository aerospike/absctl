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

func TestConfigResourceName_Validate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		resource ConfigResourceName
		wantErr  bool
		errMsg   string
	}{
		{
			name:     "valid name",
			resource: ConfigResourceName{Name: "primary-cluster"},
			wantErr:  false,
		},
		{
			name:     "empty name",
			resource: ConfigResourceName{},
			wantErr:  true,
			errMsg:   "--name is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.resource.Validate()

			if tt.wantErr {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errMsg)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestConfigUpdate_Validate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		update  ConfigUpdate
		wantErr bool
		errMsg  string
	}{
		{
			name:    "valid",
			update:  ConfigUpdate{File: "/tmp/config.json"},
			wantErr: false,
		},
		{
			name:    "missing file",
			update:  ConfigUpdate{},
			wantErr: true,
			errMsg:  "--file is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.update.Validate()

			if tt.wantErr {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errMsg)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
