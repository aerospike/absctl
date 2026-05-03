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

// validator is implemented by every per-resource fields model.
type validator interface {
	Validate() error
}

func TestConfigFields_Validate_RequireName(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		v    validator
	}{
		{"cluster", &ConfigClusterFields{}},
		{"cluster_with_name", &ConfigClusterFields{Name: "primary"}},
		{"policy", &ConfigPolicyFields{}},
		{"policy_with_name", &ConfigPolicyFields{Name: "daily"}},
		{"routine", &ConfigRoutineFields{}},
		{"routine_with_name", &ConfigRoutineFields{Name: "hourly"}},
		{"storage", &ConfigStorageFields{}},
		{"storage_with_name", &ConfigStorageFields{Name: "local"}},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.v.Validate()

			switch v := tt.v.(type) {
			case *ConfigClusterFields:
				if v.Name == "" {
					require.Error(t, err)
					require.Contains(t, err.Error(), "--name is required")
				} else {
					require.NoError(t, err)
				}
			case *ConfigPolicyFields:
				if v.Name == "" {
					require.Error(t, err)
					require.Contains(t, err.Error(), "--name is required")
				} else {
					require.NoError(t, err)
				}
			case *ConfigRoutineFields:
				if v.Name == "" {
					require.Error(t, err)
					require.Contains(t, err.Error(), "--name is required")
				} else {
					require.NoError(t, err)
				}
			case *ConfigStorageFields:
				if v.Name == "" {
					require.Error(t, err)
					require.Contains(t, err.Error(), "--name is required")
				} else {
					require.NoError(t, err)
				}
			}
		})
	}
}
