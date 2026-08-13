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

func TestServerCommon_Validate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		common     ServerCommon
		wantErr    bool
		wantErrMsg string
	}{
		{
			name: "valid common fields",
			common: ServerCommon{
				Namespace:   testServerNamespace,
				StorageType: testServerStorage,
			},
			wantErr: false,
		},
		{
			name: "missing namespace",
			common: ServerCommon{
				StorageType: testServerStorage,
			},
			wantErr:    true,
			wantErrMsg: "namespace is required",
		},
		{
			name: "missing storage type",
			common: ServerCommon{
				Namespace: testServerNamespace,
			},
			wantErr:    true,
			wantErrMsg: "storage-type is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.common.Validate()

			if tt.wantErr {
				require.Error(t, err)
				if tt.wantErrMsg != "" {
					assert.Contains(t, err.Error(), tt.wantErrMsg)
				}
				return
			}
			require.NoError(t, err)
		})
	}
}
