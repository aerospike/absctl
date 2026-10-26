// Copyright 2026 Aerospike, Inc.
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
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// errMsgRecordsPerSecond is the start of the error of a records per second limit out of
// the uint32 range.
const errMsgRecordsPerSecond = "records-per-second must be between 0 and 4294967295"

func TestServerCommon_Validate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		common     ServerCommon
		wantErr    bool
		wantErrMsg string
	}{
		{
			name:    "valid common fields",
			common:  ServerCommon{Namespace: testServerNamespace},
			wantErr: false,
		},
		{
			name:       "missing namespace",
			common:     ServerCommon{},
			wantErr:    true,
			wantErrMsg: "namespace is required",
		},
		{
			name: "valid path and filter expression",
			common: ServerCommon{
				Namespace: testServerNamespace,
				Path:      testServerPath,
				FilterExp: testServerFilterExp,
			},
			wantErr: false,
		},
		{
			name:       "path with parent segment",
			common:     ServerCommon{Namespace: testServerNamespace, Path: testParentPath},
			wantErr:    true,
			wantErrMsg: errMsgParentPath,
		},
		{
			name:       "invalid filter expression",
			common:     ServerCommon{Namespace: testServerNamespace, FilterExp: testInvalidFilterExp},
			wantErr:    true,
			wantErrMsg: errMsgInvalidFilterExp,
		},
		{
			name:    "max records per second",
			common:  ServerCommon{Namespace: testServerNamespace, RecordsPerSecond: math.MaxUint32},
			wantErr: false,
		},
		{
			name:       "negative records per second",
			common:     ServerCommon{Namespace: testServerNamespace, RecordsPerSecond: -1},
			wantErr:    true,
			wantErrMsg: errMsgRecordsPerSecond,
		},
		{
			name: "records per second above uint32",
			common: ServerCommon{
				Namespace:        testServerNamespace,
				RecordsPerSecond: math.MaxUint32 + 1,
			},
			wantErr:    true,
			wantErrMsg: errMsgRecordsPerSecond,
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

func TestNormalizeS3Prefix(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		give string
		want string
	}{
		{name: "empty", give: "", want: ""},
		{name: "root", give: "/", want: ""},
		{name: "already normalized", give: testServerPath, want: testServerPath},
		{name: "leading slash", give: "/" + testServerPath, want: testServerPath},
		{name: "trailing slash", give: testServerPath + "/", want: testServerPath},
		{name: "duplicate slashes", give: "backups//daily", want: testServerPath},
		{name: "current directory segment", give: "./backups/./daily", want: testServerPath},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, NormalizeS3Prefix(tt.give))
		})
	}
}
