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

package flags

import (
	"testing"

	as "github.com/aerospike/aerospike-client-go/v8"
	asFlags "github.com/aerospike/tools-common-go/flags"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHideInsecureAuthMode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		args     []string
		wantErr  bool
		wantMode asFlags.AuthModeFlag
	}{
		{
			name:     "default",
			args:     nil,
			wantMode: asFlags.AuthModeFlag(as.AuthModeInternal),
		},
		{
			name:     "internal",
			args:     []string{"--auth", "INTERNAL"},
			wantMode: asFlags.AuthModeFlag(as.AuthModeInternal),
		},
		{
			name:     "external",
			args:     []string{"--auth", "external"},
			wantMode: asFlags.AuthModeFlag(as.AuthModeExternal),
		},
		{
			name:     "pki",
			args:     []string{"--auth", "PKI"},
			wantMode: asFlags.AuthModeFlag(as.AuthModePKI),
		},
		{
			name:    "external insecure upper case",
			args:    []string{"--auth", authModeExternalInsecure},
			wantErr: true,
		},
		{
			name:    "external insecure lower case",
			args:    []string{"--auth", "external_insecure"},
			wantErr: true,
		},
		{
			name:    "unknown",
			args:    []string{"--auth", "UNKNOWN"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			af := asFlags.NewDefaultAerospikeFlags()
			fs := af.NewFlagSet(asFlags.DefaultWrapHelpString)
			hideInsecureAuthMode(fs)

			err := fs.Parse(tt.args)
			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantMode, af.AuthMode)
		})
	}
}

func TestHideInsecureAuthMode_Help(t *testing.T) {
	t.Parallel()

	af := asFlags.NewDefaultAerospikeFlags()
	fs := af.NewFlagSet(asFlags.DefaultWrapHelpString)
	hideInsecureAuthMode(fs)

	f := fs.Lookup(flagAuth)
	require.NotNil(t, f)

	assert.Equal(t, authModeTypes, f.Value.Type())
	assert.NotContains(t, f.Usage, authModeExternalInsecure)
	assert.NotContains(t, fs.FlagUsages(), authModeExternalInsecure)
	assert.Equal(t, "INTERNAL", f.Value.String())
}
