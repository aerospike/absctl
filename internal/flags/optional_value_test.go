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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestOptionalValue_Help pins down that an optional flag reads in the help as a plain flag
// of its type does: its type name and the server default it falls back to.
func TestOptionalValue_Help(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		wantType    string
		wantDefault string
	}{
		{name: "hydrate-replica", wantType: "bool", wantDefault: "true"},
		{name: "max-inflight", wantType: "int", wantDefault: "200"},
		{name: "retry-base-interval", wantType: "int", wantDefault: "1000"},
		{name: "retry-multiplier", wantType: "float64", wantDefault: "1"},
		{name: "retry-max-attempts", wantType: "int", wantDefault: "5"},
		{name: "ignore-record-error", wantType: "bool", wantDefault: "false"},
		{name: "apply-metadata-last", wantType: "bool", wantDefault: "false"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			flag := NewServerRestore().NewFlagSet().Lookup(tt.name)
			require.NotNil(t, flag)

			assert.Equal(t, tt.wantType, flag.Value.Type())
			assert.Equal(t, tt.wantDefault, flag.DefValue)
			assert.Equal(t, tt.wantDefault, flag.Value.String())
		})
	}
}

// TestOptionalValue_String pins down that a flag that is set reports the value it is set
// to rather than the default.
func TestOptionalValue_String(t *testing.T) {
	t.Parallel()

	const givenMaxInflight = "500"

	restore := NewServerRestore()
	fs := restore.NewFlagSet()

	require.NoError(t, fs.Parse([]string{"--max-inflight", givenMaxInflight}))

	assert.Equal(t, givenMaxInflight, fs.Lookup("max-inflight").Value.String())
	assert.Equal(t, new(500), restore.GetServerRestore().MaxInflight)
}
