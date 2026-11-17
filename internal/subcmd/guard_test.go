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

package subcmd

import (
	"testing"

	"github.com/aerospike/absctl/internal/flags"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testConfigPath = "cfg.yaml"
	testFlagOther  = "namespace"
	testFlagExtra  = "encrypt"
	testFlagValue  = "x"
)

var (
	argConfig      = "--" + flags.FlagConfig
	argConfigEmpty = argConfig + "="
	argVersion     = "--" + flags.FlagVersion
	argFlagHlp     = "--" + flags.FlagHelp
	argOther       = "--" + testFlagOther
	argExtra       = "--" + testFlagExtra
)

func newGuardCmd(t *testing.T) *cobra.Command {
	t.Helper()

	cmd := &cobra.Command{Use: testCmdName}
	cmd.Flags().String(flags.FlagConfig, "", "")
	cmd.Flags().Bool(flags.FlagVersion, false, "")
	cmd.Flags().Bool(flags.FlagHelp, false, "")
	cmd.Flags().String(testFlagOther, "", "")
	cmd.Flags().String(testFlagExtra, "", "")

	return cmd
}

func TestRejectFlagsWithConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		give      []string
		wantErrIs error
		wantErr   string
	}{
		{name: "no flags"},
		{name: "flags without config", give: []string{argOther, testFlagValue}},
		{name: "config alone", give: []string{argConfig, testConfigPath}},
		{name: "config with version and help", give: []string{argConfig, testConfigPath, argVersion, argFlagHlp}},
		{
			name:      "empty config alone",
			give:      []string{argConfigEmpty},
			wantErrIs: ErrEmptyConfigPath,
		},
		{
			name:      "empty config with other flags",
			give:      []string{argConfigEmpty, argOther, testFlagValue},
			wantErrIs: ErrEmptyConfigPath,
		},
		{
			name:      "config with one other flag",
			give:      []string{argConfig, testConfigPath, argOther, testFlagValue},
			wantErrIs: ErrFlagsWithConfig,
			wantErr:   argOther,
		},
		{
			name:      "config with several other flags",
			give:      []string{argExtra, testFlagValue, argConfig, testConfigPath, argOther, testFlagValue},
			wantErrIs: ErrFlagsWithConfig,
			wantErr:   argExtra + ", " + argOther,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cmd := newGuardCmd(t)
			require.NoError(t, cmd.ParseFlags(tt.give))

			err := RejectFlagsWithConfig(cmd)
			if tt.wantErrIs == nil {
				require.NoError(t, err)

				return
			}

			require.ErrorIs(t, err, tt.wantErrIs)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestRejectEmptyConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		give    []string
		wantErr error
	}{
		{name: "config not given"},
		{name: "config with path", give: []string{argConfig, testConfigPath}},
		{name: "empty config", give: []string{argConfigEmpty}, wantErr: ErrEmptyConfigPath},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cmd := newGuardCmd(t)
			require.NoError(t, cmd.ParseFlags(tt.give))

			err := RejectEmptyConfig(cmd)
			if tt.wantErr == nil {
				require.NoError(t, err)

				return
			}

			require.ErrorIs(t, err, tt.wantErr)
		})
	}
}
