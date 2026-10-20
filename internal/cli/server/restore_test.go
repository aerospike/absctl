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

package server

import (
	"io"
	"testing"

	"github.com/aerospike/absctl/internal/flags"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testAppVersion = "v0.0.0-test"
	testCommitHash = "deadbeef"
	testBuildTime  = "2026-01-01T00:00:00Z"

	testNamespace = "test"

	flagNamespace = "namespace"
	flagJobID     = "job-id"
	flagAsync     = "async"

	errMsgJobIDRequired = "job-id is required"
	errMsgInitPrefix    = "failed to initialize"
)

// newTestRestoreCmd builds the snapshot-restore command with its output discarded.
func newTestRestoreCmd() *cobra.Command {
	cmd := NewRestoreCmd(flags.NewRoot(), testAppVersion, testCommitHash, testBuildTime)
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)

	return cmd
}

// TestNewRestoreCmdSubcommands makes sure every subcommand is registered under its name
// with the flags it is configured by.
func TestNewRestoreCmdSubcommands(t *testing.T) {
	t.Parallel()

	tests := []struct {
		giveUse   string
		wantFlags []string
	}{
		{giveUse: UseStart, wantFlags: []string{flagNamespace, flagJobID, flagAsync, "backup-ids", "parallel-read"}},
		{giveUse: UsePrepare, wantFlags: []string{flagNamespace, flagJobID}},
		{giveUse: UseProgress, wantFlags: []string{flagNamespace, flagJobID, "watch"}},
		{giveUse: UseAbort, wantFlags: []string{flagNamespace, flagJobID}},
		{giveUse: UseOpen, wantFlags: []string{flagNamespace, flagJobID, flagAsync}},
		{giveUse: UseClearStorage, wantFlags: []string{flagNamespace, flagJobID, flagAsync}},
	}

	for _, tt := range tests {
		t.Run(tt.giveUse, func(t *testing.T) {
			t.Parallel()

			sub, _, err := newTestRestoreCmd().Find([]string{tt.giveUse})
			require.NoError(t, err)
			require.Equal(t, tt.giveUse, sub.Use)
			require.NotNil(t, sub.RunE)

			for _, name := range tt.wantFlags {
				assert.NotNil(t, sub.Flags().Lookup(name), "flag %s", name)
			}
		})
	}
}

// TestNewRestoreCmdInitError covers the prefix a subcommand puts on a configuration it
// cannot build the service from. A missing job id fails validation before the cluster
// is reached.
func TestNewRestoreCmdInitError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		giveUse    string
		wantPrefix string
	}{
		{giveUse: UsePrepare},
		{giveUse: UseProgress},
		{giveUse: UseAbort, wantPrefix: "failed to initialize restore abort: "},
		{giveUse: UseOpen, wantPrefix: "failed to initialize restore open: "},
		{giveUse: UseClearStorage, wantPrefix: "failed to initialize restore clear storage: "},
	}

	for _, tt := range tests {
		t.Run(tt.giveUse, func(t *testing.T) {
			t.Parallel()

			cmd := newTestRestoreCmd()
			cmd.SetArgs([]string{tt.giveUse, "--" + flagNamespace, testNamespace})

			err := cmd.ExecuteContext(t.Context())

			require.ErrorContains(t, err, errMsgJobIDRequired)

			if tt.wantPrefix == "" {
				assert.NotContains(t, err.Error(), errMsgInitPrefix)
				return
			}

			assert.Contains(t, err.Error(), tt.wantPrefix)
		})
	}
}
