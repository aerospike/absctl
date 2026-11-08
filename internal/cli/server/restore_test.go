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

	errMsgJobIDRequired     = "job-id is required"
	errMsgNamespaceRequired = "namespace is required"
	errMsgInitPrefix        = "failed to initialize"
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
		{giveUse: UseStart, wantFlags: []string{
			flagNamespace, flagAsync, "backup-ids", "parallel-read", "hydrate-replica",
		}},
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
// cannot build the service from. A missing job id, or for the open a missing namespace,
// fails validation before the cluster is reached.
func TestNewRestoreCmdInitError(t *testing.T) {
	t.Parallel()

	withNamespace := []string{"--" + flagNamespace, testNamespace}

	tests := []struct {
		giveUse    string
		giveArgs   []string
		wantErrMsg string
		wantPrefix string
	}{
		{giveUse: UseProgress, giveArgs: withNamespace, wantErrMsg: errMsgJobIDRequired},
		{
			giveUse: UseAbort, giveArgs: withNamespace, wantErrMsg: errMsgJobIDRequired,
			wantPrefix: "failed to initialize restore abort: ",
		},
		// The job id of the open is optional, so only a missing namespace is refused.
		{giveUse: UseOpen, wantErrMsg: errMsgNamespaceRequired, wantPrefix: "failed to initialize restore open: "},
		{
			giveUse: UseClearStorage, giveArgs: withNamespace, wantErrMsg: errMsgJobIDRequired,
			wantPrefix: "failed to initialize restore clear storage: ",
		},
	}

	for _, tt := range tests {
		t.Run(tt.giveUse, func(t *testing.T) {
			t.Parallel()

			cmd := newTestRestoreCmd()
			cmd.SetArgs(append([]string{tt.giveUse}, tt.giveArgs...))

			err := cmd.ExecuteContext(t.Context())

			require.ErrorContains(t, err, tt.wantErrMsg)

			if tt.wantPrefix == "" {
				assert.NotContains(t, err.Error(), errMsgInitPrefix)
				return
			}

			assert.Contains(t, err.Error(), tt.wantPrefix)
		})
	}
}

// TestNewRestoreCmdPolicySettings pins down that a setting of one policy given on the
// command line reaches the validation of "start" through the command flags, even at its
// default value, and is refused with the other policy before the cluster is reached.
func TestNewRestoreCmdPolicySettings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		giveArgs   []string
		wantErrMsg string
	}{
		{
			name:       "strict setting with the fuzzy policy",
			giveArgs:   []string{"--policy", "fuzzy", "--hydrate-replica"},
			wantErrMsg: "hydrate-replica requires policy strict",
		},
		{
			name:       "fuzzy setting at its default without a policy",
			giveArgs:   []string{"--max-inflight", "200"},
			wantErrMsg: "max-inflight requires policy fuzzy",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cmd := newTestRestoreCmd()
			cmd.SetArgs(append([]string{
				UseStart,
				"--" + flagNamespace, testNamespace,
				"--backup-ids", "261025T120000-ab12",
			}, tt.giveArgs...))

			require.ErrorContains(t, cmd.ExecuteContext(t.Context()), tt.wantErrMsg)
		})
	}
}

// TestNewRestoreCmdNoPrepare makes sure the prepare subcommand is gone: a cold restore is
// started by "start" alone.
func TestNewRestoreCmdNoPrepare(t *testing.T) {
	t.Parallel()

	for _, sub := range newTestRestoreCmd().Commands() {
		assert.NotEqual(t, "prepare", sub.Name())
	}
}

// TestNewRestoreCmdStartNoJobID makes sure "start" takes no job id: it is generated when
// the restore starts, as it is for a backup.
func TestNewRestoreCmdStartNoJobID(t *testing.T) {
	t.Parallel()

	sub, _, err := newTestRestoreCmd().Find([]string{UseStart})
	require.NoError(t, err)

	assert.Nil(t, sub.Flags().Lookup(flagJobID))
}

// TestNewRestoreCmdStrayArgument makes sure a flag value given apart from a bool flag,
// which the flag does not take, fails the command instead of being dropped: the flag
// would otherwise be sent as true.
func TestNewRestoreCmdStrayArgument(t *testing.T) {
	t.Parallel()

	cmd := newTestRestoreCmd()
	cmd.SetArgs([]string{
		UseStart,
		"--" + flagNamespace, testNamespace,
		"--backup-ids", "261025T120000-ab12",
		"--hydrate-replica", "false",
	})

	require.ErrorContains(t, cmd.ExecuteContext(t.Context()), "unknown command \"false\"")
}
