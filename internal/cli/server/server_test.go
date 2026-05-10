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

package server

import (
	"io"
	"os"
	"sync"
	"testing"

	"github.com/aerospike/absctl/internal/flags"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testAppVersion = "v0.0.0-test"
	testCommitHash = "deadbeef"
	testBuildTime  = "2026-01-01T00:00:00Z"
)

var captureStdoutMu sync.Mutex

// captureStdout redirects os.Stdout for the duration of fn.
func captureStdout(t *testing.T, fn func()) string {
	t.Helper()

	captureStdoutMu.Lock()
	defer captureStdoutMu.Unlock()

	r, w, err := os.Pipe()
	require.NoError(t, err)

	orig := os.Stdout
	os.Stdout = w

	fn()

	require.NoError(t, w.Close())
	os.Stdout = orig

	out, err := io.ReadAll(r)
	require.NoError(t, err)

	return string(out)
}

func subcommandNames(cmd *cobra.Command) []string {
	names := make([]string, 0, len(cmd.Commands()))
	for _, sub := range cmd.Commands() {
		names = append(names, sub.Name())
	}

	return names
}

func TestNewCmd_Structure(t *testing.T) {
	t.Parallel()

	cmd := NewCmd(flags.NewRoot(), testAppVersion, testCommitHash, testBuildTime)

	require.NotNil(t, cmd)
	assert.Equal(t, "server", cmd.Use)
	assert.Equal(t, serverShort, cmd.Short)
	assert.Equal(t, serverLong, cmd.Long)
	assert.True(t, cmd.SilenceUsage)
	assert.NotNil(t, cmd.PersistentPreRunE)
	assert.NotNil(t, cmd.PersistentPostRunE)
}

func TestNewCmd_Subcommands(t *testing.T) {
	t.Parallel()

	cmd := NewCmd(flags.NewRoot(), testAppVersion, testCommitHash, testBuildTime)

	names := subcommandNames(cmd)
	assert.ElementsMatch(t, []string{"backup", "restore"}, names)
}

func TestNewCmd_BackupSubcommandHasStartAndList(t *testing.T) {
	t.Parallel()

	cmd := NewCmd(flags.NewRoot(), testAppVersion, testCommitHash, testBuildTime)

	var backup *cobra.Command

	for _, sub := range cmd.Commands() {
		if sub.Name() == "backup" {
			backup = sub
			break
		}
	}

	require.NotNil(t, backup, "expected a 'backup' subcommand")

	assert.ElementsMatch(t, []string{"start", "list"}, subcommandNames(backup))
}

func TestNewCmd_RestoreSubcommandHasStart(t *testing.T) {
	t.Parallel()

	cmd := NewCmd(flags.NewRoot(), testAppVersion, testCommitHash, testBuildTime)

	var restore *cobra.Command

	for _, sub := range cmd.Commands() {
		if sub.Name() == "restore" {
			restore = sub
			break
		}
	}

	require.NotNil(t, restore, "expected a 'restore' subcommand")

	assert.ElementsMatch(t, []string{"start", "prepare"}, subcommandNames(restore))
}

func TestNewCmd_PersistentFlags(t *testing.T) {
	t.Parallel()

	cmd := NewCmd(flags.NewRoot(), testAppVersion, testCommitHash, testBuildTime)

	persistent := cmd.PersistentFlags()

	for _, name := range []string{"verbose", "log-level", "log-json", "log-file"} {
		assert.NotNilf(t, persistent.Lookup(name), "expected persistent flag --%s on server cmd", name)
	}
}

func TestNewCmd_PersistentPostRunENilCloser(t *testing.T) {
	t.Parallel()

	cmd := NewCmd(flags.NewRoot(), testAppVersion, testCommitHash, testBuildTime)

	// PostRun is invoked without a preceding PreRun, so loggerClose is still nil
	// and the function must short-circuit without error.
	require.NoError(t, cmd.PersistentPostRunE(cmd, nil))
}

func TestSetParentHelp_PrintsAvailableCommands(t *testing.T) {
	t.Parallel()

	parent := &cobra.Command{Use: "parent", Short: "parent short"}
	child := &cobra.Command{
		Use:   "child",
		Short: "child short",
		RunE:  func(_ *cobra.Command, _ []string) error { return nil },
	}
	parent.AddCommand(child)

	flagSet := &pflag.FlagSet{}
	flagSet.String("example", "default", "example flag")

	setParentHelp(parent, flagSet)

	require.NotNil(t, parent.HelpFunc())
	require.NotNil(t, parent.UsageFunc())

	out := captureStdout(t, func() {
		parent.HelpFunc()(parent, nil)
	})

	assert.Contains(t, out, "parent short")
	assert.Contains(t, out, "Available Commands:")
	assert.Contains(t, out, "child")
	assert.Contains(t, out, "child short")
	assert.Contains(t, out, "Flags:")
	assert.Contains(t, out, "--example")
}

func TestSetParentHelp_UsageDelegatesToHelp(t *testing.T) {
	t.Parallel()

	parent := &cobra.Command{Use: "parent", Short: "parent short"}
	setParentHelp(parent)

	out := captureStdout(t, func() {
		require.NoError(t, parent.UsageFunc()(parent))
	})

	assert.Contains(t, out, "parent short")
}

func TestSetParentHelp_NoFlagSets(t *testing.T) {
	t.Parallel()

	parent := &cobra.Command{Use: "parent", Short: "parent short"}
	setParentHelp(parent)

	out := captureStdout(t, func() {
		parent.HelpFunc()(parent, nil)
	})

	assert.Contains(t, out, "parent short")
	assert.NotContains(t, out, "Flags:")
}

func TestSetLeafHelp_PrintsLocalAndInheritedFlags(t *testing.T) {
	t.Parallel()

	parent := &cobra.Command{Use: "parent"}
	parent.PersistentFlags().String("inherited", "", "inherited flag")

	leaf := &cobra.Command{Use: "leaf", Short: "leaf short"}
	leaf.Flags().String("local", "", "local flag")
	parent.AddCommand(leaf)

	setLeafHelp(leaf)

	out := captureStdout(t, func() {
		leaf.HelpFunc()(leaf, nil)
	})

	assert.Contains(t, out, "leaf short")
	assert.Contains(t, out, "--local")
	assert.Contains(t, out, "--inherited")
	assert.Contains(t, out, "Global Flags:")
}

func TestSetLeafHelp_NoFlagsSection(t *testing.T) {
	t.Parallel()

	leaf := &cobra.Command{Use: "leaf", Short: "leaf short"}
	setLeafHelp(leaf)

	out := captureStdout(t, func() {
		leaf.HelpFunc()(leaf, nil)
	})

	assert.Contains(t, out, "leaf short")
	assert.NotContains(t, out, "Flags:")
	assert.NotContains(t, out, "Global Flags:")
}
