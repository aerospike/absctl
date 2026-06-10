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

package cli

import (
	"io"
	"os"
	"sync"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testAppVersion = "v0.0.0-test"
	testCommitHash = "deadbeef"
	testBuildTime  = "2026-01-01T00:00:00Z"
)

var captureStdoutMu sync.Mutex

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

	rootCmd, c := NewCmd(testAppVersion, testCommitHash, testBuildTime)

	require.NotNil(t, rootCmd)
	require.NotNil(t, c)
	assert.Equal(t, useMessage, rootCmd.Use)
	assert.Equal(t, shortMessage, rootCmd.Short)
	assert.Equal(t, welcomeMessage, rootCmd.Long)
	assert.True(t, rootCmd.SilenceUsage)
	assert.NotNil(t, rootCmd.RunE)

	assert.Equal(t, testAppVersion, c.appVersion)
	assert.Equal(t, testCommitHash, c.commitHash)
	assert.Equal(t, testBuildTime, c.buildTime)
	assert.NotNil(t, c.Logger)
}

func TestNewCmd_Subcommands(t *testing.T) {
	t.Parallel()

	rootCmd, _ := NewCmd(testAppVersion, testCommitHash, testBuildTime)

	assert.ElementsMatch(t,
		[]string{"backup", "restore"},
		subcommandNames(rootCmd),
	)
}

func TestNewCmd_PersistentFlags(t *testing.T) {
	t.Parallel()

	rootCmd, _ := NewCmd(testAppVersion, testCommitHash, testBuildTime)
	persistent := rootCmd.PersistentFlags()

	for _, name := range []string{"help", "version"} {
		assert.NotNilf(t, persistent.Lookup(name), "expected persistent flag --%s", name)
	}
}

func TestRun_VersionPrintsVersion(t *testing.T) {
	t.Parallel()

	rootCmd, c := NewCmd(testAppVersion, testCommitHash, testBuildTime)
	c.flagsRoot.Version = true

	out := captureStdout(t, func() {
		require.NoError(t, c.run(rootCmd, nil))
	})

	assert.Contains(t, out, testAppVersion)
	assert.Contains(t, out, testCommitHash)
	assert.Contains(t, out, testBuildTime)
}

func TestRun_NoVersionShowsHelp(t *testing.T) {
	t.Parallel()

	rootCmd, c := NewCmd(testAppVersion, testCommitHash, testBuildTime)
	c.flagsRoot.Version = false

	out := captureStdout(t, func() {
		require.NoError(t, c.run(rootCmd, nil))
	})

	assert.Contains(t, out, welcomeMessage)
}

func TestPrintVersion(t *testing.T) {
	t.Parallel()

	c := &Cmd{
		appVersion: testAppVersion,
		commitHash: testCommitHash,
		buildTime:  testBuildTime,
	}

	out := captureStdout(t, c.printVersion)

	assert.Contains(t, out, testAppVersion)
	assert.Contains(t, out, testCommitHash)
	assert.Contains(t, out, testBuildTime)
}

/*
func TestNewHelpFunction(t *testing.T) {
	t.Parallel()

	flagSet := flags.NewRoot().NewFlagSet()
	helpFn := newHelpFunction(flagSet)

	out := captureStdout(t, helpFn)

	assert.Contains(t, out, welcomeMessage)
	assert.Contains(t, out, strings.Repeat("-", len(welcomeMessage)))
	assert.Contains(t, out, "Available Commands:")
	assert.Contains(t, out, "backup")
	assert.Contains(t, out, "restore")
	assert.Contains(t, out, "server")
	assert.Contains(t, out, "service")
	assert.Contains(t, out, "Flags:")
}
*/
