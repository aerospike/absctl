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

package cli

import (
	"testing"

	"github.com/aerospike/absctl/internal/testutil"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testAppVersion = "v0.0.0-test"
	testCommitHash = "deadbeef"
	testBuildTime  = "2026-01-01T00:00:00Z"
)

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
		[]string{
			"backup", "restore",
			"snapshot-backup", "snapshot-restore",
		},
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

// The tests below are not parallel: they capture the process-wide standard streams,
// which testutil.CaptureOutput does not allow in a parallel test.

func TestRun_VersionPrintsVersion(t *testing.T) {
	rootCmd, c := NewCmd(testAppVersion, testCommitHash, testBuildTime)
	c.flagsRoot.Version = true

	out, _ := testutil.CaptureOutput(t, func() {
		require.NoError(t, c.run(rootCmd, nil))
	})

	assert.Contains(t, out, versionLine)
}

func TestRun_NoVersionShowsHelp(t *testing.T) {
	rootCmd, c := NewCmd(testAppVersion, testCommitHash, testBuildTime)
	c.flagsRoot.Version = false

	out, _ := testutil.CaptureOutput(t, func() {
		require.NoError(t, c.run(rootCmd, nil))
	})

	assert.Contains(t, out, welcomeMessage)
}

func TestPrintVersion(t *testing.T) {
	c := &Cmd{
		appVersion: testAppVersion,
		commitHash: testCommitHash,
		buildTime:  testBuildTime,
	}

	out, _ := testutil.CaptureOutput(t, c.printVersion)

	assert.Contains(t, out, versionLine)
}
