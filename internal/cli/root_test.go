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
	"bytes"
	"errors"
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

	// The pipe is drained while fn runs, so output larger than the pipe buffer
	// cannot block fn.
	var (
		buf     bytes.Buffer
		readErr error
	)

	done := make(chan struct{})

	go func() {
		defer close(done)

		_, readErr = io.Copy(&buf, r)
	}()

	restored := false
	restore := func() error {
		if restored {
			return nil
		}

		restored = true
		os.Stdout = orig
		closeErr := w.Close()
		<-done

		return errors.Join(closeErr, r.Close())
	}

	// Restores os.Stdout even when fn stops the test early, for example through a
	// failed require, so later tests in the package do not write into a dead pipe.
	defer func() {
		_ = restore()
	}()

	fn()

	require.NoError(t, restore())
	require.NoError(t, readErr)

	return buf.String()
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

func TestRun_VersionPrintsVersion(t *testing.T) {
	t.Parallel()

	rootCmd, c := NewCmd(testAppVersion, testCommitHash, testBuildTime)
	c.flagsRoot.Version = true

	out := captureStdout(t, func() {
		require.NoError(t, c.run(rootCmd, nil))
	})

	assert.Contains(t, out, versionLine)
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

	assert.Contains(t, out, versionLine)
}
