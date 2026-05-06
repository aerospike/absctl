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

package scan

import (
	"io"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/aerospike/absctl/internal/flags"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testAppVersion  = "v0.0.0-test"
	testCommitHash  = "deadbeef"
	testBuildTime   = "2026-01-01T00:00:00Z"
	expectedNiceMsg = "use --bandwidth instead"
)

// captureStdoutMu serializes tests that swap os.Stdout to avoid races between
// parallel tests in the same package.
var captureStdoutMu sync.Mutex

// captureStdout redirects os.Stdout for the duration of fn and returns what was written.
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

func TestNewBackupCmd_Structure(t *testing.T) {
	t.Parallel()

	rootFlags := flags.NewRoot()
	cmd, shared := NewBackupCmd(rootFlags, testAppVersion, testCommitHash, testBuildTime)

	require.NotNil(t, cmd)
	require.NotNil(t, shared)
	assert.Equal(t, "backup", cmd.Use)
	assert.Equal(t, backupWelcomeMessageShort, cmd.Short)
	assert.Equal(t, backupWelcomeMessage, cmd.Long)
	assert.True(t, cmd.SilenceUsage)
	assert.NotNil(t, cmd.RunE)
}

func TestNewBackupCmd_HasExpectedFlags(t *testing.T) {
	t.Parallel()

	rootFlags := flags.NewRoot()
	cmd, _ := NewBackupCmd(rootFlags, testAppVersion, testCommitHash, testBuildTime)

	expectedFlags := []string{
		"namespace",
		"directory",
		"output-file",
		"compress",
		"encrypt",
		"parallel",
		"file-limit",
		"partition-list",
		"node-list",
		"rack-list",
		"estimate",
		"continue",
		"state-file-dst",
	}

	for _, name := range expectedFlags {
		assert.NotNilf(t, cmd.Flag(name), "expected backup command to define --%s", name)
	}
}

func TestNewBackupCmd_NiceFlagDeprecated(t *testing.T) {
	t.Parallel()

	rootFlags := flags.NewRoot()
	cmd, _ := NewBackupCmd(rootFlags, testAppVersion, testCommitHash, testBuildTime)

	nice := cmd.Flag("nice")
	require.NotNil(t, nice, "deprecated --nice flag must still be registered")
	assert.Equal(t, expectedNiceMsg, nice.Deprecated)
	assert.False(t, nice.Hidden, "deprecated --nice flag must remain visible in help")
}

func TestNewBackupCmd_RunnerImplementsRunner(t *testing.T) {
	t.Parallel()

	r := &backupRunner{
		flagsBackup: flags.NewBackup(),
		flagsLocal:  flags.NewLocal(flags.OperationBackup),
	}
	r.flagsCommon = flags.NewCommon(&r.flagsBackup.Common, flags.OperationBackup)

	sets := r.FlagSets()
	require.Len(t, sets, 3)
	for _, fs := range sets {
		assert.NotNil(t, fs)
	}
}

func TestNewBackupHelpFunction_Output(t *testing.T) {
	t.Parallel()

	rootFlags := flags.NewRoot()
	_, shared := NewBackupCmd(rootFlags, testAppVersion, testCommitHash, testBuildTime)

	appFS := shared.App.NewFlagSet()

	helpFn := newBackupHelpFunction(
		appFS, appFS, appFS,
		appFS, appFS, appFS, appFS, appFS,
		appFS, appFS, appFS, appFS,
	)

	out := captureStdout(t, helpFn)
	assert.Contains(t, out, backupWelcomeMessage)
	assert.Contains(t, out, strings.Repeat("-", len(backupWelcomeMessage)))
}
