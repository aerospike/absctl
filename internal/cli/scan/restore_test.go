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
	"strings"
	"testing"

	"github.com/aerospike/absctl/internal/flags"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewRestoreCmd_Structure(t *testing.T) {
	t.Parallel()

	rootFlags := flags.NewRoot()
	cmd, shared := NewRestoreCmd(rootFlags, testAppVersion, testCommitHash, testBuildTime)

	require.NotNil(t, cmd)
	require.NotNil(t, shared)
	assert.Equal(t, "restore", cmd.Use)
	assert.Equal(t, restoreWelcomeMessageShort, cmd.Short)
	assert.Equal(t, restoreWelcomeMessage, cmd.Long)
	assert.True(t, cmd.SilenceUsage)
	assert.NotNil(t, cmd.RunE)
}

func TestNewRestoreCmd_HasExpectedFlags(t *testing.T) {
	t.Parallel()

	rootFlags := flags.NewRoot()
	cmd, _ := NewRestoreCmd(rootFlags, testAppVersion, testCommitHash, testBuildTime)

	expectedFlags := []string{
		"namespace",
		"directory",
		"input-file",
		"compress",
		"encrypt",
		"parallel",
		"batch-size",
		"max-async-batches",
		"validate",
	}

	for _, name := range expectedFlags {
		assert.NotNilf(t, cmd.Flag(name), "expected restore command to define --%s", name)
	}
}

func TestNewRestoreCmd_NiceFlagDeprecated(t *testing.T) {
	t.Parallel()

	rootFlags := flags.NewRoot()
	cmd, _ := NewRestoreCmd(rootFlags, testAppVersion, testCommitHash, testBuildTime)

	nice := cmd.Flag("nice")
	require.NotNil(t, nice, "deprecated --nice flag must still be registered")
	assert.Equal(t, expectedNiceMsg, nice.Deprecated)
	assert.False(t, nice.Hidden, "deprecated --nice flag must remain visible in help")
}

func TestNewRestoreCmd_RunnerImplementsRunner(t *testing.T) {
	t.Parallel()

	r := &restoreRunner{
		flagsRestore: flags.NewRestore(),
	}
	r.flagsCommon = flags.NewCommon(&r.flagsRestore.Common, flags.OperationRestore)

	sets := r.FlagSets()
	require.Len(t, sets, 2)
	for _, fs := range sets {
		assert.NotNil(t, fs)
	}
}

func TestNewRestoreHelpFunction_Output(t *testing.T) {
	t.Parallel()

	rootFlags := flags.NewRoot()
	_, shared := NewRestoreCmd(rootFlags, testAppVersion, testCommitHash, testBuildTime)

	appFS := shared.App.NewFlagSet()

	helpFn := newRestoreHelpFunction(
		appFS, appFS, appFS,
		appFS, appFS, appFS, appFS, appFS,
		appFS, appFS, appFS,
	)

	out := captureStdout(t, helpFn)
	assert.Contains(t, out, restoreWelcomeMessage)
	assert.Contains(t, out, strings.Repeat("-", len(restoreWelcomeMessage)))
}
