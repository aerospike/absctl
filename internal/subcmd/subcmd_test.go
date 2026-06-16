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

package subcmd

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/aerospike/absctl/internal/flags"
	"github.com/aerospike/absctl/internal/models"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"
)

const (
	testCmdName       = "fake"
	testCmdShort      = "fake short"
	testCmdLong       = "fake long description"
	testAppVersion    = "v0.0.0-test"
	testCommitHash    = "deadbeef"
	testBuildTime     = "2026-01-01T00:00:00Z"
	testRunnerFlag    = "fake-runner-flag"
	testRunnerFlagVal = "fake"
)

// fakeServiceConfig is a minimal ServiceConfig implementation used in tests.
type fakeServiceConfig struct {
	app         *models.App
	validateErr error
}

func (f *fakeServiceConfig) GetApp() *models.App { return f.app }
func (f *fakeServiceConfig) Validate() error     { return f.validateErr }

// fakeRunner is a configurable Runner implementation used to drive tests.
type fakeRunner struct {
	flagSets        []*pflag.FlagSet
	postRegCalled   bool
	helpUsageCalled bool

	cfg       ServiceConfig
	cfgErr    error
	runErr    error
	runCalled bool
}

func newFakeRunner() *fakeRunner {
	fs := pflag.NewFlagSet("runner", pflag.ContinueOnError)
	fs.String(testRunnerFlag, "", "test only")

	return &fakeRunner{
		flagSets: []*pflag.FlagSet{fs},
	}
}

func (f *fakeRunner) FlagSets() []*pflag.FlagSet { return f.flagSets }

func (f *fakeRunner) PostRegistration(_ *cobra.Command) {
	f.postRegCalled = true
}

func (f *fakeRunner) SetHelpUsage(_ *cobra.Command, _ *SharedFlagSets) {
	f.helpUsageCalled = true
}

func (f *fakeRunner) NewServiceConfig(_ context.Context, _ *SharedFlags) (ServiceConfig, error) {
	return f.cfg, f.cfgErr
}

func (f *fakeRunner) RunService(_ context.Context, _ ServiceConfig, _ *slog.Logger) error {
	f.runCalled = true

	return f.runErr
}

// validAppCfg returns a ServiceConfig with a log level that NewLogger accepts.
func validAppCfg() *fakeServiceConfig {
	return &fakeServiceConfig{
		app: &models.App{LogLevel: "info"},
	}
}

func newCmdWithBuffer() *bytes.Buffer {
	return &bytes.Buffer{}
}

func TestBuildCommand_Structure(t *testing.T) {
	t.Parallel()

	r := newFakeRunner()
	cmd, shared := BuildCommand(
		testCmdName, testCmdShort, testCmdLong,
		flags.NewRoot(), testAppVersion, testCommitHash, testBuildTime,
		flags.OperationBackup, r,
	)

	require.NotNil(t, cmd)
	require.NotNil(t, shared)

	require.Equal(t, testCmdName, cmd.Use)
	require.Equal(t, testCmdShort, cmd.Short)
	require.Equal(t, testCmdLong, cmd.Long)
	require.True(t, cmd.SilenceUsage)
	require.False(t, cmd.PersistentFlags().SortFlags)

	require.NotNil(t, cmd.RunE)
	require.NotNil(t, cmd.PersistentPreRunE)

	require.True(t, r.postRegCalled, "runner.PostRegistration must be called")
	require.True(t, r.helpUsageCalled, "runner.SetHelpUsage must be called")
}

func TestBuildCommand_SharedFlagsAllPopulated(t *testing.T) {
	t.Parallel()

	r := newFakeRunner()
	_, shared := BuildCommand(
		testCmdName, testCmdShort, testCmdLong,
		flags.NewRoot(), testAppVersion, testCommitHash, testBuildTime,
		flags.OperationBackup, r,
	)

	require.NotNil(t, shared.Root)
	require.NotNil(t, shared.App)
	require.NotNil(t, shared.Aerospike)
	require.NotNil(t, shared.ClientPolicy)
	require.NotNil(t, shared.Compression)
	require.NotNil(t, shared.Encryption)
	require.NotNil(t, shared.SecretAgent)
	require.NotNil(t, shared.Aws)
	require.NotNil(t, shared.Gcp)
	require.NotNil(t, shared.Azure)
}

func TestBuildCommand_RegistersSharedPersistentFlags(t *testing.T) {
	t.Parallel()

	r := newFakeRunner()
	cmd, _ := BuildCommand(
		testCmdName, testCmdShort, testCmdLong,
		flags.NewRoot(), testAppVersion, testCommitHash, testBuildTime,
		flags.OperationBackup, r,
	)

	// A representative subset of flags coming from each shared group.
	expected := []string{
		"verbose",   // App
		"log-level", // App
		"log-json",  // App
		"log-file",  // App
		"config",    // App
		"host",      // Aerospike
	}

	for _, name := range expected {
		require.NotNilf(t, cmd.PersistentFlags().Lookup(name),
			"expected persistent flag --%s", name)
	}
}

func TestBuildCommand_RegistersRunnerFlagSets(t *testing.T) {
	t.Parallel()

	r := newFakeRunner()
	cmd, _ := BuildCommand(
		testCmdName, testCmdShort, testCmdLong,
		flags.NewRoot(), testAppVersion, testCommitHash, testBuildTime,
		flags.OperationBackup, r,
	)

	// Runner-specific flags are registered as regular Flags, not PersistentFlags.
	require.NotNil(t, cmd.Flags().Lookup(testRunnerFlag),
		"expected runner flag --%s on cmd.Flags()", testRunnerFlag)
	require.Nil(t, cmd.PersistentFlags().Lookup(testRunnerFlag),
		"runner flags must not be promoted to PersistentFlags")
}

func TestNewSharedFlagSets_AllNonNil(t *testing.T) {
	t.Parallel()

	r := newFakeRunner()
	_, shared := BuildCommand(
		testCmdName, testCmdShort, testCmdLong,
		flags.NewRoot(), testAppVersion, testCommitHash, testBuildTime,
		flags.OperationBackup, r,
	)

	sets := NewSharedFlagSets(shared)

	require.NotNil(t, sets.App)
	require.NotNil(t, sets.Aerospike)
	require.NotNil(t, sets.ClientPolicy)
	require.NotNil(t, sets.Compression)
	require.NotNil(t, sets.Encryption)
	require.NotNil(t, sets.SecretAgent)
	require.NotNil(t, sets.Aws)
	require.NotNil(t, sets.Gcp)
	require.NotNil(t, sets.Azure)
}

func TestRunCommand_NoFlagsShowsHelpAndSkipsRunner(t *testing.T) {
	t.Parallel()

	r := newFakeRunner()
	cmd, shared := BuildCommand(
		testCmdName, testCmdShort, testCmdLong,
		flags.NewRoot(), testAppVersion, testCommitHash, testBuildTime,
		flags.OperationBackup, r,
	)

	require.Equal(t, 0, cmd.Flags().NFlag(), "no flags should be set on a fresh command")

	buf := newCmdWithBuffer()
	cmd.SetOut(buf)
	cmd.SetErr(buf)

	require.NoError(t, RunCommand(cmd, r, shared, testAppVersion, testCommitHash, testBuildTime))

	require.False(t, r.runCalled, "RunService must not be called when no flags are provided")
}

func TestRunCommand_NewServiceConfigErrorWrapped(t *testing.T) {
	t.Parallel()

	r := newFakeRunner()
	r.cfgErr = errors.New("bad config")

	cmd, shared := BuildCommand(
		testCmdName, testCmdShort, testCmdLong,
		flags.NewRoot(), testAppVersion, testCommitHash, testBuildTime,
		flags.OperationBackup, r,
	)

	// Ensure NFlag() > 0 so we bypass the no-flags branch.
	require.NoError(t, cmd.Flags().Set(testRunnerFlag, testRunnerFlagVal))

	err := RunCommand(cmd, r, shared, testAppVersion, testCommitHash, testBuildTime)

	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to initialize app")
	require.ErrorIs(t, err, r.cfgErr)
	require.False(t, r.runCalled)
}

func TestRunCommand_ValidateErrorWrapped(t *testing.T) {
	t.Parallel()

	r := newFakeRunner()
	r.cfg = &fakeServiceConfig{
		app:         &models.App{LogLevel: "info"},
		validateErr: errors.New("bad cfg fields"),
	}

	cmd, shared := BuildCommand(
		testCmdName, testCmdShort, testCmdLong,
		flags.NewRoot(), testAppVersion, testCommitHash, testBuildTime,
		flags.OperationBackup, r,
	)

	require.NoError(t, cmd.Flags().Set(testRunnerFlag, testRunnerFlagVal))

	err := RunCommand(cmd, r, shared, testAppVersion, testCommitHash, testBuildTime)

	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to validate config")
	require.False(t, r.runCalled)
}

func TestRunCommand_LoggerInitErrorWrapped(t *testing.T) {
	t.Parallel()

	r := newFakeRunner()
	r.cfg = &fakeServiceConfig{
		app: &models.App{LogLevel: "not-a-real-level"},
	}

	cmd, shared := BuildCommand(
		testCmdName, testCmdShort, testCmdLong,
		flags.NewRoot(), testAppVersion, testCommitHash, testBuildTime,
		flags.OperationBackup, r,
	)

	require.NoError(t, cmd.Flags().Set(testRunnerFlag, testRunnerFlagVal))

	err := RunCommand(cmd, r, shared, testAppVersion, testCommitHash, testBuildTime)

	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to initialize logger")
	require.False(t, r.runCalled, "RunService must not be called when logger init fails")
}

func TestRunCommand_RunServiceErrorPropagated(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("service blew up")

	r := newFakeRunner()
	r.cfg = validAppCfg()
	r.runErr = wantErr

	cmd, shared := BuildCommand(
		testCmdName, testCmdShort, testCmdLong,
		flags.NewRoot(), testAppVersion, testCommitHash, testBuildTime,
		flags.OperationBackup, r,
	)

	require.NoError(t, cmd.Flags().Set(testRunnerFlag, testRunnerFlagVal))

	err := RunCommand(cmd, r, shared, testAppVersion, testCommitHash, testBuildTime)

	require.Error(t, err)
	require.ErrorIs(t, err, wantErr)
	require.True(t, r.runCalled, "RunService must be called on the happy path")
}

func TestRunCommand_RunServiceSuccess(t *testing.T) {
	t.Parallel()

	r := newFakeRunner()
	r.cfg = validAppCfg()

	cmd, shared := BuildCommand(
		testCmdName, testCmdShort, testCmdLong,
		flags.NewRoot(), testAppVersion, testCommitHash, testBuildTime,
		flags.OperationBackup, r,
	)

	require.NoError(t, cmd.Flags().Set(testRunnerFlag, testRunnerFlagVal))

	err := RunCommand(cmd, r, shared, testAppVersion, testCommitHash, testBuildTime)

	require.NoError(t, err)
	require.True(t, r.runCalled, "RunService must be called on the happy path")
}

func TestBuildCommand_PersistentPreRunE_NoSecrets(t *testing.T) {
	t.Parallel()

	r := newFakeRunner()
	cmd, _ := BuildCommand(
		testCmdName, testCmdShort, testCmdLong,
		flags.NewRoot(), testAppVersion, testCommitHash, testBuildTime,
		flags.OperationBackup, r,
	)

	require.NotNil(t, cmd.PersistentPreRunE)

	// With no secret-prefixed flag values, PreRun must succeed.
	require.NoError(t, cmd.PersistentPreRunE(cmd, nil))
}
