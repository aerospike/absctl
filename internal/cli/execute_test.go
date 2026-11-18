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
	"os"
	"path/filepath"
	"testing"

	"github.com/aerospike/absctl/internal/cli/server"
	"github.com/aerospike/absctl/internal/flags"
	"github.com/aerospike/absctl/internal/subcmd"
	"github.com/aerospike/absctl/internal/testutil"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	argBackup          = "backup"
	argRestore         = "restore"
	argSnapshotBackup  = "snapshot-backup"
	argSnapshotRestore = "snapshot-restore"
	argStart           = "start"
	argAbort           = "abort"
	argOpen            = "open"
	argHelp            = "help"
	argMistyped        = "begin"
	argNamespace       = "--namespace"
	argNamespaceShort  = "-n"
	argNamespaceValue  = "test"
	argDirShort        = "-d"
	argLogFile         = "--log-file"
	argLogLevel        = "--log-level"
	argEncrypt         = "--encrypt"
	argFalse           = "false"

	errUnknownCommand = "unknown command"
	// errClientCreate proves that the service was built from a valid config and
	// tried to reach the cluster, which is not running.
	errClientCreate = "failed to create aerospike client"

	// versionLine is the whole version line, so a dropped or swapped field fails.
	versionLine = "version: " + testAppVersion + " (" + testCommitHash + ") " + testBuildTime

	// deadSeed is a cluster seed nothing listens on, so a client fails at once.
	deadSeed            = "cluster:\n  seeds:\n    - host: 127.0.0.1\n      port: 1\n"
	backupAbortSection  = "abort:\n  backup-id: 261025T120000-ab12\n"
	restoreAbortSection = "abort:\n  namespace: test\n  job-id: rst-1\n"
	badLogLevel         = "app:\n  log-level: nope\n"
)

var (
	argConfig      = "--" + flags.FlagConfig
	argConfigEmpty = argConfig + "="
	argVersion     = "--" + flags.FlagVersion
	argHelpFlg     = "--" + flags.FlagHelp

	// argsSecretAgent makes any secret resolution try a dead Secret Agent, so a run
	// that resolves secrets fails instead of printing the version.
	argsSecretAgent = []string{"-P", "secrets:resource:key", "--sa-address", "127.0.0.1", "--sa-port", "1"}
)

// newTestRootCmd builds the whole command tree and silences the errors of cobra, as main
// does. The error that main logs itself is returned by the run instead.
func newTestRootCmd() *cobra.Command {
	rootCmd, _ := NewCmd(testAppVersion, testCommitHash, testBuildTime)
	rootCmd.SilenceErrors = true

	return rootCmd
}

// executeEnv holds the paths a test case may refer to.
type executeEnv struct {
	// backupDir is an existing backup directory that holds sentinel.
	backupDir string
	sentinel  string
	// config is the config file written from executeCase.giveConfig; it does not
	// exist when giveConfig is empty.
	config string
	// missingDirLog is a log file path in a directory that does not exist.
	missingDirLog string
}

func newExecuteEnv(t *testing.T, config string) executeEnv {
	t.Helper()

	root := t.TempDir()

	env := executeEnv{
		backupDir:     filepath.Join(root, "backup"),
		config:        filepath.Join(root, "config.yaml"),
		missingDirLog: filepath.Join(root, "missing", "absctl.log"),
	}
	env.sentinel = filepath.Join(env.backupDir, "keep.asb")

	require.NoError(t, os.MkdirAll(env.backupDir, 0o755))
	require.NoError(t, os.WriteFile(env.sentinel, []byte("data"), 0o600))

	if config != "" {
		require.NoError(t, os.WriteFile(env.config, []byte(config), 0o600))
	}

	return env
}

type executeCase struct {
	name string
	// giveConfig is written to env.config before the run.
	giveConfig string
	giveArgs   func(env executeEnv) []string
	// wantErrIs and wantErr describe the expected error; both empty means success.
	wantErrIs error
	wantErr   string
	// wantOut must appear on stdout.
	wantOut string
}

// snapshotGroup describes a snapshot command group for groupCases.
type snapshotGroup struct {
	name  string
	usage string
	// clusterSubcommands connect to the cluster and so have Secret Agent flags;
	// storageSubcommands only read object storage and have none.
	clusterSubcommands []string
	storageSubcommands []string
	// abortSection is a valid config section for the abort subcommand.
	abortSection string
}

// groupCases returns the cases that apply to both snapshot command groups.
func groupCases(g *snapshotGroup) []executeCase {
	group := g.name

	cases := []executeCase{
		{
			name:     group + ": mistyped subcommand",
			giveArgs: func(executeEnv) []string { return []string{group, argMistyped} },
			wantErr:  errUnknownCommand + ` "` + argMistyped + `"`,
		},
		{
			name:     group + ": help argument prints help",
			giveArgs: func(executeEnv) []string { return []string{group, argHelp} },
			wantOut:  g.usage,
		},
		{
			name:     group + ": no arguments prints help",
			giveArgs: func(executeEnv) []string { return []string{group} },
			wantOut:  g.usage,
		},
		{
			name:     group + ": help flag prints help",
			giveArgs: func(executeEnv) []string { return []string{group, argHelpFlg} },
			wantOut:  g.usage,
		},
		{
			name:     group + ": help with subcommand points to subcommand help",
			giveArgs: func(executeEnv) []string { return []string{group, argHelp, argStart} },
			wantErr:  group + " " + argStart + " " + argHelpFlg,
		},
		{
			name:      group + ": flag without subcommand",
			giveArgs:  func(executeEnv) []string { return []string{group, argLogLevel, "debug"} },
			wantErrIs: server.ErrFlagsWithoutSubcommand,
			wantErr:   argLogLevel,
		},
		{
			name:       group + ": config without subcommand is not loaded",
			giveConfig: deadSeed + g.abortSection,
			giveArgs:   func(env executeEnv) []string { return []string{group, argConfig, env.config} },
			wantErrIs:  server.ErrFlagsWithoutSubcommand,
			wantErr:    argConfig,
		},
		{
			name:      group + ": log file without subcommand is not opened",
			giveArgs:  func(env executeEnv) []string { return []string{group, argLogFile, env.missingDirLog} },
			wantErrIs: server.ErrFlagsWithoutSubcommand,
			wantErr:   argLogFile,
		},
		{
			name:      group + ": empty config without subcommand",
			giveArgs:  func(executeEnv) []string { return []string{group, argConfigEmpty} },
			wantErrIs: subcmd.ErrEmptyConfigPath,
		},
		{
			name:     group + ": version on group",
			giveArgs: func(executeEnv) []string { return []string{group, argVersion} },
			wantOut:  versionLine,
		},
		{
			name:     group + ": version wins over empty config",
			giveArgs: func(executeEnv) []string { return []string{group, argStart, argConfigEmpty, argVersion} },
			wantOut:  versionLine,
		},
		{
			name: group + ": flag combined with config",
			giveArgs: func(env executeEnv) []string {
				return []string{group, argStart, argConfig, env.config, argNamespace, argNamespaceValue}
			},
			wantErrIs: subcmd.ErrFlagsWithConfig,
			wantErr:   argNamespace,
		},
		{
			name:       group + ": config section reaches the service",
			giveConfig: deadSeed + g.abortSection,
			giveArgs:   func(env executeEnv) []string { return []string{group, argAbort, argConfig, env.config} },
			wantErr:    errClientCreate,
		},
		{
			name:       group + ": logger uses the app section of the config",
			giveConfig: badLogLevel + deadSeed + g.abortSection,
			giveArgs:   func(env executeEnv) []string { return []string{group, argAbort, argConfig, env.config} },
			wantErr:    "invalid log level",
		},
	}

	for _, sub := range g.clusterSubcommands {
		cases = append(cases, executeCase{
			name: group + " " + sub + ": version resolves no secrets and runs nothing",
			giveArgs: func(executeEnv) []string {
				return append([]string{group, sub, argVersion}, argsSecretAgent...)
			},
			wantOut: versionLine,
		})
	}

	for _, sub := range g.storageSubcommands {
		cases = append(cases, executeCase{
			name:     group + " " + sub + ": version runs nothing",
			giveArgs: func(executeEnv) []string { return []string{group, sub, argVersion} },
			wantOut:  versionLine,
		})
	}

	return cases
}

// TestExecute_RejectsIgnoredInput runs the real command tree and checks that input the
// CLI would not act on fails instead of being silently dropped. No case needs a
// running cluster or Secret Agent: the ones that reach the network expect it to fail.
//
// The test is not parallel: testutil.RunCLI captures the process-wide standard streams.
func TestExecute_RejectsIgnoredInput(t *testing.T) {
	tests := []executeCase{
		{
			name: "stray positional argument after bool flag",
			giveArgs: func(env executeEnv) []string {
				return []string{
					argBackup, argNamespaceShort, argNamespaceValue, argDirShort, env.backupDir, "--remove-artifacts", argFalse,
				}
			},
			wantErr: errUnknownCommand + ` "` + argFalse + `"`,
		},
		{
			name:     "help argument of scan backup",
			giveArgs: func(executeEnv) []string { return []string{argBackup, argHelp} },
			wantErr:  errUnknownCommand + ` "` + argHelp + `"`,
		},
		{
			name:     "positional argument of scan restore",
			giveArgs: func(executeEnv) []string { return []string{argRestore, argMistyped} },
			wantErr:  errUnknownCommand + ` "` + argMistyped + `"`,
		},
		{
			name: "scan flag combined with config",
			giveArgs: func(env executeEnv) []string {
				return []string{argBackup, argConfig, env.config, argEncrypt, "AES256"}
			},
			wantErrIs: subcmd.ErrFlagsWithConfig,
			wantErr:   argEncrypt,
		},
		{
			name: "empty config with flags",
			giveArgs: func(env executeEnv) []string {
				return []string{argBackup, argConfigEmpty, argNamespaceShort, argNamespaceValue, argDirShort, env.backupDir}
			},
			wantErrIs: subcmd.ErrEmptyConfigPath,
		},
		{
			name:      "empty config alone",
			giveArgs:  func(executeEnv) []string { return []string{argSnapshotRestore, argAbort, argConfigEmpty} },
			wantErrIs: subcmd.ErrEmptyConfigPath,
		},
		{
			name:     "version wins over empty config",
			giveArgs: func(executeEnv) []string { return []string{argBackup, argConfigEmpty, argVersion} },
			wantOut:  versionLine,
		},
		{
			name:     "version combined with config",
			giveArgs: func(env executeEnv) []string { return []string{argBackup, argConfig, env.config, "-V"} },
			wantOut:  versionLine,
		},
		{
			name: "version on scan backup resolves no secrets",
			giveArgs: func(executeEnv) []string {
				return append([]string{argBackup, argVersion}, argsSecretAgent...)
			},
			wantOut: versionLine,
		},
		{
			name:       "snapshot subcommand reads only its own section",
			giveConfig: deadSeed + restoreAbortSection,
			giveArgs: func(env executeEnv) []string {
				return []string{argSnapshotRestore, argOpen, argConfig, env.config}
			},
			wantErr: "namespace is required",
		},
		{
			name:       "snapshot config is decoded strictly",
			giveConfig: "bogus: 1\n",
			giveArgs: func(env executeEnv) []string {
				return []string{argSnapshotBackup, argAbort, argConfig, env.config}
			},
			wantErr: "field bogus not found",
		},
	}

	tests = append(tests, groupCases(&snapshotGroup{
		name:               argSnapshotBackup,
		usage:              flags.SectionTextUsageBackupServer,
		clusterSubcommands: []string{server.UseStart, server.UseProgress, server.UseAbort},
		storageSubcommands: []string{server.UseList, server.UseValidate},
		abortSection:       backupAbortSection,
	})...)
	tests = append(tests, groupCases(&snapshotGroup{
		name:  argSnapshotRestore,
		usage: flags.SectionTextUsageRestoreServer,
		clusterSubcommands: []string{
			server.UseStart, server.UseProgress, server.UseAbort, server.UseOpen, server.UseClearStorage,
		},
		abortSection: restoreAbortSection,
	})...)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env := newExecuteEnv(t, tt.giveConfig)

			out, _, err := testutil.RunCLI(t, newTestRootCmd, tt.giveArgs(env)...)

			// No case may remove or change existing backup files.
			assert.FileExists(t, env.sentinel)
			assert.Contains(t, out, tt.wantOut)

			if tt.wantErr == "" && tt.wantErrIs == nil {
				require.NoError(t, err)

				return
			}

			require.Error(t, err)

			if tt.wantErrIs != nil {
				require.ErrorIs(t, err, tt.wantErrIs)
			}

			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}
