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
	"os"
	"path/filepath"
	"testing"

	"github.com/aerospike/absctl/internal/config"
	"github.com/aerospike/absctl/internal/flags"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testGroupName = "group"
	testMistyped  = "begin"
	testFlagValue = "x"

	// testBackupTreeYAML holds a section for every snapshot-backup subcommand.
	testBackupTreeYAML = `
backup:
  namespace: test
list:
  path: some/prefix
validate:
  backup-id: 261025T120000-ab12
progress:
  backup-id: 261025T120000-ab12
abort:
  backup-id: 261025T120000-ab12
`

	// testRestoreTreeYAML holds a section for every snapshot-restore subcommand.
	testRestoreTreeYAML = `
restore:
  namespace: test
  backup-ids:
    - 261025T120000-ab12
progress:
  namespace: test
  job-id: rst-1
abort:
  namespace: test
  job-id: rst-1
open:
  namespace: test
  job-id: rst-1
clear-storage:
  namespace: test
  job-id: rst-1
`
)

var argConfig = "--" + flags.FlagConfig

func newGroupTestCmd(t *testing.T) *cobra.Command {
	t.Helper()

	cmd := &cobra.Command{Use: testGroupName}
	cmd.Flags().String(flagNamespace, "", "")
	cmd.Flags().String(flags.FlagConfig, "", "")

	return cmd
}

func TestGroupArgs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		give    []string
		wantErr string
	}{
		{name: "no args"},
		{name: "lone help", give: []string{argHelp}},
		{name: "unknown subcommand", give: []string{testMistyped}, wantErr: "unknown command"},
		{
			name:    "help with subcommand",
			give:    []string{argHelp, UseStart},
			wantErr: `use "` + testGroupName + " " + UseStart + ` --help"`,
		},
		{
			name:    "help with several arguments",
			give:    []string{argHelp, UseStart, testFlagValue},
			wantErr: `use "` + testGroupName + " " + UseStart + " " + testFlagValue + ` --help"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := groupArgs(newGroupTestCmd(t), tt.give)
			if tt.wantErr == "" {
				require.NoError(t, err)

				return
			}

			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

func TestRejectFlagsWithoutSubcommand(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		give    []string
		wantErr string
	}{
		{name: "no flags"},
		{name: "one flag", give: []string{"--" + flagNamespace, testFlagValue}, wantErr: "--" + flagNamespace},
		{name: "config flag", give: []string{argConfig, testFlagValue}, wantErr: argConfig},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cmd := newGroupTestCmd(t)
			require.NoError(t, cmd.ParseFlags(tt.give))

			err := rejectFlagsWithoutSubcommand(cmd)
			if tt.wantErr == "" {
				require.NoError(t, err)

				return
			}

			require.ErrorIs(t, err, ErrFlagsWithoutSubcommand)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

// loadSection runs the PersistentPreRunE of group for its subcommand name with --config
// set to path, so the config file is decoded for that subcommand without running it.
func loadSection(t *testing.T, group *cobra.Command, name, path string) {
	t.Helper()

	leaf, _, err := group.Find([]string{name})
	require.NoError(t, err)
	require.Equal(t, name, leaf.Name())

	// cobra sets the context before it runs the hooks.
	leaf.SetContext(t.Context())
	require.NoError(t, leaf.ParseFlags([]string{argConfig, path}))
	require.NoError(t, group.PersistentPreRunE(leaf, nil))
	require.NoError(t, group.PersistentPostRunE(leaf, nil))
}

func writeConfig(t *testing.T, content string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))

	return path
}

func newTestRunCtx() *runCtx {
	return newRunCtx(flags.NewRoot(), testAppVersion, testCommitHash, testBuildTime)
}

// subcommandNames returns the names of the subcommands group registers, so the tests
// below also cover a subcommand added later.
func subcommandNames(group *cobra.Command) []string {
	names := make([]string, 0, len(group.Commands()))
	for _, sub := range group.Commands() {
		names = append(names, sub.Name())
	}

	return names
}

// TestBackupSubcommandSections makes sure every snapshot-backup subcommand decodes its
// own section of the config file and only that one.
func TestBackupSubcommandSections(t *testing.T) {
	t.Parallel()

	path := writeConfig(t, testBackupTreeYAML)

	sections := func(cfg *config.ServerBackupServiceConfig) map[string]bool {
		return map[string]bool{
			UseStart:    cfg.Start != nil,
			UseList:     cfg.List != nil,
			UseValidate: cfg.Validation != nil,
			UseProgress: cfg.Progress != nil,
			UseAbort:    cfg.Abort != nil,
		}
	}

	for _, name := range subcommandNames(newBackupCmd(newTestRunCtx(), newBackupCtx())) {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			bf := newBackupCtx()
			group := newBackupCmd(newTestRunCtx(), bf)

			loadSection(t, group, name, path)

			require.NotNil(t, bf.fileCfg)

			got := sections(bf.fileCfg)
			require.Containsf(t, got, name, "no config section known for subcommand %s", name)

			for section, set := range got {
				assert.Equalf(t, section == name, set, "section %s", section)
			}
		})
	}
}

// TestRestoreSubcommandSections makes sure every snapshot-restore subcommand decodes its
// own section of the config file and only that one.
func TestRestoreSubcommandSections(t *testing.T) {
	t.Parallel()

	path := writeConfig(t, testRestoreTreeYAML)

	sections := func(cfg *config.ServerRestoreServiceConfig) map[string]bool {
		return map[string]bool{
			UseStart:        cfg.Start != nil,
			UseProgress:     cfg.Progress != nil,
			UseAbort:        cfg.Abort != nil,
			UseOpen:         cfg.Open != nil,
			UseClearStorage: cfg.ClearStorage != nil,
		}
	}

	for _, name := range subcommandNames(newRestoreCmd(newTestRunCtx(), newRestoreCtx())) {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			rf := newRestoreCtx()
			group := newRestoreCmd(newTestRunCtx(), rf)

			loadSection(t, group, name, path)

			require.NotNil(t, rf.fileCfg)

			got := sections(rf.fileCfg)
			require.Containsf(t, got, name, "no config section known for subcommand %s", name)

			for section, set := range got {
				assert.Equalf(t, section == name, set, "section %s", section)
			}
		})
	}
}

// TestSubcommandConfigNotLoaded makes sure every subcommand run with --config fails,
// rather than falling back to its flags, when the config file was not decoded for it.
func TestSubcommandConfigNotLoaded(t *testing.T) {
	t.Parallel()

	groups := map[string]func() *cobra.Command{
		UseSnapshotBackup: func() *cobra.Command {
			return newBackupCmd(newTestRunCtx(), newBackupCtx())
		},
		UseSnapshotRestore: func() *cobra.Command {
			return newRestoreCmd(newTestRunCtx(), newRestoreCtx())
		},
	}

	for groupName, newGroup := range groups {
		for _, name := range subcommandNames(newGroup()) {
			t.Run(groupName+"/"+name, func(t *testing.T) {
				t.Parallel()

				leaf, _, err := newGroup().Find([]string{name})
				require.NoError(t, err)

				leaf.SetContext(t.Context())
				require.NoError(t, leaf.ParseFlags([]string{argConfig, testFlagValue}))

				// RunE without the PersistentPreRunE of the group, which decodes the file.
				err = leaf.RunE(leaf, nil)
				require.ErrorIs(t, err, errConfigNotLoaded)
			})
		}
	}
}
