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

package service

import (
	"io"
	"os"
	"sync"
	"testing"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func findSubcommand(cmd *cobra.Command, name string) *cobra.Command {
	for _, sub := range cmd.Commands() {
		if sub.Name() == name {
			return sub
		}
	}

	return nil
}

func TestNewCmd_Structure(t *testing.T) {
	t.Parallel()

	cmd := NewCmd()
	require.NotNil(t, cmd)
	assert.Equal(t, useService, cmd.Use)
	assert.Equal(t, serviceShort, cmd.Short)
	assert.Equal(t, serviceLong, cmd.Long)
	assert.True(t, cmd.SilenceUsage)
	assert.NotNil(t, cmd.PersistentPreRunE)
	assert.NotNil(t, cmd.PersistentPostRunE)
}

func TestNewCmd_Subcommands(t *testing.T) {
	t.Parallel()

	cmd := NewCmd()
	assert.ElementsMatch(t, []string{useBackup, useRestore, useConfig}, subcommandNames(cmd))
}

func TestNewCmd_BackupSubcommandTree(t *testing.T) {
	t.Parallel()

	cmd := NewCmd()
	backup := findSubcommand(cmd, useBackup)
	require.NotNil(t, backup)

	assert.ElementsMatch(t,
		[]string{useCancel, useStatus, useFull, useIncremental},
		subcommandNames(backup),
	)

	full := findSubcommand(backup, useFull)
	require.NotNil(t, full)
	assert.ElementsMatch(t, []string{useList, useStart}, subcommandNames(full))

	incr := findSubcommand(backup, useIncremental)
	require.NotNil(t, incr)
	assert.ElementsMatch(t, []string{useList, useStart}, subcommandNames(incr))
}

func TestNewCmd_RestoreSubcommandTree(t *testing.T) {
	t.Parallel()

	cmd := NewCmd()
	restore := findSubcommand(cmd, useRestore)
	require.NotNil(t, restore)

	assert.ElementsMatch(t,
		[]string{useCancel, useStatus, useJobs, useFull, useIncremental, useTimestamp},
		subcommandNames(restore),
	)

	for _, name := range []string{useFull, useIncremental, useTimestamp} {
		sub := findSubcommand(restore, name)
		require.NotNilf(t, sub, "expected %q under restore", name)
		assert.ElementsMatch(t, []string{useStart}, subcommandNames(sub))
	}
}

func TestNewCmd_ConfigSubcommandTree(t *testing.T) {
	t.Parallel()

	cmd := NewCmd()
	cfgCmd := findSubcommand(cmd, useConfig)
	require.NotNil(t, cfgCmd)

	assert.ElementsMatch(t,
		[]string{useShow, useUpdate, useApply, useClusters, usePolicies, useRoutines, useStorage},
		subcommandNames(cfgCmd),
	)
}

// Each resource (clusters/policies/routines/storage) should expose the same
// list/show/delete/add/update subcommand surface.
func TestNewCmd_ConfigResourceCommandsSurface(t *testing.T) {
	t.Parallel()

	cmd := NewCmd()
	cfgCmd := findSubcommand(cmd, useConfig)
	require.NotNil(t, cfgCmd)

	for _, resource := range []string{useClusters, usePolicies, useRoutines, useStorage} {
		sub := findSubcommand(cfgCmd, resource)
		require.NotNilf(t, sub, "expected resource %q", resource)

		got := subcommandNames(sub)
		expected := []string{useList, useShow, useDelete, useAdd, useUpdate}

		// Routines additionally expose enable/disable.
		if resource == useRoutines {
			expected = append(expected, useEnable, useDisable)
		}

		assert.ElementsMatchf(t, expected, got, "subcommands of %q", resource)
	}
}

func TestNewCmd_RequiredFlags(t *testing.T) {
	t.Parallel()

	cmd := NewCmd()

	cases := map[string]struct {
		path     []string
		required string
	}{
		"backup cancel":          {[]string{useBackup, useCancel}, "name"},
		"backup status":          {[]string{useBackup, useStatus}, "name"},
		"backup full start":      {[]string{useBackup, useFull, useStart}, "name"},
		"backup incr start":      {[]string{useBackup, useIncremental, useStart}, "name"},
		"restore cancel":         {[]string{useRestore, useCancel}, "job-id"},
		"restore status":         {[]string{useRestore, useStatus}, "job-id"},
		"config update":          {[]string{useConfig, useUpdate}, "file"},
		"config clusters add":    {[]string{useConfig, useClusters, useAdd}, "name"},
		"config clusters update": {[]string{useConfig, useClusters, useUpdate}, "name"},
		"config clusters show":   {[]string{useConfig, useClusters, useShow}, "name"},
		"config clusters delete": {[]string{useConfig, useClusters, useDelete}, "name"},
		"config storage add":     {[]string{useConfig, useStorage, useAdd}, "name"},
		"config policies add":    {[]string{useConfig, usePolicies, useAdd}, "name"},
		"config routines add":    {[]string{useConfig, useRoutines, useAdd}, "name"},
	}

	for label, tc := range cases {
		t.Run(label, func(t *testing.T) {
			t.Parallel()

			node := cmd
			for _, name := range tc.path {
				node = findSubcommand(node, name)
				require.NotNilf(t, node, "could not find subcommand %v", tc.path)
			}

			flag := node.Flag(tc.required)
			require.NotNilf(t, flag, "expected --%s on %v", tc.required, tc.path)

			annotations := flag.Annotations[cobra.BashCompOneRequiredFlag]
			require.NotEmpty(t, annotations, "expected --%s on %v to be required", tc.required, tc.path)
			assert.Equal(t, "true", annotations[0])
		})
	}
}

func TestNewCmd_PersistentPreAndPostRunENilCloser(t *testing.T) {
	t.Parallel()

	cmd := NewCmd()

	// PreRunE should set up a logger using default flags without erroring.
	require.NoError(t, cmd.PersistentPreRunE(cmd, nil))

	// PostRunE should close the logger cleanly.
	require.NoError(t, cmd.PersistentPostRunE(cmd, nil))
}

func TestSetParentHelp_ServicePackagePrintsCommands(t *testing.T) {
	t.Parallel()

	parent := &cobra.Command{Use: "parent", Short: "parent short"}
	child := &cobra.Command{
		Use:   "child",
		Short: "child short",
		RunE:  func(_ *cobra.Command, _ []string) error { return nil },
	}
	parent.AddCommand(child)

	flagSet := &pflag.FlagSet{}
	flagSet.String("foo", "", "foo flag")

	setParentHelp(parent, flagSet)
	require.NotNil(t, parent.HelpFunc())

	out := captureStdout(t, func() {
		parent.HelpFunc()(parent, nil)
	})

	assert.Contains(t, out, "parent short")
	assert.Contains(t, out, "child")
	assert.Contains(t, out, "Flags:")
	assert.Contains(t, out, "--foo")
}

func TestSetLeafHelp_ServicePackagePrintsLocalFlags(t *testing.T) {
	t.Parallel()

	leaf := &cobra.Command{Use: "leaf", Short: "leaf short"}
	leaf.Flags().String("local", "", "local flag")

	setLeafHelp(leaf)

	out := captureStdout(t, func() {
		leaf.HelpFunc()(leaf, nil)
	})

	assert.Contains(t, out, "leaf short")
	assert.Contains(t, out, "--local")
}
