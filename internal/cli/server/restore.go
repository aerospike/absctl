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
	"fmt"

	"github.com/aerospike/absctl/internal/flags"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// restoreFlags groups the storage flag holders shared by the "server restore"
// subcommands.
type restoreFlags struct {
	// objectStorageS3 is the restore read source (start, prepare).
	objectStorageS3 *flags.ObjectStorageS3
}

// NewRestoreCmd builds the top-level "restore" command for server-integrated
// restores.
func NewRestoreCmd(flagsRoot *flags.Root, appVersion, commitHash, buildTime string) *cobra.Command {
	rc := newRunCtx(flagsRoot, appVersion, commitHash, buildTime)

	rf := &restoreFlags{
		objectStorageS3: flags.NewObjectStorageS3(),
	}

	cmd := &cobra.Command{
		Use:   useRestore,
		Short: shortRestore,
	}

	cmd.AddCommand(
		newRestoreStartCmd(rc, rf),
		newRestorePrepareCmd(rc, rf),
	)

	applyRootPersistent(cmd, rc)
	setHelpRestore(cmd)

	return cmd
}

func setHelpRestore(cmd *cobra.Command) {
	cmd.SetHelpFunc(func(c *cobra.Command, _ []string) {
		w := c.OutOrStdout()

		printHelpHeader(w, flags.SectionTextUsageRestoreServer)
		printCommands(w, c)
	})

	usageFromHelp(cmd)
}

//nolint:dupl // Restore commands looks the same, but it should be different sub commands.
func newRestoreStartCmd(rc *runCtx, rf *restoreFlags) *cobra.Command {
	ssbFlags := flags.NewServerBackup()
	ssbFlagSet := ssbFlags.NewRestoreStartFlagSet()
	objectStoreFlagSet := rf.objectStorageS3.NewFlagSet()

	cmd := &cobra.Command{
		Use:   useStart,
		Short: shortRestoreStart,
		Long:  longRestoreStart,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg := rc.newServiceConfig(ssbFlags.GetIntegratedBackup(), rf.objectStorageS3.ToAwsS3())

			svc, err := newService(rc, cfg, errInitRestore)
			if err != nil {
				return err
			}

			if err := svc.StartRestore(cmd.Context()); err != nil {
				return fmt.Errorf("server side restore failed: %w", err)
			}

			return nil
		},
	}

	common := applyCommon(cmd, rc)
	cmd.Flags().AddFlagSet(ssbFlagSet)
	cmd.Flags().AddFlagSet(objectStoreFlagSet)

	setHelpRestoreStart(cmd, ssbFlagSet, objectStoreFlagSet, common)

	return cmd
}

func setHelpRestoreStart(
	cmd *cobra.Command,
	ssbFlagSet, objectStoreFlagSet *pflag.FlagSet,
	common commonFlagSets,
) {
	cmd.SetHelpFunc(func(c *cobra.Command, _ []string) {
		w := c.OutOrStdout()

		printHelpHeader(w, flags.SectionTextUsageRestoreStart)
		printSection(w, flags.SectionTextGeneral, common.app)
		printSection(w, flags.SectionTextAerospike, common.aerospike, common.clientPolicy)
		printSection(w, flags.SectionTextRestore, ssbFlagSet)
		printSection(w, flags.SectionTextAWS, objectStoreFlagSet)
		printSection(w, flags.SectionTextSecretAgentBackup, common.secretAgent)
	})

	usageFromHelp(cmd)
}

func newRestorePrepareCmd(rc *runCtx, rf *restoreFlags) *cobra.Command {
	ssbFlags := flags.NewServerBackup()
	ssbFlagSet := ssbFlags.NewRestoreStartFlagSet()

	cmd := &cobra.Command{
		Use:   usePrepare,
		Short: shortRestorePrepare,
		Long:  longRestorePrepare,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg := rc.newServiceConfig(ssbFlags.GetIntegratedBackup(), rf.objectStorageS3.ToAwsS3())

			svc, err := newService(rc, cfg, errInitRestore)
			if err != nil {
				return err
			}

			if err := svc.PrepareRestore(cmd.Context()); err != nil {
				return fmt.Errorf("server side restore preparation failed: %w", err)
			}

			return nil
		},
	}

	common := applyCommon(cmd, rc)
	cmd.Flags().AddFlagSet(ssbFlagSet)

	setHelpRestorePrepare(cmd, ssbFlagSet, common)

	return cmd
}

func setHelpRestorePrepare(cmd *cobra.Command, ssbFlagSet *pflag.FlagSet, common commonFlagSets) {
	cmd.SetHelpFunc(func(c *cobra.Command, _ []string) {
		w := c.OutOrStdout()

		printHelpHeader(w, flags.SectionTextUsageRestorePrepare)
		printSection(w, flags.SectionTextGeneral, common.app)
		printSection(w, flags.SectionTextAerospike, common.aerospike, common.clientPolicy)
		printSection(w, flags.SectionTextRestore, ssbFlagSet)
		printSection(w, flags.SectionTextSecretAgentBackup, common.secretAgent)
	})

	usageFromHelp(cmd)
}
