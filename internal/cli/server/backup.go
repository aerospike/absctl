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

// backupFlags groups the storage flag holders shared by the "server backup"
// subcommands.
type backupFlags struct {
	// aws is the listing/validation source (list, validate, progress).
	aws *flags.AwsS3
	// objectStorageS3 is the backup write target (start).
	objectStorageS3 *flags.ObjectStorageS3
}

// NewBackupCmd builds the top-level "backup" command for server-integrated
// backups.
func NewBackupCmd(flagsRoot *flags.Root, appVersion, commitHash, buildTime string) *cobra.Command {
	rc := newRunCtx(flagsRoot, appVersion, commitHash, buildTime)

	bf := &backupFlags{
		aws:             flags.NewAwsS3(flags.OperationRestore),
		objectStorageS3: flags.NewObjectStorageS3(),
	}

	cmd := &cobra.Command{
		Use:   useBackup,
		Short: shortBackup,
	}

	cmd.AddCommand(
		newBackupStartCmd(rc, bf),
		newBackupListCmd(rc, bf),
		newBackupProgressCmd(rc, bf),
		newBackupValidateCmd(rc, bf),
	)

	applyRootPersistent(cmd, rc)
	setHelpBackup(cmd)

	return cmd
}

func setHelpBackup(cmd *cobra.Command) {
	cmd.SetHelpFunc(func(c *cobra.Command, _ []string) {
		w := c.OutOrStdout()

		printHelpHeader(w, flags.SectionTextUsageBackupServer)
		printCommands(w, c)
	})

	usageFromHelp(cmd)
}

//nolint:dupl // Start, list, progress, validate look the same, but it should be different sub commands.
func newBackupStartCmd(rc *runCtx, bf *backupFlags) *cobra.Command {
	ssbFlags := flags.NewServerBackup()
	ssbFlagSet := ssbFlags.NewBackupCreateFlagSet()
	objectStoreFlagSet := bf.objectStorageS3.NewFlagSet()

	cmd := &cobra.Command{
		Use:   useStart,
		Short: shortBackupStart,
		Long:  longBackupStart,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg := rc.newServiceConfig(ssbFlags.GetIntegratedBackup(), bf.objectStorageS3.ToAwsS3())

			svc, err := newService(rc, cfg, errInitBackup)
			if err != nil {
				return err
			}

			if err := svc.StartBackup(cmd.Context()); err != nil {
				return fmt.Errorf("server side backup failed: %w", err)
			}

			return nil
		},
	}

	common := applyCommon(cmd, rc)
	cmd.Flags().AddFlagSet(ssbFlagSet)
	cmd.Flags().AddFlagSet(objectStoreFlagSet)

	setHelpBackupStart(cmd, ssbFlagSet, objectStoreFlagSet, common)

	return cmd
}

func setHelpBackupStart(
	cmd *cobra.Command,
	ssbFlagSet, objectStoreFlagSet *pflag.FlagSet,
	common commonFlagSets,
) {
	cmd.SetHelpFunc(func(c *cobra.Command, _ []string) {
		w := c.OutOrStdout()

		printHelpHeader(w, flags.SectionTextUsageBackupStart)
		printSection(w, flags.SectionTextGeneral, common.app)
		printSection(w, flags.SectionTextAerospike, common.aerospike, common.clientPolicy)
		printSection(w, flags.SectionTextBackup, ssbFlagSet)
		printSection(w, flags.SectionTextAWS, objectStoreFlagSet)
		printSection(w, flags.SectionTextSecretAgentBackup, common.secretAgent)
	})

	usageFromHelp(cmd)
}

func newBackupListCmd(rc *runCtx, bf *backupFlags) *cobra.Command {
	awsFlagSet := bf.aws.NewFlagSet()

	cmd := &cobra.Command{
		Use:   useList,
		Short: shortBackupList,
		Long:  longBackupList,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg := rc.newServiceConfig(nil, bf.aws.GetAwsS3())

			svc, err := newService(rc, cfg, errInitBackup)
			if err != nil {
				return err
			}

			if err := svc.ListBackups(cmd.Context()); err != nil {
				return fmt.Errorf("listing backups failed: %w", err)
			}

			return nil
		},
	}

	cmd.Flags().AddFlagSet(awsFlagSet)

	setHelpBackupList(cmd, awsFlagSet)

	return cmd
}

func setHelpBackupList(cmd *cobra.Command, awsFlagSet *pflag.FlagSet) {
	cmd.SetHelpFunc(func(c *cobra.Command, _ []string) {
		w := c.OutOrStdout()

		printHelpHeader(w, flags.SectionTextUsageBackupList)
		printSection(w, flags.SectionTextAWS, awsFlagSet)
	})

	usageFromHelp(cmd)
}

func newBackupProgressCmd(rc *runCtx, bf *backupFlags) *cobra.Command {
	cmd := &cobra.Command{
		Use:   useProgress,
		Short: shortBackupProgress,
		Long:  longBackupProgress,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg := rc.newServiceConfig(nil, bf.aws.GetAwsS3())

			svc, err := newService(rc, cfg, errInitBackup)
			if err != nil {
				return err
			}

			if err := svc.GetStatus(cmd.Context()); err != nil {
				return fmt.Errorf("getting backup progress failed: %w", err)
			}

			return nil
		},
	}

	common := applyCommon(cmd, rc)

	setHelpBackupProgress(cmd, common)

	return cmd
}

func setHelpBackupProgress(cmd *cobra.Command, common commonFlagSets) {
	cmd.SetHelpFunc(func(c *cobra.Command, _ []string) {
		w := c.OutOrStdout()

		printHelpHeader(w, flags.SectionTextUsageBackupProgress)
		printSection(w, flags.SectionTextGeneral, common.app)
		printSection(w, flags.SectionTextAerospike, common.aerospike, common.clientPolicy)
		printSection(w, flags.SectionTextSecretAgentBackup, common.secretAgent)
	})

	usageFromHelp(cmd)
}

func newBackupValidateCmd(rc *runCtx, bf *backupFlags) *cobra.Command {
	ssbFlags := flags.NewServerBackup()
	ssbFlagSet := ssbFlags.NewBackupValidateFlagSet()
	awsFlagSet := bf.aws.NewFlagSet()

	cmd := &cobra.Command{
		Use:   useValidate,
		Short: shortBackupValidate,
		Long:  longBackupValidate,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg := rc.newServiceConfig(ssbFlags.GetIntegratedBackup(), bf.aws.GetAwsS3())

			svc, err := newService(rc, cfg, errInitBackup)
			if err != nil {
				return err
			}

			if err := svc.Validate(cmd.Context()); err != nil {
				return fmt.Errorf("validating backups failed: %w", err)
			}

			return nil
		},
	}

	cmd.Flags().AddFlagSet(ssbFlagSet)
	cmd.Flags().AddFlagSet(awsFlagSet)

	setHelpBackupValidate(cmd, awsFlagSet, ssbFlagSet)

	return cmd
}

func setHelpBackupValidate(cmd *cobra.Command, awsFlagSet, ssbFlagSet *pflag.FlagSet) {
	cmd.SetHelpFunc(func(c *cobra.Command, _ []string) {
		w := c.OutOrStdout()

		printHelpHeader(w, flags.SectionTextUsageValidate)
		printSection(w, flags.SectionTextBackup, ssbFlagSet)
		printSection(w, flags.SectionTextAWS, awsFlagSet)
	})

	usageFromHelp(cmd)
}
