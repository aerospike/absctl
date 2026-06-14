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
	"strings"

	"github.com/aerospike/absctl/internal/config"
	"github.com/aerospike/absctl/internal/flags"
	"github.com/aerospike/absctl/internal/server"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// restoreFlags groups the operation-specific flag holders shared by all
// "server restore" subcommands.
type restoreFlags struct {
	// Used for restore.
	objectStorageS3 *flags.ObjectStorageS3
}

// NewRestoreCmd builds the top-level "restore" command for server-integrated restores.
func NewRestoreCmd(flagsRoot *flags.Root, appVersion, commitHash, buildTime string) *cobra.Command {
	rc := newRunCtx(flagsRoot, appVersion, commitHash, buildTime)

	rf := &restoreFlags{
		objectStorageS3: flags.NewObjectStorageS3(),
	}

	cmd := &cobra.Command{
		Use:   useRestore,
		Short: "Manage server-integrated restores",
	}

	cmd.AddCommand(
		newRestoreStartCmd(rc, rf),
		newRestorePrepareCmd(rc, rf),
	)

	setHelpRestore(cmd)

	return cmd
}

func setHelpRestore(cmd *cobra.Command) {
	cmd.SetHelpFunc(func(c *cobra.Command, _ []string) {
		fmt.Println(backupWelcomeMessage)
		fmt.Println(strings.Repeat("-", len(backupWelcomeMessage)))
		fmt.Println(flags.SectionTextUsageRestoreServer)

		fmt.Println(flags.SectionAvailableCommands)

		for _, sub := range c.Commands() {
			if !sub.IsAvailableCommand() {
				continue
			}

			fmt.Printf("  %-25s %s\n", sub.Name(), sub.Short)
		}
	})

	cmd.SetUsageFunc(func(c *cobra.Command) error {
		c.HelpFunc()(c, nil)
		return nil
	})
}

func newRestoreStartCmd(rc *runCtx, rf *restoreFlags) *cobra.Command {
	ssbFlags := flags.NewServerBackup()
	ssbFlagSet := ssbFlags.NewRestoreStartFlagSet()
	objectStoreFlagSet := rf.objectStorageS3.NewFlagSet()

	cmd := &cobra.Command{
		Use:   useStart,
		Short: "Start a server-integrated restore",
		Long:  "Start a server-integrated restore on the Aerospike cluster.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg := newIntegratedRestoreConfig(rc, rf, ssbFlags)

			if err := cfg.Validate(false); err != nil {
				return fmt.Errorf("failed to validate config: %w", err)
			}

			svc, err := server.NewService(cfg, rc.logger)
			if err != nil {
				return fmt.Errorf("server side restore initialization failed: %w", err)
			}

			if err := svc.StartRestore(cmd.Context()); err != nil {
				return fmt.Errorf("server side restore failed: %w", err)
			}

			return nil
		},
	}

	commonFlagSet := applyCommon(cmd, rc)
	cmd.Flags().AddFlagSet(ssbFlagSet)
	cmd.Flags().AddFlagSet(objectStoreFlagSet)

	setHelpRestoreStart(cmd, ssbFlagSet, objectStoreFlagSet, commonFlagSet)

	return cmd
}

//nolint:dupl // Each sub-command should have it's own help.
func setHelpRestoreStart(cmd *cobra.Command, ssbFlagSet, objectStoreFlagSet *pflag.FlagSet,
	commonFlagSet []*pflag.FlagSet) {
	cmd.SetHelpFunc(func(_ *cobra.Command, _ []string) {
		fmt.Println(backupWelcomeMessage)
		fmt.Println(strings.Repeat("-", len(backupWelcomeMessage)))
		fmt.Println(flags.SectionTextUsageRestoreStart)

		// Print section: App Flags
		fmt.Println(flags.SectionTextGeneral)
		commonFlagSet[0].PrintDefaults()

		// Print section: Common Flags
		fmt.Println(flags.SectionTextAerospike)
		commonFlagSet[1].PrintDefaults()
		commonFlagSet[2].PrintDefaults()

		// Print section: Restore Flags
		fmt.Println(flags.SectionTextRestore)
		ssbFlagSet.PrintDefaults()

		// Print section: AWS Flags
		fmt.Println(flags.SectionTextAWS)
		objectStoreFlagSet.PrintDefaults()

		// Print section: Secret Agent Flags
		fmt.Println(flags.SectionTextSecretAgentBackup)
		commonFlagSet[3].PrintDefaults()
	})

	cmd.SetUsageFunc(func(c *cobra.Command) error {
		c.HelpFunc()(c, nil)
		return nil
	})
}

func newRestorePrepareCmd(rc *runCtx, rf *restoreFlags) *cobra.Command {
	ssbFlags := flags.NewServerBackup()
	ssbFlagSet := ssbFlags.NewRestoreStartFlagSet()

	cmd := &cobra.Command{
		Use:   usePrepare,
		Short: "Prepare a server-integrated restore",
		Long:  "Prepare a server-integrated restore on the Aerospike cluster.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg := newIntegratedRestoreConfig(rc, rf, ssbFlags)

			if err := cfg.Validate(false); err != nil {
				return fmt.Errorf("failed to validate config: %w", err)
			}

			svc, err := server.NewService(cfg, rc.logger)
			if err != nil {
				return fmt.Errorf("server side restore initialization failed: %w", err)
			}

			if err := svc.PrepareRestore(cmd.Context()); err != nil {
				return fmt.Errorf("server side restore preparation failed: %w", err)
			}

			return nil
		},
	}

	commonFlagSet := applyCommon(cmd, rc)
	cmd.Flags().AddFlagSet(ssbFlagSet)

	setHelpRestorePrepare(cmd, ssbFlagSet, commonFlagSet)

	return cmd
}

func setHelpRestorePrepare(cmd *cobra.Command, ssbFlagSet *pflag.FlagSet,
	commonFlagSet []*pflag.FlagSet) {
	cmd.SetHelpFunc(func(_ *cobra.Command, _ []string) {
		fmt.Println(backupWelcomeMessage)
		fmt.Println(strings.Repeat("-", len(backupWelcomeMessage)))
		fmt.Println(flags.SectionTextUsageRestorePrepare)

		// Print section: App Flags
		fmt.Println(flags.SectionTextGeneral)
		commonFlagSet[0].PrintDefaults()

		// Print section: Common Flags
		fmt.Println(flags.SectionTextAerospike)
		commonFlagSet[1].PrintDefaults()
		commonFlagSet[2].PrintDefaults()

		// Print section: Restore Flags
		fmt.Println(flags.SectionTextRestore)
		ssbFlagSet.PrintDefaults()

		// Print section: Secret Agent Flags
		fmt.Println(flags.SectionTextSecretAgentBackup)
		commonFlagSet[3].PrintDefaults()
	})

	cmd.SetUsageFunc(func(c *cobra.Command) error {
		c.HelpFunc()(c, nil)
		return nil
	})
}

// newIntegratedRestoreConfig builds the IntegratedServiceConfig from the flag
// objects collected on the parent commands.
func newIntegratedRestoreConfig(
	rc *runCtx,
	rf *restoreFlags,
	ssbFlags *flags.ServerBackup,
) *config.ServerBackupServiceConfig {
	return config.NewServerBackupServiceConfig(
		ssbFlags.GetIntegratedBackup(),
		rc.app.GetApp(),
		rc.aerospike.NewAerospikeConfig(),
		rc.clientPolicy.GetClientPolicy(),
		rc.secretAgent.GetSecretAgent(),
		rf.objectStorageS3.ToAwsS3(),
	)
}
