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
	"github.com/aerospike/absctl/internal/models"
	"github.com/aerospike/absctl/internal/server"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// backupFlags groups the operation-specific flag holders shared by all
// "server backup" subcommands.
type backupFlags struct {
	// Used for listing.
	aws *flags.AwsS3
	// Used for backup.
	objectStorageS3 *flags.ObjectStorageS3
}

// NewBackupCmd builds the top-level "backup" command for server-integrated backups.
func NewBackupCmd(flagsRoot *flags.Root, appVersion, commitHash, buildTime string) *cobra.Command {
	rc := newRunCtx(flagsRoot, appVersion, commitHash, buildTime)

	bf := &backupFlags{
		aws:             flags.NewAwsS3(flags.OperationRestore),
		objectStorageS3: flags.NewObjectStorageS3(),
	}

	cmd := &cobra.Command{
		Use:   useBackup,
		Short: "Manage server-integrated backups",
	}

	cmd.AddCommand(
		newBackupStartCmd(rc, bf),
		newBackupListCmd(rc, bf),
		newBackupProgressCmd(rc, bf),
		newBackupValidateCmd(rc, bf),
	)

	cmd.PersistentFlags().AddFlagSet(rc.app.NewFlagSet())

	setHelpBackup(cmd)

	return cmd
}

func setHelpBackup(cmd *cobra.Command) {
	cmd.SetHelpFunc(func(c *cobra.Command, _ []string) {
		fmt.Println(backupWelcomeMessage)
		fmt.Println(strings.Repeat("-", len(backupWelcomeMessage)))
		fmt.Println(flags.SectionTextUsageBackupServer)

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

func newBackupSvc(rc *runCtx, bf *backupFlags, ssbFlags *flags.ServerBackup,
) (*server.Service, error) {
	cfg := newIntegratedBackupConfig(rc, bf, ssbFlags)

	if err := cfg.Validate(false); err != nil {
		return nil, fmt.Errorf("failed to validate config: %w", err)
	}

	svc, err := server.NewService(cfg, rc.logger)
	if err != nil {
		return nil, fmt.Errorf("server side backup initialization failed: %w", err)
	}

	return svc, nil
}

func newBackupStartCmd(rc *runCtx, bf *backupFlags) *cobra.Command {
	ssbFlags := flags.NewServerBackup()
	ssbFlagSet := ssbFlags.NewBackupCreateFlagSet()
	objectStoreFlagSet := bf.objectStorageS3.NewFlagSet()

	cmd := &cobra.Command{
		Use:   useStart,
		Short: "Start a server-integrated backup",
		Long:  "Start a server-integrated backup on the Aerospike cluster.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			svc, err := newBackupSvc(rc, bf, ssbFlags)
			if err != nil {
				return err
			}

			if err := svc.StartBackup(cmd.Context()); err != nil {
				return fmt.Errorf("server side backup failed: %w", err)
			}

			return nil
		},
	}

	commonFlagSet := applyCommon(cmd, rc)
	cmd.Flags().AddFlagSet(ssbFlagSet)
	cmd.Flags().AddFlagSet(objectStoreFlagSet)

	setHelpBackupStart(cmd, ssbFlagSet, objectStoreFlagSet, commonFlagSet)

	return cmd
}

//nolint:dupl // Each sub-command should have it's own help.
func setHelpBackupStart(cmd *cobra.Command, ssbFlagSet, objectStoreFlagSet *pflag.FlagSet,
	commonFlagSet []*pflag.FlagSet) {
	cmd.SetHelpFunc(func(_ *cobra.Command, _ []string) {
		fmt.Println(backupWelcomeMessage)
		fmt.Println(strings.Repeat("-", len(backupWelcomeMessage)))
		fmt.Println(flags.SectionTextUsageBackupStart)

		// Print section: App Flags
		fmt.Println(flags.SectionTextGeneral)
		commonFlagSet[0].PrintDefaults()

		// Print section: Common Flags
		fmt.Println(flags.SectionTextAerospike)
		commonFlagSet[1].PrintDefaults()
		commonFlagSet[2].PrintDefaults()

		// Print section: Backup Flags
		fmt.Println(flags.SectionTextBackup)
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

func newBackupListCmd(rc *runCtx, bf *backupFlags) *cobra.Command {
	awsFlagSet := bf.aws.NewFlagSet()

	cmd := &cobra.Command{
		Use:   useList,
		Short: "List server-integrated backups",
		Long:  "List available server-integrated backups from the configured storage.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			svc, err := newBackupSvc(rc, bf, nil)
			if err != nil {
				return err
			}

			if err := svc.ListBackupsV2(cmd.Context()); err != nil {
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
	cmd.SetHelpFunc(func(_ *cobra.Command, _ []string) {
		fmt.Println(backupWelcomeMessage)
		fmt.Println(strings.Repeat("-", len(backupWelcomeMessage)))
		fmt.Println(flags.SectionTextUsageBackupList)

		// Print section: AWS Flags
		fmt.Println(flags.SectionTextAWS)
		awsFlagSet.PrintDefaults()
	})

	cmd.SetUsageFunc(func(c *cobra.Command) error {
		c.HelpFunc()(c, nil)
		return nil
	})
}

func newBackupProgressCmd(rc *runCtx, bf *backupFlags) *cobra.Command {
	cmd := &cobra.Command{
		Use:   useProgress,
		Short: "Shows the progress of a backup",
		Long:  "Shows the progress of a current ongoing server backup",
		RunE: func(cmd *cobra.Command, _ []string) error {
			svc, err := newBackupSvc(rc, bf, nil)
			if err != nil {
				return err
			}

			if err := svc.GetStatus(cmd.Context()); err != nil {
				return fmt.Errorf("getting backup progress failed: %w", err)
			}

			return nil
		},
	}

	commonFlagSet := applyCommon(cmd, rc)
	setHelpBackupProgress(cmd, commonFlagSet)

	return cmd
}

func setHelpBackupProgress(cmd *cobra.Command, commonFlagSet []*pflag.FlagSet) {
	cmd.SetHelpFunc(func(_ *cobra.Command, _ []string) {
		fmt.Println(backupWelcomeMessage)
		fmt.Println(strings.Repeat("-", len(backupWelcomeMessage)))
		fmt.Println(flags.SectionTextUsageBackupProgress)

		// Print section: App Flags
		fmt.Println(flags.SectionTextGeneral)
		commonFlagSet[0].PrintDefaults()

		// Print section: Common Flags
		fmt.Println(flags.SectionTextAerospike)
		commonFlagSet[1].PrintDefaults()
		commonFlagSet[2].PrintDefaults()

		// Print section: Secret Agent Flags
		fmt.Println(flags.SectionTextSecretAgentBackup)
		commonFlagSet[3].PrintDefaults()
	})

	cmd.SetUsageFunc(func(c *cobra.Command) error {
		c.HelpFunc()(c, nil)
		return nil
	})
}

func newBackupValidateCmd(rc *runCtx, bf *backupFlags) *cobra.Command {
	ssbFlags := flags.NewServerBackup()
	ssbFlagSet := ssbFlags.NewBackupValidateFlagSet()
	awsFlagSet := bf.aws.NewFlagSet()

	cmd := &cobra.Command{
		Use:   useValidate,
		Short: "Validate server-integrated backups",
		Long:  "Validate available server-integrated backups from the configured storage.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			svc, err := newBackupSvc(rc, bf, ssbFlags)
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

	setHelpValidate(cmd, awsFlagSet, ssbFlagSet)

	return cmd
}

func setHelpValidate(cmd *cobra.Command, awsFlagSet, ssbFlagSet *pflag.FlagSet) {
	cmd.SetHelpFunc(func(_ *cobra.Command, _ []string) {
		fmt.Println(backupWelcomeMessage)
		fmt.Println(strings.Repeat("-", len(backupWelcomeMessage)))
		fmt.Println(flags.SectionTextUsageValidate)

		// Print section: Backup Flags
		fmt.Println(flags.SectionTextBackup)
		ssbFlagSet.PrintDefaults()

		// Print section: AWS Flags
		fmt.Println(flags.SectionTextAWS)
		awsFlagSet.PrintDefaults()
	})

	cmd.SetUsageFunc(func(c *cobra.Command) error {
		c.HelpFunc()(c, nil)
		return nil
	})
}

// newIntegratedBackupConfig builds the IntegratedServiceConfig from the flag
// objects collected on the parent commands.
func newIntegratedBackupConfig(
	rc *runCtx,
	bf *backupFlags,
	sb *flags.ServerBackup,
) *config.ServerBackupServiceConfig {
	var sbCfg *models.ServerBackup
	awsCfg := bf.aws.GetAwsS3()
	// TODO: refactore this!
	if sb != nil && sb.SampleSize == 0 {
		sbCfg = sb.GetIntegratedBackup()
		awsCfg = bf.objectStorageS3.ToAwsS3()
	}

	if sb != nil && sb.SampleSize != 0 {
		sbCfg = sb.GetIntegratedBackup()
	}

	return config.NewServerBackupServiceConfig(
		sbCfg,
		rc.app.GetApp(),
		rc.aerospike.NewAerospikeConfig(),
		rc.clientPolicy.GetClientPolicy(),
		rc.secretAgent.GetSecretAgent(),
		awsCfg,
	)
}
