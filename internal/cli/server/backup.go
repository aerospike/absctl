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

	"github.com/aerospike/absctl/internal/config"
	"github.com/aerospike/absctl/internal/flags"
	"github.com/aerospike/absctl/internal/server"
	"github.com/spf13/cobra"
)

// backupFlags groups the operation-specific flag holders shared by all
// "server backup" subcommands.
type backupFlags struct {
	aws *flags.AwsS3
}

func newBackupCmd(rc *runCtx) *cobra.Command {
	bf := &backupFlags{
		aws: flags.NewAwsS3(flags.OperationBackup),
	}

	cmd := &cobra.Command{
		Use:   useBackup,
		Short: "Manage server-integrated backups",
	}

	awsFlagSet := bf.aws.NewFlagSet()

	cmd.PersistentFlags().AddFlagSet(awsFlagSet)

	cmd.AddCommand(
		newBackupStartCmd(rc, bf),
		newBackupListCmd(rc, bf),
	)

	setParentHelp(cmd,
		awsFlagSet,
	)

	return cmd
}

//nolint:dupl // Sub commands are intentionally symmetric per operation.
func newBackupStartCmd(rc *runCtx, bf *backupFlags) *cobra.Command {
	ssbFlags := flags.NewServerBackup()
	ssbFlagSet := ssbFlags.NewBackupCreateFlagSet()

	cmd := &cobra.Command{
		Use:   useStart,
		Short: "Start a server-integrated backup",
		Long:  "Start a server-integrated backup on the Aerospike cluster.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg := newIntegratedBackupConfig(rc, bf, ssbFlags)

			if err := cfg.Validate(true); err != nil {
				return fmt.Errorf("failed to validate config: %w", err)
			}

			svc, err := server.NewService(cmd.Context(), cfg, rc.logger)
			if err != nil {
				return fmt.Errorf("server side backup initialization failed: %w", err)
			}

			if err := svc.StartBackup(cmd.Context()); err != nil {
				return fmt.Errorf("server side backup failed: %w", err)
			}

			return nil
		},
	}

	cmd.Flags().AddFlagSet(ssbFlagSet)
	setLeafHelp(cmd)

	return cmd
}

func newBackupListCmd(rc *runCtx, bf *backupFlags) *cobra.Command {
	ssbFlags := flags.NewServerBackup()
	ssbFlagSet := ssbFlags.NewBackupListFlagSet()

	cmd := &cobra.Command{
		Use:   useList,
		Short: "List server-integrated backups",
		Long:  "List available server-integrated backups from the configured storage.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg := newIntegratedBackupConfig(rc, bf, ssbFlags)

			// Mock.
			if cfg.ServerBackup.ListPath == "" {
				cfg.ServerBackup.ListPath = "/"
			}

			if err := cfg.Validate(true); err != nil {
				return fmt.Errorf("failed to validate config: %w", err)
			}

			svc, err := server.NewService(cmd.Context(), cfg, rc.logger)
			if err != nil {
				return fmt.Errorf("server side backup initialization failed: %w", err)
			}

			if err := svc.ListBackups(cmd.Context()); err != nil {
				return fmt.Errorf("listing backups failed: %w", err)
			}

			return nil
		},
	}

	cmd.Flags().AddFlagSet(ssbFlagSet)
	setLeafHelp(cmd)

	return cmd
}

// newIntegratedBackupConfig builds the IntegratedServiceConfig from the flag
// objects collected on the parent commands.
func newIntegratedBackupConfig(
	rc *runCtx,
	bf *backupFlags,
	ssbFlags *flags.ServerBackup,
) *config.ServerBackupServiceConfig {
	return config.NewServerBackupServiceConfig(
		ssbFlags.GetIntegratedBackup(),
		rc.app.GetApp(),
		rc.aerospike.NewAerospikeConfig(),
		rc.clientPolicy.GetClientPolicy(),
		rc.secretAgent.GetSecretAgent(),
		bf.aws.GetAwsS3(),
	)
}
