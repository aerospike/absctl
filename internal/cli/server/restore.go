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

// restoreFlags groups the operation-specific flag holders shared by all
// "server restore" subcommands.
type restoreFlags struct {
	aws *flags.AwsS3
}

func newRestoreCmd(rc *runCtx) *cobra.Command {
	rf := &restoreFlags{
		aws: flags.NewAwsS3(flags.OperationRestore),
	}

	cmd := &cobra.Command{
		Use:   useRestore,
		Short: "Manage server-integrated restores",
	}

	awsFlagSet := rf.aws.NewFlagSet()

	cmd.PersistentFlags().AddFlagSet(awsFlagSet)

	cmd.AddCommand(newRestoreStartCmd(rc, rf))
	cmd.AddCommand(newRestorePrepareCmd(rc, rf))

	setParentHelp(cmd,
		awsFlagSet,
	)

	return cmd
}

//nolint:dupl // Sub commands are intentionally symmetric per operation.
func newRestoreStartCmd(rc *runCtx, rf *restoreFlags) *cobra.Command {
	ssbFlags := flags.NewServerBackup()
	ssbFlagSet := ssbFlags.NewRestoreStartFlagSet()

	cmd := &cobra.Command{
		Use:   useStart,
		Short: "Start a server-integrated restore",
		Long:  "Start a server-integrated restore on the Aerospike cluster.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg := newIntegratedRestoreConfig(rc, rf, ssbFlags)

			if err := cfg.Validate(false); err != nil {
				return fmt.Errorf("failed to validate config: %w", err)
			}

			svc, err := server.NewService(cmd.Context(), cfg, rc.logger)
			if err != nil {
				return fmt.Errorf("server side restore initialization failed: %w", err)
			}

			if err := svc.StartRestore(cmd.Context()); err != nil {
				return fmt.Errorf("server side restore failed: %w", err)
			}

			return nil
		},
	}

	cmd.Flags().AddFlagSet(ssbFlagSet)
	setLeafHelp(cmd)

	return cmd
}

//nolint:dupl // Sub commands look the same.
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

			svc, err := server.NewService(cmd.Context(), cfg, rc.logger)
			if err != nil {
				return fmt.Errorf("server side restore initialization failed: %w", err)
			}

			if err := svc.PrepareRestore(cmd.Context()); err != nil {
				return fmt.Errorf("server side restore preparation failed: %w", err)
			}

			return nil
		},
	}

	cmd.Flags().AddFlagSet(ssbFlagSet)
	setLeafHelp(cmd)

	return cmd
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
		rf.aws.GetAwsS3(),
	)
}
