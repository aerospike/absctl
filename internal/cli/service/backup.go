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
	"log/slog"

	"github.com/aerospike/absctl/internal/flags"
	backupService "github.com/aerospike/absctl/internal/service"
	"github.com/spf13/cobra"
)

func newBackupCmd(rc *runCtx) *cobra.Command {
	cmd := &cobra.Command{
		Use:   useBackup,
		Short: "Manage backups on the Aerospike Backup Service",
	}

	cmd.AddCommand(
		newBackupCancelCmd(rc),
		newBackupStatusCmd(rc),
		newBackupFullCmd(rc),
		newBackupIncrementalCmd(rc),
	)

	setParentHelp(cmd)

	return cmd
}

func newBackupCancelCmd(rc *runCtx) *cobra.Command {
	f := flags.NewServiceBackupRoutine()

	cmd := &cobra.Command{
		Use:   useCancel,
		Short: "Cancel a currently running backup",
		RunE: func(cmd *cobra.Command, _ []string) error {
			handler, err := newBackupHandler(rc)
			if err != nil {
				return err
			}

			if err := handler.Cancel(cmd.Context(), f.Name); err != nil {
				return err
			}

			rc.logger.Info("backup canceled successfully", slog.String("routine", f.Name))

			return nil
		},
	}

	cmd.Flags().AddFlagSet(f.NewFlagSet())
	_ = cmd.MarkFlagRequired("name")
	setLeafHelp(cmd)

	return cmd
}

func newBackupStatusCmd(rc *runCtx) *cobra.Command {
	f := flags.NewServiceBackupRoutine()

	cmd := &cobra.Command{
		Use:   useStatus,
		Short: "Get current backup statistics for a routine",
		RunE: func(cmd *cobra.Command, _ []string) error {
			handler, err := newBackupHandler(rc)
			if err != nil {
				return err
			}

			return handler.Status(cmd.Context(), f.Name)
		},
	}

	cmd.Flags().AddFlagSet(f.NewFlagSet())
	_ = cmd.MarkFlagRequired("name")
	setLeafHelp(cmd)

	return cmd
}

// newBackupHandler validates the service connection and creates a BackupHandler
// configured with the shared logger and JSON output flag.
func newBackupHandler(rc *runCtx) (*backupService.BackupHandler, error) {
	conn := rc.conn.GetServiceConnection()
	if err := conn.Validate(); err != nil {
		return nil, err
	}

	return backupService.NewBackupHandler(conn.ServerURL(), rc.logger, rc.app.GetApp().LogJSON)
}
