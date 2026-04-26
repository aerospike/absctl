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

//nolint:dupl // Full, incremental backups look the same, but it should be different sub commands.
package service

import (
	"log/slog"

	"github.com/aerospike/absctl/internal/flags"
	"github.com/spf13/cobra"
)

func newBackupFullCmd(rc *runCtx) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "full",
		Short: "Manage full backups",
	}

	cmd.AddCommand(
		newBackupFullListCmd(rc),
		newBackupFullStartCmd(rc),
	)

	setParentHelp(cmd)

	return cmd
}

func newBackupFullListCmd(rc *runCtx) *cobra.Command {
	f := flags.NewServiceBackupList()

	cmd := &cobra.Command{
		Use:   "list",
		Short: "List available full backups",
		RunE: func(cmd *cobra.Command, _ []string) error {
			handler, err := newBackupHandler(rc.conn)
			if err != nil {
				return err
			}

			data, err := handler.ListFull(cmd.Context(), f.Name, f.From, f.To)
			if err != nil {
				return err
			}

			rc.logger.Info("full backups", slog.Any("backups", data))

			return nil
		},
	}

	cmd.Flags().AddFlagSet(f.NewFlagSet())
	setLeafHelp(cmd)

	return cmd
}

func newBackupFullStartCmd(rc *runCtx) *cobra.Command {
	f := flags.NewServiceBackupTrigger()

	cmd := &cobra.Command{
		Use:   "start",
		Short: "Start a full backup for a routine",
		RunE: func(cmd *cobra.Command, _ []string) error {
			handler, err := newBackupHandler(rc.conn)
			if err != nil {
				return err
			}

			if err := handler.TriggerFull(cmd.Context(), f.Name, f.Delay); err != nil {
				return err
			}

			rc.logger.Info("full backup started successfully", slog.String("routine", f.Name))

			return nil
		},
	}

	cmd.Flags().AddFlagSet(f.NewFlagSet())
	_ = cmd.MarkFlagRequired("name")
	setLeafHelp(cmd)

	return cmd
}
