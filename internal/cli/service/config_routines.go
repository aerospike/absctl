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
	"context"
	"log/slog"

	"github.com/aerospike/absctl/internal/flags"
	configService "github.com/aerospike/absctl/internal/service"
	"github.com/spf13/cobra"
)

func newConfigRoutinesCmd(rc *runCtx) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "routines",
		Short: "Manage backup routine definitions",
	}

	addCommonResourceSubcommands(cmd, rc, &resourceOps{
		resource:       "routine",
		resourcePlural: "routines",
		listKey:        "routines",
		itemKey:        "routine",
		list: func(ctx context.Context, h *configService.ConfigHandler) (any, error) {
			return h.ListRoutines(ctx)
		},
		show: func(ctx context.Context, h *configService.ConfigHandler, name string) (any, error) {
			return h.ReadRoutine(ctx, name)
		},
		del: func(ctx context.Context, h *configService.ConfigHandler, name string) error {
			return h.DeleteRoutine(ctx, name)
		},
	})

	cmd.AddCommand(
		newConfigRoutineAddCmd(rc),
		newConfigRoutineUpdateCmd(rc),
		newConfigRoutineEnableCmd(rc),
		newConfigRoutineDisableCmd(rc),
	)

	setParentHelp(cmd)

	return cmd
}

//nolint:dupl // add/update commands are intentionally symmetric per resource; see newConfigClustersCmd doc.
func newConfigRoutineAddCmd(rc *runCtx) *cobra.Command {
	f := flags.NewServiceConfigRoutine()

	cmd := &cobra.Command{
		Use:   "add",
		Short: "Add a new backup routine definition",
		Long: "Add a new backup routine definition. Body fields can be supplied as " +
			"individual flags or via --file (a JSON DtoBackupRoutine body). When both " +
			"are provided, flag values override the corresponding fields loaded from " +
			"the file.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := f.Validate(); err != nil {
				return err
			}

			handler, err := newConfigHandler(rc.conn)
			if err != nil {
				return err
			}

			if err := handler.AddRoutine(cmd.Context(), &f.ConfigRoutineFields); err != nil {
				return err
			}

			rc.logger.Info("routine added successfully", slog.String("name", f.Name))

			return nil
		},
	}

	cmd.Flags().AddFlagSet(f.NewFlagSet())
	_ = cmd.MarkFlagRequired("name")
	setLeafHelp(cmd)

	return cmd
}

//nolint:dupl // add/update commands are intentionally symmetric per resource; see newConfigClustersCmd doc.
func newConfigRoutineUpdateCmd(rc *runCtx) *cobra.Command {
	f := flags.NewServiceConfigRoutine()

	cmd := &cobra.Command{
		Use:   "update",
		Short: "Replace an existing backup routine definition",
		Long: "Replace an existing backup routine definition. Body fields can be " +
			"supplied as individual flags or via --file (a JSON DtoBackupRoutine body). " +
			"When both are provided, flag values override the corresponding fields " +
			"loaded from the file.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := f.Validate(); err != nil {
				return err
			}

			handler, err := newConfigHandler(rc.conn)
			if err != nil {
				return err
			}

			if err := handler.UpdateRoutine(cmd.Context(), &f.ConfigRoutineFields); err != nil {
				return err
			}

			rc.logger.Info("routine updated successfully", slog.String("name", f.Name))

			return nil
		},
	}

	cmd.Flags().AddFlagSet(f.NewFlagSet())
	_ = cmd.MarkFlagRequired("name")
	setLeafHelp(cmd)

	return cmd
}

func newConfigRoutineEnableCmd(rc *runCtx) *cobra.Command {
	f := flags.NewServiceConfigName()

	cmd := &cobra.Command{
		Use:   "enable",
		Short: "Enable a backup routine",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := f.Validate(); err != nil {
				return err
			}

			handler, err := newConfigHandler(rc.conn)
			if err != nil {
				return err
			}

			if err := handler.EnableRoutine(cmd.Context(), f.Name); err != nil {
				return err
			}

			rc.logger.Info("routine enabled successfully", slog.String("name", f.Name))

			return nil
		},
	}

	cmd.Flags().AddFlagSet(f.NewFlagSet())
	_ = cmd.MarkFlagRequired("name")
	setLeafHelp(cmd)

	return cmd
}

func newConfigRoutineDisableCmd(rc *runCtx) *cobra.Command {
	f := flags.NewServiceConfigName()

	cmd := &cobra.Command{
		Use:   "disable",
		Short: "Disable a backup routine",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := f.Validate(); err != nil {
				return err
			}

			handler, err := newConfigHandler(rc.conn)
			if err != nil {
				return err
			}

			if err := handler.DisableRoutine(cmd.Context(), f.Name); err != nil {
				return err
			}

			rc.logger.Info("routine disabled successfully", slog.String("name", f.Name))

			return nil
		},
	}

	cmd.Flags().AddFlagSet(f.NewFlagSet())
	_ = cmd.MarkFlagRequired("name")
	setLeafHelp(cmd)

	return cmd
}
