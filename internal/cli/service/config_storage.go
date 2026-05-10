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

func newConfigStorageCmd(rc *runCtx) *cobra.Command {
	cmd := &cobra.Command{
		Use:   useStorage,
		Short: "Manage storage definitions",
	}

	addCommonResourceSubcommands(cmd, rc, &resourceOps{
		resource:       useStorage,
		resourcePlural: useStorage,
		list: func(ctx context.Context, h *configService.ConfigHandler) error {
			return h.ListStorage(ctx)
		},
		show: func(ctx context.Context, h *configService.ConfigHandler, name string) error {
			return h.ReadStorage(ctx, name)
		},
		del: func(ctx context.Context, h *configService.ConfigHandler, name string) error {
			return h.DeleteStorage(ctx, name)
		},
	})

	cmd.AddCommand(
		newConfigStorageAddCmd(rc),
		newConfigStorageUpdateCmd(rc),
	)

	setParentHelp(cmd)

	return cmd
}

//nolint:dupl // add/update commands are intentionally symmetric per resource; see newConfigClustersCmd doc.
func newConfigStorageAddCmd(rc *runCtx) *cobra.Command {
	f := flags.NewServiceConfigStorage()

	cmd := &cobra.Command{
		Use:   useAdd,
		Short: "Add a new storage definition",
		Long: "Add a new storage definition. Storage wraps exactly one of " +
			"local/s3/gcp/azure provider blocks; populating any field of a provider " +
			"group selects that provider. Body fields can be supplied as individual " +
			"flags.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := f.Validate(); err != nil {
				return err
			}

			handler, err := newConfigHandler(rc)
			if err != nil {
				return err
			}

			if err := handler.AddStorage(cmd.Context(), &f.ConfigStorageFields); err != nil {
				return err
			}

			rc.logger.Info("storage added successfully", slog.String("name", f.Name))

			return nil
		},
	}

	cmd.Flags().AddFlagSet(f.NewFlagSet())
	_ = cmd.MarkFlagRequired("name")
	setLeafHelp(cmd)

	return cmd
}

//nolint:dupl // add/update commands are intentionally symmetric per resource; see newConfigClustersCmd doc.
func newConfigStorageUpdateCmd(rc *runCtx) *cobra.Command {
	f := flags.NewServiceConfigStorage()

	cmd := &cobra.Command{
		Use:   useUpdate,
		Short: "Replace an existing storage definition",
		Long: "Replace an existing storage definition. Storage wraps exactly one of " +
			"local/s3/gcp/azure provider blocks; populating any field of a provider " +
			"group selects that provider. Body fields can be supplied as individual " +
			"flags.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := f.Validate(); err != nil {
				return err
			}

			handler, err := newConfigHandler(rc)
			if err != nil {
				return err
			}

			if err := handler.UpdateStorage(cmd.Context(), &f.ConfigStorageFields); err != nil {
				return err
			}

			rc.logger.Info("storage updated successfully", slog.String("name", f.Name))

			return nil
		},
	}

	cmd.Flags().AddFlagSet(f.NewFlagSet())
	_ = cmd.MarkFlagRequired("name")
	setLeafHelp(cmd)

	return cmd
}
