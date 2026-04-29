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

package service //nolint:dupl // see newConfigClustersCmd doc.

import (
	"context"
	"log/slog"

	"github.com/aerospike/absctl/internal/flags"
	configService "github.com/aerospike/absctl/internal/service"
	"github.com/spf13/cobra"
)

// Each resource (clusters/policies/routines/storage) wires the same Cobra
// subcommand shape but binds its own typed flag struct, validation, handler
// method, and DTO. Sharing a generic helper would erase compile-time type
// safety on the body fields, which is the whole point of the refactor.
//

func newConfigClustersCmd(rc *runCtx) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "clusters",
		Short: "Manage Aerospike cluster definitions",
	}

	addCommonResourceSubcommands(cmd, rc, &resourceOps{
		resource:       "cluster",
		resourcePlural: "clusters",
		list: func(ctx context.Context, h *configService.ConfigHandler) error {
			return h.ListClusters(ctx)
		},
		show: func(ctx context.Context, h *configService.ConfigHandler, name string) error {
			return h.ReadCluster(ctx, name)
		},
		del: func(ctx context.Context, h *configService.ConfigHandler, name string) error {
			return h.DeleteCluster(ctx, name)
		},
	})

	cmd.AddCommand(
		newConfigClusterAddCmd(rc),
		newConfigClusterUpdateCmd(rc),
	)

	setParentHelp(cmd)

	return cmd
}

//nolint:dupl // add/update commands are intentionally symmetric per resource; see newConfigClustersCmd doc.
func newConfigClusterAddCmd(rc *runCtx) *cobra.Command {
	f := flags.NewServiceConfigCluster()

	cmd := &cobra.Command{
		Use:   "add",
		Short: "Add a new Aerospike cluster definition",
		Long: "Add a new Aerospike cluster definition. Body fields can be supplied " +
			"as individual flags or via --file (a JSON DtoAerospikeCluster body). " +
			"When both are provided, flag values override the corresponding fields " +
			"loaded from the file.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := f.Validate(); err != nil {
				return err
			}

			handler, err := newConfigHandler(rc)
			if err != nil {
				return err
			}

			if err := handler.AddCluster(cmd.Context(), &f.ConfigClusterFields); err != nil {
				return err
			}

			rc.logger.Info("cluster added successfully", slog.String("name", f.Name))

			return nil
		},
	}

	cmd.Flags().AddFlagSet(f.NewFlagSet())
	_ = cmd.MarkFlagRequired("name")
	setLeafHelp(cmd)

	return cmd
}

//nolint:dupl // add/update commands are intentionally symmetric per resource; see newConfigClustersCmd doc.
func newConfigClusterUpdateCmd(rc *runCtx) *cobra.Command {
	f := flags.NewServiceConfigCluster()

	cmd := &cobra.Command{
		Use:   "update",
		Short: "Replace an existing Aerospike cluster definition",
		Long: "Replace an existing Aerospike cluster definition. Body fields can be " +
			"supplied as individual flags or via --file (a JSON DtoAerospikeCluster " +
			"body). When both are provided, flag values override the corresponding " +
			"fields loaded from the file.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := f.Validate(); err != nil {
				return err
			}

			handler, err := newConfigHandler(rc)
			if err != nil {
				return err
			}

			if err := handler.UpdateCluster(cmd.Context(), &f.ConfigClusterFields); err != nil {
				return err
			}

			rc.logger.Info("cluster updated successfully", slog.String("name", f.Name))

			return nil
		},
	}

	cmd.Flags().AddFlagSet(f.NewFlagSet())
	_ = cmd.MarkFlagRequired("name")
	setLeafHelp(cmd)

	return cmd
}
