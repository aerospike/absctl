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

func newConfigPoliciesCmd(rc *runCtx) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "policies",
		Short: "Manage backup policy definitions",
	}

	addCommonResourceSubcommands(cmd, rc, &resourceOps{
		resource:       "policy",
		resourcePlural: "policies",
		list: func(ctx context.Context, h *configService.ConfigHandler) error {
			return h.ListPolicies(ctx)
		},
		show: func(ctx context.Context, h *configService.ConfigHandler, name string) error {
			return h.ReadPolicy(ctx, name)
		},
		del: func(ctx context.Context, h *configService.ConfigHandler, name string) error {
			return h.DeletePolicy(ctx, name)
		},
	})

	cmd.AddCommand(
		newConfigPolicyAddCmd(rc),
		newConfigPolicyUpdateCmd(rc),
	)

	setParentHelp(cmd)

	return cmd
}

func newConfigPolicyAddCmd(rc *runCtx) *cobra.Command {
	f := flags.NewServiceConfigPolicy()

	cmd := &cobra.Command{
		Use:   "add",
		Short: "Add a new backup policy definition",
		Long: "Add a new backup policy definition. Body fields can be supplied as " +
			"individual flags.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := f.Validate(); err != nil {
				return err
			}

			handler, err := newConfigHandler(rc)
			if err != nil {
				return err
			}

			if err := handler.AddPolicy(cmd.Context(), &f.ConfigPolicyFields); err != nil {
				return err
			}

			rc.logger.Info("policy added successfully", slog.String("name", f.Name))

			return nil
		},
	}

	cmd.Flags().AddFlagSet(f.NewFlagSet())
	_ = cmd.MarkFlagRequired("name")
	setLeafHelp(cmd)

	return cmd
}

func newConfigPolicyUpdateCmd(rc *runCtx) *cobra.Command {
	f := flags.NewServiceConfigPolicy()

	cmd := &cobra.Command{
		Use:   "update",
		Short: "Replace an existing backup policy definition",
		Long: "Replace an existing backup policy definition. Body fields can be " +
			"supplied as individual flags.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := f.Validate(); err != nil {
				return err
			}

			handler, err := newConfigHandler(rc)
			if err != nil {
				return err
			}

			if err := handler.UpdatePolicy(cmd.Context(), &f.ConfigPolicyFields); err != nil {
				return err
			}

			rc.logger.Info("policy updated successfully", slog.String("name", f.Name))

			return nil
		},
	}

	cmd.Flags().AddFlagSet(f.NewFlagSet())
	_ = cmd.MarkFlagRequired("name")
	setLeafHelp(cmd)

	return cmd
}
