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

// resourceOps groups the resource-agnostic list/show/delete handler methods
// so the generic CLI builder can produce these subcommands without
// duplication. Per-resource add/update commands are defined alongside each
// resource because they bind a resource-specific flag struct.
//
// Note: list and show no longer return data because the handler now prints
// the result through the logging package directly.
type resourceOps struct {
	// resource is a singular human-readable name used in command short
	// descriptions and log messages (e.g. "cluster", "policy", "routine",
	// "storage").
	resource string

	// resourcePlural is used in list-related help text (e.g. "clusters",
	// "policies").
	resourcePlural string

	list func(ctx context.Context, h *configService.ConfigHandler) error
	show func(ctx context.Context, h *configService.ConfigHandler, name string) error
	del  func(ctx context.Context, h *configService.ConfigHandler, name string) error
}

// addCommonResourceSubcommands attaches list/show/delete subcommands to parent.
// Each resource separately attaches its own add/update commands so they can
// bind a dedicated flag struct exposing every body field as an individual flag.
func addCommonResourceSubcommands(parent *cobra.Command, rc *runCtx, ops *resourceOps) {
	parent.AddCommand(
		newResourceListCmd(rc, ops),
		newResourceShowCmd(rc, ops),
		newResourceDeleteCmd(rc, ops),
	)
}

func newResourceListCmd(rc *runCtx, ops *resourceOps) *cobra.Command {
	cmd := &cobra.Command{
		Use:   useList,
		Short: "List configured " + ops.resourcePlural,
		RunE: func(cmd *cobra.Command, _ []string) error {
			handler, err := newConfigHandler(rc)
			if err != nil {
				return err
			}

			return ops.list(cmd.Context(), handler)
		},
	}

	setLeafHelp(cmd)

	return cmd
}

func newResourceShowCmd(rc *runCtx, ops *resourceOps) *cobra.Command {
	f := flags.NewServiceConfigName()

	cmd := &cobra.Command{
		Use:   useShow,
		Short: "Show a single " + ops.resource + " by name",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := f.Validate(); err != nil {
				return err
			}

			handler, err := newConfigHandler(rc)
			if err != nil {
				return err
			}

			return ops.show(cmd.Context(), handler, f.Name)
		},
	}

	cmd.Flags().AddFlagSet(f.NewFlagSet())
	_ = cmd.MarkFlagRequired("name")
	setLeafHelp(cmd)

	return cmd
}

func newResourceDeleteCmd(rc *runCtx, ops *resourceOps) *cobra.Command {
	f := flags.NewServiceConfigName()

	cmd := &cobra.Command{
		Use:   useDelete,
		Short: "Delete a " + ops.resource + " by name",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := f.Validate(); err != nil {
				return err
			}

			handler, err := newConfigHandler(rc)
			if err != nil {
				return err
			}

			if err := ops.del(cmd.Context(), handler, f.Name); err != nil {
				return err
			}

			rc.logger.Info(ops.resource+" deleted successfully", slog.String("name", f.Name))

			return nil
		},
	}

	cmd.Flags().AddFlagSet(f.NewFlagSet())
	_ = cmd.MarkFlagRequired("name")
	setLeafHelp(cmd)

	return cmd
}
