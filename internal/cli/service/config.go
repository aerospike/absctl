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
	configService "github.com/aerospike/absctl/internal/service"
	"github.com/spf13/cobra"
)

func newConfigCmd(rc *runCtx) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "config",
		Short: "Manage Aerospike Backup Service configuration",
	}

	cmd.AddCommand(
		newConfigShowCmd(rc),
		newConfigUpdateCmd(rc),
		newConfigApplyCmd(rc),
		newConfigClustersCmd(rc),
		newConfigPoliciesCmd(rc),
		newConfigRoutinesCmd(rc),
		newConfigStorageCmd(rc),
	)

	setParentHelp(cmd)

	return cmd
}

func newConfigShowCmd(rc *runCtx) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "show",
		Short: "Show the full service configuration",
		RunE: func(cmd *cobra.Command, _ []string) error {
			handler, err := newConfigHandler(rc)
			if err != nil {
				return err
			}

			return handler.Read(cmd.Context())
		},
	}

	setLeafHelp(cmd)

	return cmd
}

func newConfigUpdateCmd(rc *runCtx) *cobra.Command {
	f := flags.NewServiceConfigFile()

	cmd := &cobra.Command{
		Use:   "update",
		Short: "Replace the full service configuration from a JSON file",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := f.Validate(); err != nil {
				return err
			}

			handler, err := newConfigHandler(rc)
			if err != nil {
				return err
			}

			if err := handler.Update(cmd.Context(), f.File); err != nil {
				return err
			}

			rc.logger.Info("config updated successfully", slog.String("file", f.File))

			return nil
		},
	}

	cmd.Flags().AddFlagSet(f.NewFlagSet())
	_ = cmd.MarkFlagRequired("file")
	setLeafHelp(cmd)

	return cmd
}

func newConfigApplyCmd(rc *runCtx) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "apply",
		Short: "Apply the pending configuration on the service",
		RunE: func(cmd *cobra.Command, _ []string) error {
			handler, err := newConfigHandler(rc)
			if err != nil {
				return err
			}

			if err := handler.Apply(cmd.Context()); err != nil {
				return err
			}

			rc.logger.Info("config applied successfully")

			return nil
		},
	}

	setLeafHelp(cmd)

	return cmd
}

// newConfigHandler validates the service connection and creates a
// ConfigHandler configured with the shared logger and JSON output flag.
func newConfigHandler(rc *runCtx) (*configService.ConfigHandler, error) {
	conn := rc.conn.GetServiceConnection()
	if err := conn.Validate(); err != nil {
		return nil, err
	}

	return configService.NewConfigHandler(conn.ServerURL(), rc.logger, rc.app.GetApp().LogJSON)
}
