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

//nolint:dupl // Full, incremental, timestamp restore looks the same, but it should be different sub commands.
package service

import (
	"log/slog"

	"github.com/aerospike/absctl/internal/flags"
	"github.com/spf13/cobra"
)

func newRestoreTimestampCmd(rc *runCtx) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "timestamp",
		Short: "Manage point-in-time restore operations",
	}

	cmd.AddCommand(
		newRestoreTimestampStartCmd(rc),
	)

	setParentHelp(cmd)

	return cmd
}

func newRestoreTimestampStartCmd(rc *runCtx) *cobra.Command {
	f := flags.NewServiceRestoreTimestamp()

	cmd := &cobra.Command{
		Use:   "start",
		Short: "Trigger an asynchronous restore operation to a specific point in time",
		RunE: func(cmd *cobra.Command, _ []string) error {
			handler, err := newRestoreHandler(rc.conn)
			if err != nil {
				return err
			}

			jobID, err := handler.RestoreTimestamp(cmd.Context(), &f.RestoreTimestampRequest)
			if err != nil {
				return err
			}

			rc.logger.Info("timestamp restore started successfully", slog.String("jobId", jobID))

			return nil
		},
	}

	cmd.Flags().AddFlagSet(f.NewFlagSet())
	setLeafHelp(cmd)

	return cmd
}
