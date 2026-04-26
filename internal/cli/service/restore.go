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
	restoreService "github.com/aerospike/absctl/internal/service"
	"github.com/spf13/cobra"
)

func newRestoreCmd(rc *runCtx) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "restore",
		Short: "Manage restores on the Aerospike Backup Service",
	}

	cmd.AddCommand(
		newRestoreCancelCmd(rc),
		newRestoreStatusCmd(rc),
		newRestoreJobsCmd(rc),
		newRestoreFullCmd(rc),
		newRestoreIncrementalCmd(rc),
		newRestoreTimestampCmd(rc),
	)

	setParentHelp(cmd)

	return cmd
}

func newRestoreCancelCmd(rc *runCtx) *cobra.Command {
	f := flags.NewServiceRestoreJobID()

	cmd := &cobra.Command{
		Use:   "cancel",
		Short: "Cancel a running restore operation",
		RunE: func(cmd *cobra.Command, _ []string) error {
			handler, err := newRestoreHandler(rc.conn)
			if err != nil {
				return err
			}

			if err := handler.Cancel(cmd.Context(), f.JobID); err != nil {
				return err
			}

			rc.logger.Info("restore canceled successfully", slog.Int64("jobId", f.JobID))

			return nil
		},
	}

	cmd.Flags().AddFlagSet(f.NewFlagSet())
	_ = cmd.MarkFlagRequired("job-id")
	setLeafHelp(cmd)

	return cmd
}

func newRestoreStatusCmd(rc *runCtx) *cobra.Command {
	f := flags.NewServiceRestoreJobID()

	cmd := &cobra.Command{
		Use:   "status",
		Short: "Get the status of a restore job",
		RunE: func(cmd *cobra.Command, _ []string) error {
			handler, err := newRestoreHandler(rc.conn)
			if err != nil {
				return err
			}

			data, err := handler.Status(cmd.Context(), f.JobID)
			if err != nil {
				return err
			}

			rc.logger.Info("restore status",
				slog.Int64("jobId", f.JobID),
				slog.Any("status", data),
			)

			return nil
		},
	}

	cmd.Flags().AddFlagSet(f.NewFlagSet())
	_ = cmd.MarkFlagRequired("job-id")
	setLeafHelp(cmd)

	return cmd
}

func newRestoreJobsCmd(rc *runCtx) *cobra.Command {
	f := flags.NewServiceRestoreJobs()

	cmd := &cobra.Command{
		Use:   "jobs",
		Short: "List restore jobs",
		RunE: func(cmd *cobra.Command, _ []string) error {
			handler, err := newRestoreHandler(rc.conn)
			if err != nil {
				return err
			}

			data, err := handler.ListJobs(cmd.Context(), f.From, f.To, f.Status)
			if err != nil {
				return err
			}

			rc.logger.Info("restore jobs", slog.Any("jobs", data))

			return nil
		},
	}

	cmd.Flags().AddFlagSet(f.NewFlagSet())
	setLeafHelp(cmd)

	return cmd
}

// newRestoreHandler validates the service connection and creates a RestoreHandler.
func newRestoreHandler(connFlags *flags.ServiceConnection) (*restoreService.RestoreHandler, error) {
	conn := connFlags.GetServiceConnection()
	if err := conn.Validate(); err != nil {
		return nil, err
	}

	return restoreService.NewRestoreHandler(conn.ServerURL())
}
