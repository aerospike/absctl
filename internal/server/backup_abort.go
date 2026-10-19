// Copyright 2026 Aerospike, Inc.
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

package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/aerospike/backup-go"
	"github.com/aerospike/backup-go/pkg/asinfo"
	infomodels "github.com/aerospike/backup-go/pkg/asinfo/models"
)

// msgNoRunningBackup is reported by an abort that found no running backup to abort.
const msgNoRunningBackup = "no running backup found, nothing to abort"

// errAbortStatusTimeout is returned when the cluster accepted an abort request but kept
// reporting the job as running until the wait ran out.
var errAbortStatusTimeout = errors.New("checking backup status after abortion timed out")

// terminalBackupStates are the states of a backup job that has already ended. The cluster
// keeps the state of a finished job for a while after it stops working on it, so a job
// reporting one of these is not running and holds nothing left to abort.
//
// BackupStateUnknown is deliberately not one of them: a state that could not be read says
// nothing about whether the job runs, and refusing the abort on it would leave a running
// backup with no way to stop it.
var terminalBackupStates = map[infomodels.BackupState]bool{
	infomodels.BackupStateComplete: true,
	infomodels.BackupStateFailed:   true,
	infomodels.BackupStateAborted:  true,
}

// AbortBackup asks the cluster to abort the configured backup job, once it has confirmed
// there is a running job to abort, and waits for that job to stop.
func (s *Service) AbortBackup(ctx context.Context) error {
	client, err := s.newInfoClient(ctx)
	if err != nil {
		return err
	}
	defer client.Close()

	return s.abortBackup(ctx, client)
}

// abortBackup requests the abort and then confirms it through the job status.
func (s *Service) abortBackup(ctx context.Context, client backup.ServerBackupInfo) error {
	jobID := s.backupCfg.Abort.JobID

	running, err := s.backupIsRunning(ctx, client, jobID)
	if err != nil {
		return err
	}

	if !running {
		return nil
	}

	// The error the cluster answers with already names the failed abort, so it is
	// returned as it is: wrapping it here only repeats what the command line prints.
	if err := client.AbortBackup(ctx, jobID); err != nil {
		return err
	}

	s.logger.Info("aborting",
		slog.String("backup-id", jobID))

	outcome, err := s.waitForAbort(ctx,
		fmt.Errorf("%w: backup-id %s", errAbortStatusTimeout, jobID),
		func(ctx context.Context) (abortOutcome, string, error) {
			return backupAbortState(ctx, client, jobID)
		})
	if err != nil {
		return err
	}

	// A job that reached the end of the backup before the abort landed was not aborted:
	// its data is in storage, and announcing it as aborted would hide a backup that
	// finished.
	if outcome == abortTooLate {
		s.logger.Info("backup finished before it could be aborted",
			slog.String("backup-id", jobID))

		return nil
	}

	s.logger.Info("backup was successfully aborted")

	return nil
}

// backupAbortState reads the state of a job that was asked to stop, and says what it
// means for the abort.
func backupAbortState(
	ctx context.Context,
	client backup.ServerBackupInfo,
	jobID string,
) (abortOutcome, string, error) {
	status, err := backupStatus(ctx, client, jobID)
	if err != nil {
		// The cluster drops the state of a job that is no longer running.
		if errors.Is(err, asinfo.ErrNotFound) {
			return abortStopped, "", nil
		}

		return 0, "", fmt.Errorf("failed to get backup status: %w", err)
	}

	switch status.State {
	// The job is done winding down once it reports the abort. A job that hit a failure
	// on the way out reports that instead, and is just as stopped.
	case infomodels.BackupStateAborted, infomodels.BackupStateFailed:
		return abortStopped, status.State.Describe(), nil
	case infomodels.BackupStateComplete:
		return abortTooLate, status.State.Describe(), nil
	default:
		return abortPending, status.State.Describe(), nil
	}
}

// backupIsRunning reports whether the cluster is working on jobID, so that an abort is
// only requested when there is a job to abort. The cluster accepts an abort for a job it
// knows nothing about without complaining, and the status that follows such a request
// looks exactly like the status of a job that stopped on request - which would announce a
// backup that never ran, or one that ended hours ago, as successfully aborted.
func (s *Service) backupIsRunning(
	ctx context.Context,
	client backup.ServerBackupInfo,
	jobID string,
) (bool, error) {
	status, err := backupStatus(ctx, client, jobID)
	if err != nil {
		// The cluster reports only the jobs it still holds the state of, so an unknown
		// job id and a backup that finished long ago are answered the same way.
		if errors.Is(err, asinfo.ErrNotFound) {
			s.logger.Info(msgNoRunningBackup, slog.String("backup-id", jobID))

			return false, nil
		}

		return false, fmt.Errorf("failed to get backup status: %w", err)
	}

	// A job that already reached its end is still reported for a while. Naming the state
	// it ended in says why the abort did nothing.
	if terminalBackupStates[status.State] {
		s.logger.Info(msgNoRunningBackup,
			slog.String("backup-id", jobID),
			slog.String("state", status.State.Describe()))

		return false, nil
	}

	return true, nil
}
