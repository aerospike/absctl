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
	"log/slog"
	"time"

	"github.com/aerospike/backup-go"
	infomodels "github.com/aerospike/backup-go/pkg/asinfo/models"
)

// msgNoRunningBackup is reported by an abort that found no running backup to abort.
const msgNoRunningBackup = "no running backup found, nothing to abort"

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
// there is a running job to abort, and waits for that job to stop. With Async set it
// returns as soon as the cluster accepts the abort.
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

	if s.backupCfg.Abort.Async {
		return nil
	}

	outcome, err := s.waitForBackupAbort(ctx, client, jobID)
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

// waitForBackupAbort follows the job that was asked to stop until it does, and reports
// how it stopped. The cluster accepts an abort request before the job actually winds
// down, so the abort is confirmed by the status that follows it.
//
// The job is followed the same way "backup start" follows the job it started: it is
// given startedJobConfirmations lookups to turn up in the status, and once reported, it
// has to be missed vanishedJobConfirmations times in a row before it is accepted as gone.
// The cluster drops the state of a job that is no longer running, so a job gone from the
// status has stopped.
func (s *Service) waitForBackupAbort(
	ctx context.Context,
	client backup.ServerBackupInfo,
	jobID string,
) (abortOutcome, error) {
	var outcome abortOutcome

	w := s.newBackupWatch(client, jobID, startedJobConfirmations,
		func(context.Context, *infomodels.ResponseBackupState, bool) error {
			outcome = abortStopped

			return nil
		})

	w.handle = func(_ context.Context, status *infomodels.ResponseBackupState) (bool, time.Duration, error) {
		outcome = backupAbortOutcome(status.State)

		return outcome != abortPending, s.statePollInterval, nil
	}

	if err := w.run(ctx); err != nil {
		return 0, err
	}

	return outcome, nil
}

// backupAbortOutcome says what the state of a job that was asked to stop means for the
// abort.
func backupAbortOutcome(state infomodels.BackupState) abortOutcome {
	switch state {
	// The job is done winding down once it reports the abort. A job that hit a failure
	// on the way out reports that instead, and is just as stopped.
	case infomodels.BackupStateAborted, infomodels.BackupStateFailed:
		return abortStopped
	case infomodels.BackupStateComplete:
		return abortTooLate
	default:
		return abortPending
	}
}

// backupIsRunning reports whether the cluster is working on jobID, so that an abort is
// only requested when there is a job to abort. The cluster accepts an abort for a job it
// knows nothing about without complaining, and the status that follows such a request
// looks exactly like the status of a job that stopped on request - which would announce a
// backup that never ran, or one that ended hours ago, as successfully aborted.
//
// The merged status misses the job as soon as a single node does not report it, so the
// job is given startedJobConfirmations lookups to turn up before it is taken as missing.
func (s *Service) backupIsRunning(
	ctx context.Context,
	client backup.ServerBackupInfo,
	jobID string,
) (bool, error) {
	var status *infomodels.ResponseBackupState

	// A job that never turns up leaves status nil, which is answered below.
	w := s.newBackupWatch(client, jobID, startedJobConfirmations,
		func(context.Context, *infomodels.ResponseBackupState, bool) error {
			return nil
		})

	w.handle = func(_ context.Context, st *infomodels.ResponseBackupState) (bool, time.Duration, error) {
		status = st

		return true, 0, nil
	}

	if err := w.run(ctx); err != nil {
		return false, err
	}

	// The cluster reports only the jobs it still holds the state of, so an unknown job id
	// and a backup that finished long ago are answered the same way.
	if status == nil {
		s.logger.Info(msgNoRunningBackup, slog.String("backup-id", jobID))

		return false, nil
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
