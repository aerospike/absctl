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
	"time"

	"github.com/aerospike/absctl/internal/logging"
	"github.com/aerospike/absctl/internal/models"
	"github.com/aerospike/backup-go"
	"github.com/aerospike/backup-go/errclass"
	"github.com/aerospike/backup-go/pkg/asinfo"
	infomodels "github.com/aerospike/backup-go/pkg/asinfo/models"
	servermodels "github.com/aerospike/backup-go/pkg/server/lister/models"
)

const (
	// midBackupGapConfirmations bounds the wait vanishedJobConfirmations bounds, for a
	// job that stopped reporting before it reached its last stages. Such a gap cannot be a completion, so the job
	// is given a much longer benefit of the doubt before the watch gives up on it.
	midBackupGapConfirmations = 40
	// statusGapLogEvery paces the reminders logged while a job is missing from the
	// cluster status, so that a long wait can be followed in a debug log without a
	// line per lookup.
	statusGapLogEvery = 20

	// msgBackupComplete announces a finished backup, whether the completion came from
	// the job status or from the manifest of a job the cluster no longer reports.
	msgBackupComplete = "backup complete"

	// msgBackupProgress is the message every progress line of a backup is logged under.
	msgBackupProgress = "backup progress"
)

var (
	// errBackupNotFound is returned when the cluster does not report the requested job
	// at all: the id names no backup the cluster is working on, and none whose state it
	// still holds.
	errBackupNotFound = errors.New("no backup found")

	// errEmptyBackupStatus stands for a status lookup that came back with neither a
	// status nor a failure. It wraps asinfo.ErrNotFound because an empty answer says
	// exactly as much about the job as one that did not find it, and every caller
	// already knows what to do with a job the cluster does not report.
	errEmptyBackupStatus = fmt.Errorf("empty backup status: %w", asinfo.ErrNotFound)

	// errBackupFailed is returned when the cluster reports a failed backup job.
	errBackupFailed = errors.New("backup failed")

	// errBackupAborted is returned when the cluster reports a backup job that was
	// aborted: the job stopped on request and left no usable backup behind.
	errBackupAborted = errors.New("backup aborted")

	// errBackupStatusLost is returned when the cluster stopped reporting a job in the
	// middle of a backup and left no manifest behind.
	errBackupStatusLost = errors.New("cluster stopped reporting backup status")
)

// completionStates are the stages a backup job can be in when the cluster drops its
// status because the job is done: the state of a finished job is kept for a while and
// then discarded. A job that stops reporting from one of these stages has completed.
// A job that stops reporting from an earlier stage has not - its status is either
// temporarily unavailable or the job is gone for good - and treating that as a finished
// backup makes the watcher announce a success in the middle of a backup.
var completionStates = map[infomodels.BackupState]bool{
	infomodels.BackupStateStoppingChangeStream: true,
	infomodels.BackupStateFinalDraining:        true,
	infomodels.BackupStateCommitting:           true,
	infomodels.BackupStateComplete:             true,
}

// BackupProgress returns the progress of the currently running backup.
func (s *Service) BackupProgress(ctx context.Context) error {
	client, err := s.newInfoClient(ctx)
	if err != nil {
		return err
	}
	defer client.Close()

	// The storage client is created before the watch loop on purpose: a wrong endpoint
	// or wrong credentials must fail now, not after hours of watching a backup.
	l, err := s.newMetadataLister(ctx, models.NormalizeS3Prefix(s.backupCfg.Progress.Path))
	if err != nil {
		return err
	}

	mode := reportOnce
	if s.backupCfg.Progress.Watch {
		mode = followJob
	}

	return s.reportBackupProgress(ctx, client, l, s.backupCfg.Progress.JobID, mode)
}

// reportBackupProgress polls the cluster for the status of jobID and logs it until the
// backup reaches a terminal state, or once in reportOnce mode. The job is named by the
// caller rather than taken from the configuration, because the job that "backup start"
// follows is the one the cluster just handed it and not one named on the command line.
//
// A job named on the command line that the cluster does not report ends the watch right
// away: the id names no job the cluster holds, and there is nothing to wait for. The job
// "backup start" follows is different - the cluster handed out its id, so it does exist -
// and is given startedJobConfirmations lookups to turn up in the status.
//
// Only a job that was reported at least once can be missing for a reason worth waiting
// out - see jobVanished.
func (s *Service) reportBackupProgress(
	ctx context.Context,
	client backup.ServerBackupInfo,
	l metadataGetter,
	jobID string,
	mode progressMode,
) error {
	printer := newProgressPrinter(s.logger, msgBackupProgress)

	// lastState is the state the cluster reported for this job most recently. It stays
	// empty while the job has never been observed, and it decides what a job that stops
	// reporting means.
	var lastState infomodels.BackupState
	// misses counts the lookups in a row that did not find the job.
	var misses int

	for {
		status, err := backupStatus(ctx, client, jobID)

		switch {
		case err != nil && !errors.Is(err, asinfo.ErrNotFound):
			return fmt.Errorf("failed to get backup status: %w", err)
		case err != nil:
			misses++

			switch {
			case lastState == "" && !startingJobPending(mode, misses):
				return fmt.Errorf("%w: backup-id %s", errBackupNotFound, jobID)
			case lastState != "" && jobVanished(lastState, misses):
				return s.reportMissingBackup(ctx, l, jobID, lastState)
			}

			s.logStatusGap(jobID, lastState, misses)

			if err := waitFor(ctx, s.vanishedPollInterval); err != nil {
				return err
			}

			continue
		}

		lastState = status.State
		misses = 0

		// The clock is read after the lookup so that the speed and the estimate are
		// measured against the status just read: a lookup that took seconds would
		// otherwise stretch the interval the sample is divided by.
		now := time.Now()
		snap := backupSnapshot(status)

		printer.Print(snap, now)

		// BackupStateAborting is not terminal: the job is still winding down and keeps
		// reporting until it reaches BackupStateAborted or leaves the cluster status.
		switch status.State {
		case infomodels.BackupStateComplete:
			s.logBackupComplete(status)

			return s.printMetadata(ctx, l, jobID)
		case infomodels.BackupStateFailed:
			return terminalStateError(errBackupFailed, jobID, status.ErrorReason)
		case infomodels.BackupStateAborted:
			return terminalStateError(errBackupAborted, jobID, status.ErrorReason)
		}

		if mode == reportOnce {
			return nil
		}

		if err := waitFor(ctx, nextPoll(snap, now)); err != nil {
			return err
		}
	}
}

// logBackupComplete announces a backup the cluster reported as finished, and says how
// long the job took when the cluster reported both ends of it.
func (s *Service) logBackupComplete(status *infomodels.ResponseBackupState) {
	attrs := []any{slog.String("backup-id", status.JobID)}

	if duration, ok := jobDuration(status.StartTime, status.FinishTime); ok {
		attrs = append(attrs, slog.String("duration", duration.Round(time.Second).String()))
	}

	s.logger.Info(msgBackupComplete, attrs...)
}

// terminalStateError names the job that ended in a terminal state, along with the reason
// the cluster gave for it when there was one.
func terminalStateError(err error, jobID, errorReason string) error {
	if errorReason == "" {
		return fmt.Errorf("%w: backup-id %s", err, jobID)
	}

	return fmt.Errorf("%w: backup-id %s, reason %q", err, jobID, errorReason)
}

// jobVanished reports whether a job that was reported before and is missing now has been
// missing long enough to be accepted as gone. It has to be missing several times in a
// row, so that a momentary gap in the cluster status is not mistaken for a terminal
// outcome, and a job that was last seen before it reached its last stages gets the longer
// of the two waits: its status is the only thing that can end the watch.
func jobVanished(lastState infomodels.BackupState, misses int) bool {
	if completionStates[lastState] {
		return misses >= vanishedJobConfirmations
	}

	return misses >= midBackupGapConfirmations
}

// logStatusGap reports a job that is temporarily missing from the cluster status. A gap
// is a normal part of watching a backup - the status is unavailable between stages, or
// while a node is busy - so none of it reaches the user: both lines are logged on the
// debug level, where a gap that outlives the routine confirmations says so, and is
// repeated while it lasts.
func (s *Service) logStatusGap(jobID string, lastState infomodels.BackupState, misses int) {
	attrs := []any{
		slog.String("backup-id", jobID),
		slog.Int("attempt", misses),
	}

	if lastState != "" {
		attrs = append(attrs, slog.String("last-state", lastState.Describe()))
	}

	if misses == vanishedJobConfirmations || misses%statusGapLogEvery == 0 {
		s.logger.Debug("backup status is unavailable, still waiting for the job", attrs...)

		return
	}

	s.logger.Debug("backup status is temporarily unavailable", attrs...)
}

// backupStatus reads the status of jobID, and reports an answer that carries no status at
// all as a job the cluster does not report. The status is dereferenced by every caller,
// so the one place that can hand out a nil is here.
func backupStatus(
	ctx context.Context,
	client backup.ServerBackupInfo,
	jobID string,
) (*infomodels.ResponseBackupState, error) {
	status, err := client.GetBackupStatus(ctx, jobID)
	if err != nil {
		return nil, err
	}

	if status == nil {
		return nil, errEmptyBackupStatus
	}

	return status, nil
}

// reportMissingBackup handles a job that was being watched and that the cluster no longer
// reports, once the caller has confirmed that it is really gone. The cluster drops the
// state of a job shortly after finishing it, so a job last seen in one of the
// completionStates has completed, and its manifest is worth waiting for. Anything else
// stopped reporting in the middle of a backup, and only a manifest can turn that into a
// completion.
func (s *Service) reportMissingBackup(
	ctx context.Context,
	l metadataGetter,
	jobID string,
	lastState infomodels.BackupState,
) error {
	if completionStates[lastState] {
		s.logger.Info(msgBackupComplete)

		return s.printMetadata(ctx, l, jobID)
	}

	// The watch may have been between polls while the job ran through its last stages,
	// in which case the backup is done and the manifest is already in place. A missing
	// manifest means the backup did not finish, and reporting it as complete would hide
	// a job that died, was aborted, or fell out of the cluster status for good. Any
	// other failure is about the storage and is reported as itself.
	md, err := s.fetchMetadata(ctx, l, jobID, false)

	switch {
	case err == nil:
		s.logger.Info(msgBackupComplete)

		return s.printMetadataEntry(md)
	case errors.Is(err, errclass.ErrNotFound):
		return fmt.Errorf("%w: backup-id %s, last reported state %q",
			errBackupStatusLost, jobID, lastState.Describe())
	default:
		return err
	}
}

// printMetadata reads the manifest of a finished backup and prints it. The manifest is
// waited for: every caller has seen the job reach its end, and the server uploads the
// manifest shortly after that.
func (s *Service) printMetadata(ctx context.Context, l metadataGetter, jobID string) error {
	md, err := s.fetchMetadata(ctx, l, jobID, true)
	if err != nil {
		return err
	}

	return s.printMetadataEntry(md)
}

// printMetadataEntry prints a single backup manifest.
func (s *Service) printMetadataEntry(md servermodels.Metadata) error {
	return logging.PrintMetadata([]servermodels.Metadata{md}, s.backupCfg.App.LogJSON, s.logger)
}

// fetchMetadata reads the backup manifest from object storage. Only a missing manifest is
// retried, and only when wait is set: every other failure is permanent, and polling for a
// backup that never existed would just hang the command.
func (s *Service) fetchMetadata(
	ctx context.Context,
	l metadataGetter,
	jobID string,
	wait bool,
) (servermodels.Metadata, error) {
	if !wait {
		md, err := l.GetMetadata(ctx, jobID)
		if err != nil {
			return servermodels.Metadata{}, fmt.Errorf("failed to get backup metadata: %w", err)
		}

		return md, nil
	}

	var (
		md      servermodels.Metadata
		permErr error
	)

	retryErr := s.metadataRetryPolicy.Do(ctx, func() error {
		var err error

		md, err = l.GetMetadata(ctx, jobID)

		switch {
		case err == nil:
			return nil
		case errors.Is(err, errclass.ErrNotFound):
			s.logger.Debug("backup metadata is not uploaded yet", slog.String("backup-id", jobID))

			return err
		default:
			permErr = err

			return nil
		}
	})

	switch {
	case permErr != nil:
		return servermodels.Metadata{}, fmt.Errorf("failed to get backup metadata: %w", permErr)
	case retryErr != nil:
		return servermodels.Metadata{}, fmt.Errorf("timed out waiting for backup metadata: %w", retryErr)
	}

	return md, nil
}

// backupSnapshot describes the progress of a backup job.
func backupSnapshot(status *infomodels.ResponseBackupState) *progressSnapshot {
	return &progressSnapshot{
		attrs: []any{
			slog.String("backup-id", status.JobID),
			slog.String("state", status.State.Describe()),
		},
		stage:      string(status.State),
		ratio:      status.ProgressPct / percentScale,
		records:    status.RecsBackedUp,
		hasRecords: true,
		startTime:  status.StartTime,
	}
}
