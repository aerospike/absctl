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

	"github.com/aerospike/backup-go/pkg/asinfo"
	infomodels "github.com/aerospike/backup-go/pkg/asinfo/models"
)

const (
	// msgRestoreComplete announces a finished restore.
	msgRestoreComplete = "restore complete"

	// msgOpenHint and msgAbortOpenHint tell how to release the namespace a cold restore
	// that did not complete keeps closed to clients. A failed job is still running until
	// it is aborted, and only an aborted one can be released.
	msgOpenHint = "the namespace stays closed to clients until it is released " +
		"with \"snapshot-restore open\""
	msgAbortOpenHint = "the namespace stays closed to clients: abort the restore with " +
		"\"snapshot-restore abort\", then release the namespace with \"snapshot-restore open\""

	// partitionCount is the number of partitions of a namespace, which the restore
	// coverage counters are measured against.
	partitionCount = 4096

	// msgRestoreProgress is the message every progress line of a restore is logged
	// under.
	msgRestoreProgress = "restore progress"
)

var (
	// errRestoreNotFound is returned when the cluster does not report the requested
	// restore job at all.
	errRestoreNotFound = errors.New("no restore found")

	// errEmptyRestoreStatus is the restore counterpart of errEmptyBackupStatus.
	errEmptyRestoreStatus = fmt.Errorf("empty restore status: %w", asinfo.ErrNotFound)

	// errRestoreFailed is returned when the cluster reports a failed restore job.
	errRestoreFailed = errors.New("restore failed")

	// errRestoreAborted is returned when the cluster reports an aborted restore job.
	errRestoreAborted = errors.New("restore aborted")

	// errRestoreStatusLost is returned when the cluster stopped reporting a restore job
	// that was being watched.
	errRestoreStatusLost = errors.New("cluster stopped reporting restore status")
)

// RestoreProgress reports the progress of the configured restore job, once or, with
// Watch set, until the restore reaches a terminal state.
func (s *Service) RestoreProgress(ctx context.Context) error {
	client, err := s.newInfoClient(ctx)
	if err != nil {
		return err
	}
	defer client.Close()

	return s.restoreProgress(ctx, client, client.activeNodes)
}

// restoreProgress reports the progress of the configured restore job in the mode the
// configuration asks for. layoutNodes is passed on to reportRestoreProgress.
func (s *Service) restoreProgress(ctx context.Context, client restoreStatusGetter, layoutNodes func() int) error {
	progress := s.restoreCfg.Progress

	mode := reportOnce
	if progress.Watch {
		mode = followJob
	}

	return s.reportRestoreProgress(ctx, client, layoutNodes, progress.Namespace, progress.JobID, mode)
}

// restoreStatusGetter is the part of the info client that reads the status of a restore.
type restoreStatusGetter interface {
	GetRestoreStatus(ctx context.Context, namespace, jobID string) (*infomodels.ResponseRestoreState, error)
}

// restoreStatus reads the status of the restore job jobID of namespace, and reports an
// answer that carries no status at all as a job the cluster does not report.
func restoreStatus(
	ctx context.Context,
	client restoreStatusGetter,
	namespace, jobID string,
) (*infomodels.ResponseRestoreState, error) {
	status, err := client.GetRestoreStatus(ctx, namespace, jobID)
	if err != nil {
		return nil, err
	}

	if status == nil {
		return nil, errEmptyRestoreStatus
	}

	return status, nil
}

// reportRestoreProgress polls the cluster for the status of the restore job jobID of
// namespace and logs it until the restore reaches a terminal state, or once in
// reportOnce mode. layoutNodes returns the number of nodes in the cluster, which the
// progress of a cold restore is measured against.
//
// A job is missing from the status the same way as a backup job is, see
// reportBackupProgress, except that the cluster keeps the state of a finished restore
// job, so a job that stops reporting has not completed.
func (s *Service) reportRestoreProgress(
	ctx context.Context,
	client restoreStatusGetter,
	layoutNodes func() int,
	namespace, jobID string,
	mode progressMode,
) error {
	printer := newProgressPrinter(s.logger, msgRestoreProgress)

	var (
		// seen is set once the cluster reported the job.
		seen bool
		// misses counts the lookups in a row that did not find the job.
		misses int
	)

	for {
		status, err := restoreStatus(ctx, client, namespace, jobID)

		switch {
		case err != nil && !errors.Is(err, asinfo.ErrNotFound):
			return fmt.Errorf("failed to get restore status: %w", err)
		case err != nil:
			misses++

			switch {
			case !seen && !startingJobPending(mode, misses):
				return fmt.Errorf("%w: namespace %s, job-id %s", errRestoreNotFound, namespace, jobID)
			case seen && misses >= vanishedJobConfirmations:
				return fmt.Errorf("%w: namespace %s, job-id %s", errRestoreStatusLost, namespace, jobID)
			}

			s.logger.Debug("restore status is temporarily unavailable",
				slog.String("job-id", jobID),
				slog.Int("attempt", misses))

			if err := waitFor(ctx, s.vanishedPollInterval); err != nil {
				return err
			}

			continue
		}

		seen = true
		misses = 0

		now := time.Now()
		// The nodes of the layout that left the cluster are not active, and still own
		// a share of the restore. A node that joined after the layout was frozen owns
		// none, but the merged status cannot tell how many did, so it is counted too:
		// the estimate then stays below 100% until the job completes.
		snap := restoreSnapshot(status, layoutNodes()+int(status.MissingLayoutNodes))

		printer.Print(snap, now)

		switch status.State {
		case infomodels.RestoreStateComplete:
			s.logRestoreComplete(status)

			return nil
		case infomodels.RestoreStateFailed:
			if status.Kind == infomodels.RestoreKindCold {
				s.logger.Warn(msgAbortOpenHint,
					slog.String("namespace", namespace),
					slog.String("job-id", jobID))
			}

			return restoreTerminalError(errRestoreFailed, namespace, jobID, status.ErrorReason)
		case infomodels.RestoreStateAborted:
			if status.Kind == infomodels.RestoreKindCold {
				s.logOpenHint(namespace, jobID)
			}

			return restoreTerminalError(errRestoreAborted, namespace, jobID, status.ErrorReason)
		}

		if mode == reportOnce {
			return nil
		}

		if err := waitFor(ctx, nextPoll(snap, now)); err != nil {
			return err
		}
	}
}

// logRestoreComplete announces a restore the cluster reported as finished, and says how
// long the job took when the cluster reported both ends of it.
func (s *Service) logRestoreComplete(status *infomodels.ResponseRestoreState) {
	attrs := []any{slog.String("job-id", status.JobID)}

	if duration, ok := jobDuration(status.StartTime, status.FinishTime); ok {
		attrs = append(attrs, slog.String("duration", duration.Round(time.Second).String()))
	}

	s.logger.Info(msgRestoreComplete, attrs...)
}

// logOpenHint tells how to release the namespace an aborted cold restore keeps closed.
func (s *Service) logOpenHint(namespace, jobID string) {
	s.logger.Warn(msgOpenHint,
		slog.String("namespace", namespace),
		slog.String("job-id", jobID))
}

// restoreTerminalError names the restore job that ended in a terminal state, along with
// the reason the cluster gave for it when there was one.
func restoreTerminalError(err error, namespace, jobID, errorReason string) error {
	if errorReason == "" {
		return fmt.Errorf("%w: namespace %s, job-id %s", err, namespace, jobID)
	}

	return fmt.Errorf("%w: namespace %s, job-id %s, reason %q", err, namespace, jobID, errorReason)
}

// restoreSnapshot describes the progress of a restore job. layoutNodes is the number of
// nodes the cold restore is spread over, see restoreRatio.
func restoreSnapshot(status *infomodels.ResponseRestoreState, layoutNodes int) *progressSnapshot {
	attrs := []any{
		slog.String("job-id", status.JobID),
		slog.String("state", string(status.State)),
	}

	stage := string(status.State)

	// Only a cold restore gates the namespace, and the gate tells the stages of a cold
	// restore apart: the job is ACTIVE from its preparation to its end.
	if status.Kind == infomodels.RestoreKindCold {
		attrs = append(attrs, slog.String("ns-state", string(status.NsState)))
		stage += "/" + string(status.NsState)
	}

	if status.BackupEntries > 1 {
		attrs = append(attrs,
			slog.String("backup-id", status.BackupID),
			slog.Int("backup-entry", int(status.BackupEntry)+1),
			slog.Int("backup-entries", int(status.BackupEntries)))
	}

	return &progressSnapshot{
		attrs: attrs,
		stage: stage,
		ratio: restoreRatio(status, layoutNodes),
		// The cold restore counts no records, only the fuzzy one writes them one by one.
		records:    status.Applied,
		hasRecords: status.Kind == infomodels.RestoreKindFuzzy,
		startTime:  status.StartTime,
	}
}

// restoreRatio estimates the completion of a restore job, as the server reports no
// percentage for it. Both kinds of restore apply the backups of the request one by one,
// so the completed backups count in full and the current one by its own share:
//   - a fuzzy restore applies them oldest first and reports the partitions it covered in
//     the current one;
//   - a cold restore applies them newest first and reports only how many of the
//     layoutNodes nodes finished their share, so its estimate moves in steps.
//
// The figures are merged across the nodes, and a node that is behind holds back the
// backup the merged status reports, so the estimate errs on the low side.
func restoreRatio(status *infomodels.ResponseRestoreState, layoutNodes int) float64 {
	if status.State == infomodels.RestoreStateComplete {
		return 1
	}

	entries := float64(max(status.BackupEntries, 1))
	entry := float64(status.BackupEntry)

	var ratio float64

	switch status.Kind {
	case infomodels.RestoreKindFuzzy:
		covered := min(float64(status.Covered)/partitionCount, 1)
		ratio = (entry + covered) / entries
	case infomodels.RestoreKindCold:
		// Nothing is written before the restore starts.
		if !status.IsStarted() || layoutNodes <= 0 {
			return 0
		}

		done := min(float64(status.NDoneNodes)/float64(layoutNodes), 1)
		ratio = (entries - 1 - entry + done) / entries
	}

	return min(max(ratio, 0), 1)
}
