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
	"strings"
	"time"

	"github.com/aerospike/backup-go/pkg/asinfo"
	infomodels "github.com/aerospike/backup-go/pkg/asinfo/models"
)

const (
	// msgRestoreComplete announces a finished restore.
	msgRestoreComplete = "restore complete"

	// msgOpenHint tells how to release the namespace a cold restore that failed or was
	// aborted keeps closed to clients. It takes the command to run, see restoreCommand.
	msgOpenHint = "the namespace stays closed to clients until it is released with '%s'"

	// msgClearStorageRequired tells how to get past a cold restore that waits for the
	// storage of the namespace to be cleared on some nodes, and takes the command to run.
	// msgClearThenWait is how a watch adds that it waits for the clear and goes on with
	// the restore, and msgClearThenFollow is how a command that does not stay with the
	// job adds how to follow the restore after, with the command to run for that.
	msgClearStorageRequired = "the namespace storage must be cleared on some nodes before the restore " +
		"can start: run '%s'"
	msgClearThenWait   = msgClearStorageRequired + "; waiting for the clear, the restore is followed once it starts"
	msgClearThenFollow = msgClearStorageRequired + ", then follow the restore with '%s'"

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

	return s.restoreProgress(ctx, client)
}

// restoreProgress reports the progress of the configured restore job in the mode the
// configuration asks for.
func (s *Service) restoreProgress(ctx context.Context, client restoreStatusGetter) error {
	progress := s.restoreCfg.Progress

	mode := reportOnce
	if progress.Watch {
		mode = followJob
	}

	return s.reportRestoreProgress(ctx, client, progress.Namespace, progress.JobID, mode)
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
		return nil, fmt.Errorf("failed to get restore status: %w", err)
	}

	if status == nil {
		return nil, errEmptyRestoreStatus
	}

	return status, nil
}

// newRestoreWatch returns the watch of the restore job jobID of namespace, with
// everything but what the caller does with the status filled in. unseenLookups is how
// many lookups the job is given to turn up in the status, and missing decides how the
// watch ends once the job is accepted as missing, see jobWatch.
func (s *Service) newRestoreWatch(
	client restoreStatusGetter,
	namespace, jobID string,
	unseenLookups int,
	missing func(ctx context.Context, last *infomodels.ResponseRestoreState, seen bool) error,
) *jobWatch[*infomodels.ResponseRestoreState] {
	return &jobWatch[*infomodels.ResponseRestoreState]{
		logger:        s.logger,
		attrs:         []any{slog.String("namespace", namespace), slog.String("job-id", jobID)},
		gapInterval:   s.vanishedPollInterval,
		unseenLookups: unseenLookups,
		lookup: func(ctx context.Context) (*infomodels.ResponseRestoreState, error) {
			return restoreStatus(ctx, client, namespace, jobID)
		},
		missing:  missing,
		describe: restoreStateLabel,
	}
}

// missingRestore returns the missing function of a watch of the restore job jobID of
// namespace. The cluster keeps the state of a finished restore job, cold or fuzzy, so a
// job missing from the status has not finished, and ends the watch with
// restoreMissingError.
func missingRestore(
	namespace, jobID string,
) func(context.Context, *infomodels.ResponseRestoreState, bool) error {
	return func(_ context.Context, _ *infomodels.ResponseRestoreState, seen bool) error {
		return restoreMissingError(namespace, jobID, seen)
	}
}

// restoreMissingError names the restore job the cluster does not report: one it never
// reported at all, or one it stopped reporting.
func restoreMissingError(namespace, jobID string, seen bool) error {
	if seen {
		return fmt.Errorf("%w: namespace %s, job-id %s", errRestoreStatusLost, namespace, jobID)
	}

	return fmt.Errorf("%w: namespace %s, job-id %s", errRestoreNotFound, namespace, jobID)
}

// reportRestoreProgress polls the cluster for the status of the restore job jobID of
// namespace and logs it until the restore reaches a terminal state, or once in
// reportOnce mode.
//
// A cold restore does not start while a node waits for its storage to be cleared. That
// is reported once with a hint on how to clear it, and a watch keeps polling until the
// clear is done in another session, then goes on reporting the restore. A job that is
// already stopping is not reported as waiting for a clear, as clearing cannot help it.
//
// A job is missing from the status the same way as a backup job is, see
// reportBackupProgress, except that the cluster keeps the state of a finished restore
// job, so a job that stops reporting has not completed, see missingRestore.
func (s *Service) reportRestoreProgress(
	ctx context.Context,
	client restoreStatusGetter,
	namespace, jobID string,
	mode progressMode,
) error {
	printer := newProgressPrinter(s.logger, msgRestoreProgress)

	var (
		// clearReported keeps the hint from repeating on every poll while the same clear
		// is pending.
		clearReported bool
		clock         hydrationClock
	)

	w := s.newRestoreWatch(client, namespace, jobID, mode.unseenLookups(),
		missingRestore(namespace, jobID))

	w.handle = func(ctx context.Context, status *infomodels.ResponseRestoreState) (bool, time.Duration, error) {
		now := time.Now()
		snap := restoreSnapshot(status)
		clock.track(status, snap, now)

		printer.Print(ctx, snap, now)

		switch status.State {
		case infomodels.RestoreStateComplete:
			s.logRestoreComplete(status)

			return true, 0, nil
		case infomodels.RestoreStateFailed:
			if status.Kind == infomodels.RestoreKindCold {
				s.logOpenHint(namespace, jobID)
			}

			return true, 0, restoreTerminalError(errRestoreFailed, namespace, jobID, status.ErrorReason)
		case infomodels.RestoreStateAborted:
			if status.Kind == infomodels.RestoreKindCold {
				s.logOpenHint(namespace, jobID)
			}

			return true, 0, restoreTerminalError(errRestoreAborted, namespace, jobID, status.ErrorReason)
		}

		clearRequired := status.Clear == infomodels.ClearStateRequired &&
			status.State == infomodels.RestoreStateActive
		if clearRequired && !clearReported {
			clearCmd := restoreCommand(restoreCmdClearStorage, namespace, flagJobID, jobID)

			msg := fmt.Sprintf(msgClearThenFollow, clearCmd,
				restoreCommand(restoreCmdProgress, namespace, flagJobID, jobID, flagWatch))
			if mode != reportOnce {
				// The watch goes on reporting the restore once the storage is cleared.
				msg = fmt.Sprintf(msgClearThenWait, clearCmd)
			}

			// The message names the job in the command it gives, so it carries no
			// attributes that would repeat it.
			s.logger.Warn(msg)
		}

		clearReported = clearRequired

		// A cold restore held before its hydration waits for the operator, which takes
		// seconds at least, and its progress stays at zero, which the progress pacing
		// would answer with the shortest interval.
		if clearRequired || clock.waits(status) {
			return mode == reportOnce, s.statePollInterval, nil
		}

		return mode == reportOnce, nextPoll(snap, now), nil
	}

	return w.run(ctx)
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

// logOpenHint tells how to release the namespace a failed or aborted cold restore keeps
// closed. The cluster finds the restore by the namespace, but the open follows the
// release only with the job id, so the command names the job when jobID is known.
func (s *Service) logOpenHint(namespace, jobID string) {
	var extra []string
	if jobID != "" {
		extra = []string{flagJobID, jobID}
	}

	// The message names the namespace and the job in the command it gives, so it carries
	// no attributes that would repeat them.
	s.logger.Warn(fmt.Sprintf(msgOpenHint, restoreCommand(restoreCmdOpen, namespace, extra...)))
}

// The snapshot-restore subcommands a hint tells the user to run, and the flags a hint
// adds to them.
const (
	restoreCmdProgress     = "progress"
	restoreCmdOpen         = "open"
	restoreCmdClearStorage = "clear-storage"

	flagJobID = "--job-id"
	flagWatch = "--watch"
)

// restoreCommand returns the command line of the snapshot-restore subcommand that acts
// on namespace, with the extra flags and their values after it, such as the job id of
// a subcommand that names the job. The line ends with "..." for the connection and auth
// flags, which the hint cannot know.
func restoreCommand(subcommand, namespace string, extra ...string) string {
	cmd := fmt.Sprintf("absctl snapshot-restore %s --namespace %s", subcommand, namespace)
	if len(extra) > 0 {
		cmd += " " + strings.Join(extra, " ")
	}

	return cmd + " ..."
}

// restoreTerminalError names the restore job that ended in a terminal state, along with
// the reason the cluster gave for it when there was one.
func restoreTerminalError(err error, namespace, jobID, errorReason string) error {
	if errorReason == "" {
		return fmt.Errorf("%w: namespace %s, job-id %s", err, namespace, jobID)
	}

	return fmt.Errorf("%w: namespace %s, job-id %s, reason %q", err, namespace, jobID, errorReason)
}

// restoreSnapshot describes the progress of a restore job.
func restoreSnapshot(status *infomodels.ResponseRestoreState) *progressSnapshot {
	// The label of an active cold restore comes from its namespace state, so the label
	// moves on with every stage of the job.
	label := restoreStateLabel(status)

	attrs := []any{
		slog.String("job-id", status.JobID),
		slog.String("state", label),
	}

	if status.HeldReason != "" {
		attrs = append(attrs, slog.String("held-reason", status.HeldReason))
	}

	// The job cannot finish until the layout nodes that left come back, or it is aborted.
	if status.MissingLayoutNodes > 0 {
		attrs = append(attrs, slog.Uint64("missing-layout-nodes", uint64(status.MissingLayoutNodes)))
	}

	if status.BackupEntries > 1 {
		attrs = append(attrs,
			slog.String("backup-id", status.BackupID),
			slog.Int("backup-entry", int(status.BackupEntry)+1),
			slog.Int("backup-entries", int(status.BackupEntries)))
	}

	return &progressSnapshot{
		attrs: attrs,
		debugAttrs: func() []any {
			return restoreDebugAttrs(status, restoreStateDescription(status))
		},
		stage:      label,
		ratio:      completionRatio(status.ProgressPct, status.State == infomodels.RestoreStateComplete),
		recsPerSec: status.RecsPerSec,
		startTime:  status.StartTime,
	}
}

// preparationNamespaceStates are the merged namespace states of an active cold restore
// before any partition is hydrated: the gate is not up yet on some node (NONE, or no state
// when every node reports a stub row), or the namespace is being prepared.
var preparationNamespaceStates = map[infomodels.NamespaceState]bool{
	infomodels.NamespaceStateNone:      true,
	"":                                 true,
	infomodels.NamespaceStatePreparing: true,
	infomodels.NamespaceStateReady:     true,
}

// waitsForPreparation reports whether a cold restore is still preparing its namespace,
// before any partition is hydrated.
func waitsForPreparation(status *infomodels.ResponseRestoreState) bool {
	return status.Kind == infomodels.RestoreKindCold && status.State == infomodels.RestoreStateActive &&
		preparationNamespaceStates[status.NsState]
}

// hydrationClock follows the stages of a cold restore across the statuses of one watch.
// It tells when the hydration started after a wait for a clear or for the preparation,
// which can take any time and would throw the projected time left far off, and tells the
// NONE of a released namespace from the NONE of a gate that is not up yet.
type hydrationClock struct {
	// held is set once the watch has seen the job waiting for its preparation.
	held bool
	// hydrating is set once the watch has seen the job past its preparation.
	hydrating bool
	// start and startRatio are when the watch first saw the job past its preparation
	// after a wait, and the completion it reported then.
	start      time.Time
	startRatio float64
}

// track records the status seen at now, and sets the point snap, the snapshot of that
// status, is projected from: the start of the hydration once the watch has seen it begin
// after a wait. Otherwise snap keeps the job start, such as for a job that was already
// hydrating when the watch began.
func (c *hydrationClock) track(status *infomodels.ResponseRestoreState, snap *progressSnapshot, now time.Time) {
	switch {
	case c.waits(status):
		c.held = true
	case !c.hydrating && status.Kind == infomodels.RestoreKindCold &&
		status.State == infomodels.RestoreStateActive:
		c.hydrating = true

		if c.held {
			c.start, c.startRatio = now, snap.ratio
		}
	}

	if !c.start.IsZero() {
		snap.startTime, snap.startRatio = c.start, c.startRatio
	}
}

// waits reports whether the cold restore still waits for its preparation, see
// waitsForPreparation. Once the job was seen hydrating, NONE is the namespace being
// released, for a moment before the job is COMPLETE, and not a gate still to come up.
func (c *hydrationClock) waits(status *infomodels.ResponseRestoreState) bool {
	return waitsForPreparation(status) &&
		(!c.hydrating || status.NsState != infomodels.NamespaceStateNone)
}
