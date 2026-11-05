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

// msgNoRunningRestore is reported by an abort that found no running restore to abort.
const msgNoRunningRestore = "no running restore found, nothing to abort"

// terminalRestoreStates are the states of a restore job that has already ended, and that
// an abort has nothing left to do for. The server refuses to abort a FAILED job: a failed
// cold restore is released with "snapshot-restore open" instead.
//
// RestoreStateUnknown is deliberately not one of them either, for the same reason as in
// terminalBackupStates.
var terminalRestoreStates = map[infomodels.RestoreState]bool{
	infomodels.RestoreStateComplete: true,
	infomodels.RestoreStateFailed:   true,
	infomodels.RestoreStateAborted:  true,
}

// AbortRestore asks the cluster to abort the configured restore job, once it has
// confirmed the namespace holds a restore to abort, and waits for the namespace to report
// that it holds one no longer. With Async set it returns as soon as the cluster accepts
// the abort.
func (s *Service) AbortRestore(ctx context.Context) error {
	client, err := s.newInfoClient(ctx)
	if err != nil {
		return err
	}
	defer client.Close()

	return s.abortRestore(ctx, client)
}

// abortRestore checks that the restore job is running, requests the abort, and then
// confirms it through the status of the job. An aborted cold restore keeps its namespace
// closed to clients, so the abort ends by saying how to release it.
func (s *Service) abortRestore(ctx context.Context, client backup.ServerBackupInfo) error {
	namespace, jobID := s.restoreCfg.Abort.Namespace, s.restoreCfg.Abort.JobID

	status, err := s.runningRestore(ctx, client, namespace, jobID)
	if err != nil || status == nil {
		return err
	}

	// The error the cluster answers with already names the failed abort, so it is
	// returned as it is: wrapping it here only repeats what the command line prints.
	if err := client.AbortRestore(ctx, namespace, jobID); err != nil {
		return err
	}

	s.logger.Info("aborting restore",
		slog.String("namespace", namespace),
		slog.String("job-id", jobID))

	if s.restoreCfg.Abort.Async {
		if status.Kind == infomodels.RestoreKindCold {
			s.logOpenHint(namespace, jobID)
		}

		return nil
	}

	outcome, last, err := s.waitForRestoreAbort(ctx, client, namespace, jobID, status.Kind)
	if err != nil {
		return err
	}

	// A job that completed before the abort landed was not aborted: its data is in the
	// namespace, and the namespace opens on its own.
	if outcome == abortTooLate {
		s.logger.Info("restore finished before it could be aborted",
			slog.String("job-id", jobID))

		return nil
	}

	// A job some node failed before the abort landed stopped all the same, which is what
	// the abort asks for, but the cause of the failure must not be lost.
	if last != nil && last.State == infomodels.RestoreStateFailed {
		s.logger.Warn("restore failed before the abort landed",
			slog.String("job-id", jobID),
			slog.String("error-reason", last.ErrorReason))
	} else {
		s.logger.Info("restore was successfully aborted")
	}

	if status.Kind == infomodels.RestoreKindCold {
		s.logOpenHint(namespace, jobID)
	}

	return nil
}

// waitForRestoreAbort follows the restore job of kind that was asked to stop until it
// does, and reports how it stopped along with the last status read, nil when the job
// left the status. The job is followed the same way "restore start" follows the job it
// started, see jobWatch.
//
// The cluster keeps the state of a stopped cold restore job, which holds the namespace
// until it is released, so only its state confirms the abort, and a cold job the status
// no longer reports ends the wait with an error. The abort of a fuzzy restore removes
// the job, so a fuzzy job that leaves the status has stopped, as a backup job does.
func (s *Service) waitForRestoreAbort(
	ctx context.Context,
	client restoreStatusGetter,
	namespace, jobID string,
	kind infomodels.RestoreKind,
) (abortOutcome, *infomodels.ResponseRestoreState, error) {
	var (
		outcome abortOutcome
		last    *infomodels.ResponseRestoreState
	)

	// The pre-abort check has seen the job running, so a job this wait never finds is one
	// the cluster stopped reporting.
	w := s.newRestoreWatch(client, namespace, jobID, startedJobConfirmations,
		func(context.Context, *infomodels.ResponseRestoreState, bool) error {
			if kind == infomodels.RestoreKindFuzzy {
				outcome = abortStopped
				last = nil

				return nil
			}

			return restoreMissingError(namespace, jobID, true)
		})

	w.handle = func(_ context.Context, status *infomodels.ResponseRestoreState) (bool, time.Duration, error) {
		outcome = restoreAbortOutcome(status.State)
		last = status

		return outcome != abortPending, s.statePollInterval, nil
	}

	if err := w.run(ctx); err != nil {
		return 0, nil, err
	}

	return outcome, last, nil
}

// restoreAbortOutcome says what the state of a restore job that was asked to stop means
// for the abort. A job that reports FAILED failed on some node before the abort landed,
// and stopped all the same: there is nothing left to wait for.
func restoreAbortOutcome(state infomodels.RestoreState) abortOutcome {
	switch state {
	case infomodels.RestoreStateAborted, infomodels.RestoreStateFailed:
		return abortStopped
	case infomodels.RestoreStateComplete:
		return abortTooLate
	default:
		return abortPending
	}
}

// runningRestore returns the status of the restore job when the cluster is working on it,
// and nil when it is not, so that an abort is only requested when there is a job to
// abort. The cluster accepts the abort of a job it knows nothing about, and the status
// that follows such a request cannot be told from a job that stopped on request - which
// announces a restore that never ran as aborted.
//
// The status misses the job as soon as a single node does not report it, for instance
// for a moment after the node restarted, while the other nodes keep running it. The job
// is therefore given startedJobConfirmations lookups to turn up before it is taken as
// missing.
func (s *Service) runningRestore(
	ctx context.Context,
	client restoreStatusGetter,
	namespace, jobID string,
) (*infomodels.ResponseRestoreState, error) {
	var status *infomodels.ResponseRestoreState

	// A job that never turns up leaves status nil, which is answered below.
	w := s.newRestoreWatch(client, namespace, jobID, startedJobConfirmations,
		func(context.Context, *infomodels.ResponseRestoreState, bool) error {
			return nil
		})

	w.handle = func(_ context.Context, st *infomodels.ResponseRestoreState) (bool, time.Duration, error) {
		status = st

		return true, 0, nil
	}

	if err := w.run(ctx); err != nil {
		return nil, err
	}

	if status == nil {
		s.logger.Info(msgNoRunningRestore,
			slog.String("namespace", namespace),
			slog.String("job-id", jobID))

		return nil, nil
	}

	// A job that already reached its end is still reported for a while. Naming the state
	// it ended in says why the abort did nothing.
	if terminalRestoreStates[status.State] {
		s.logger.Info(msgNoRunningRestore,
			slog.String("namespace", namespace),
			slog.String("job-id", jobID),
			slog.String("state", string(status.State)))

		if status.Kind == infomodels.RestoreKindCold && status.State != infomodels.RestoreStateComplete {
			s.logOpenHint(namespace, jobID)
		}

		return nil, nil
	}

	return status, nil
}
