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

const (
	// msgNoRunningRestore is reported by an abort that found no running restore to abort.
	msgNoRunningRestore = "no running restore found, nothing to abort"

	// stateNotReported stands for the state of a job the status did not report, in the
	// error of an abort whose wait ran out.
	stateNotReported = "not reported"

	// missingRestoreConfirmations is how many lookups in a row must miss a restore job
	// before an abort accepts that there is nothing to abort. Each lookup already retries
	// inside the info client, so a single node that is briefly behind is covered, and a
	// mistyped job id is not answered only after half a minute.
	missingRestoreConfirmations = 2
)

// errAbortRestoreStatusTimeout is the restore counterpart of errAbortStatusTimeout: the
// cluster accepted the abort but kept reporting the job as running until the wait ran out.
var errAbortRestoreStatusTimeout = errors.New("checking restore status after abortion timed out")

// terminalRestoreStates are the states of a restore job that has already ended, and that
// an abort has nothing left to do for. A FAILED job is not one of them: the server keeps
// it running until the principal aborts it, and an abort is how it is stopped.
//
// RestoreStateUnknown is deliberately not one of them either, for the same reason as in
// terminalBackupStates.
var terminalRestoreStates = map[infomodels.RestoreState]bool{
	infomodels.RestoreStateComplete: true,
	infomodels.RestoreStateAborted:  true,
}

// AbortRestore asks the cluster to abort the configured restore job, once it has
// confirmed the namespace holds a restore to abort, and waits for the namespace to report
// that it holds one no longer.
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

	outcome, err := s.waitForAbort(ctx,
		fmt.Errorf("%w: namespace %s, job-id %s", errAbortRestoreStatusTimeout, namespace, jobID),
		func(ctx context.Context) (abortOutcome, string, error) {
			return restoreAbortState(ctx, client, namespace, jobID)
		})
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

	s.logger.Info("restore was successfully aborted")

	if status.Kind == infomodels.RestoreKindCold {
		s.logOpenHint(namespace, jobID)
	}

	return nil
}

// restoreAbortState reads the state of a restore job that was asked to stop, and says
// what it means for the abort. A FAILED job is still pending: the principal turns the
// failure into an abort, and the job is only stopped once it reports that.
//
// A job missing from the status is pending as well. The cluster keeps the state of a
// stopped restore job, and the status misses the job as soon as a single node does not
// report it, so a miss says nothing about the job: only ABORTED confirms the abort.
func restoreAbortState(
	ctx context.Context,
	client backup.ServerBackupInfo,
	namespace, jobID string,
) (abortOutcome, string, error) {
	status, err := restoreStatus(ctx, client, namespace, jobID)
	if err != nil {
		if errors.Is(err, asinfo.ErrNotFound) {
			return abortPending, stateNotReported, nil
		}

		return 0, "", fmt.Errorf("failed to get restore status: %w", err)
	}

	switch status.State {
	case infomodels.RestoreStateAborted:
		return abortStopped, string(status.State), nil
	case infomodels.RestoreStateComplete:
		return abortTooLate, string(status.State), nil
	default:
		return abortPending, string(status.State), nil
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
// is therefore only taken as missing after missingRestoreConfirmations lookups in a row.
func (s *Service) runningRestore(
	ctx context.Context,
	client backup.ServerBackupInfo,
	namespace, jobID string,
) (*infomodels.ResponseRestoreState, error) {
	var status *infomodels.ResponseRestoreState

	for misses := 1; ; misses++ {
		var err error

		status, err = restoreStatus(ctx, client, namespace, jobID)
		if err == nil {
			break
		}

		if !errors.Is(err, asinfo.ErrNotFound) {
			return nil, fmt.Errorf("failed to get restore status: %w", err)
		}

		if misses >= missingRestoreConfirmations {
			s.logger.Info(msgNoRunningRestore,
				slog.String("namespace", namespace),
				slog.String("job-id", jobID))

			return nil, nil
		}

		s.logger.Debug("restore status is temporarily unavailable",
			slog.String("job-id", jobID),
			slog.Int("attempt", misses))

		if err := waitFor(ctx, s.vanishedPollInterval); err != nil {
			return nil, err
		}
	}

	// A job that already reached its end is still reported for a while. Naming the state
	// it ended in says why the abort did nothing.
	if terminalRestoreStates[status.State] {
		s.logger.Info(msgNoRunningRestore,
			slog.String("namespace", namespace),
			slog.String("job-id", jobID),
			slog.String("state", string(status.State)))

		return nil, nil
	}

	return status, nil
}
