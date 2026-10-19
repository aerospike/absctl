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

	"github.com/aerospike/backup-go"
	infomodels "github.com/aerospike/backup-go/pkg/asinfo/models"
)

// msgStorageCleared announces a namespace that is ready for the restore.
const msgStorageCleared = "storage was successfully cleared"

var (
	// errRestoreNotPreparing is returned when the restore job whose namespace is being
	// cleared stopped before the namespace became ready for the restore.
	errRestoreNotPreparing = errors.New("restore job is no longer being prepared")

	// errPrepareFailed is returned when the preparation of the namespace failed while
	// its storage was being cleared.
	errPrepareFailed = errors.New("namespace preparation failed")
)

// endedRestoreStates are the states in which a restore job no longer prepares its
// namespace, which a clear of the storage cannot get past.
var endedRestoreStates = map[infomodels.RestoreState]bool{
	infomodels.RestoreStateComplete: true,
	infomodels.RestoreStateFailed:   true,
	infomodels.RestoreStateAborting: true,
	infomodels.RestoreStateAborted:  true,
}

// ClearRestoreStorage clears the storage of the namespace of the configured cold restore
// job on every node that needs it, and waits for the namespace to become ready for the
// restore. With Async set it returns as soon as the nodes accept the clear.
func (s *Service) ClearRestoreStorage(ctx context.Context) error {
	client, err := s.newInfoClient(ctx)
	if err != nil {
		return err
	}
	defer client.Close()

	return s.clearRestoreStorage(ctx, client)
}

// clearRestoreStorage requests the clear and follows it through the status of the job.
// A node can back out of an accepted clear and ask for it again, in which case the clear
// is requested once more: the request is only sent to the nodes that ask for it.
func (s *Service) clearRestoreStorage(ctx context.Context, client backup.ServerBackupInfo) error {
	namespace, jobID := s.restoreCfg.ClearStorage.Namespace, s.restoreCfg.ClearStorage.JobID

	// The error the cluster answers with already names the failed request, so it is
	// returned as it is.
	if err := client.ClearRestoreStorage(ctx, namespace, jobID); err != nil {
		return err
	}

	s.logger.Info("clearing storage",
		slog.String("namespace", namespace),
		slog.String("job-id", jobID))

	if s.restoreCfg.ClearStorage.Async {
		return nil
	}

	// The status of a node is refreshed shortly after it accepts the clear. Reading it
	// right away could still find the node asking for the clear it has just accepted,
	// and sending the clear again would be refused.
	if err := waitFor(ctx, s.statePollInterval); err != nil {
		return err
	}

	var (
		last *infomodels.ResponseRestoreState
		// reported is the state last logged, so that only a change is logged.
		reported string
	)

	// Zeroing the storage takes as long as the devices need, so the wait is not bounded:
	// only the user or a failed job ends it.
	err := s.waitForState(ctx, 0, nil, func(ctx context.Context) (bool, string, error) {
		status, done, err := clearState(ctx, client, namespace, jobID)
		if err != nil || status == nil {
			return false, "", err
		}

		last = status

		state := describeClearState(status)
		if !done && state != reported {
			s.logger.Info("clearing storage in progress",
				slog.String("job-id", jobID),
				slog.String("clear", string(status.Clear)),
				slog.String("ns-state", string(status.NsState)))

			reported = state
		}

		return done, state, nil
	})
	if err != nil {
		return err
	}

	s.logClearDone(namespace, jobID, last)

	return nil
}

// logClearDone announces a namespace that is ready for the restore, along with what the
// clear did when a node reported one.
func (s *Service) logClearDone(namespace, jobID string, status *infomodels.ResponseRestoreState) {
	attrs := []any{
		slog.String("namespace", namespace),
		slog.String("job-id", jobID),
	}

	// The figures stay zero when no node had to clear its storage.
	if status.ClearZeroedBytes > 0 || status.ClearMs > 0 {
		attrs = append(attrs,
			slog.Uint64("zeroed-bytes", status.ClearZeroedBytes),
			slog.String("duration", (time.Duration(status.ClearMs)*time.Millisecond).String()))
	}

	s.logger.Info(msgStorageCleared, attrs...)
}

// clearState reads the status of the restore job whose namespace is being cleared, and
// reports whether the namespace is ready for the restore. A node that asks for the clear
// again is sent it again.
func clearState(
	ctx context.Context,
	client backup.ServerBackupInfo,
	namespace, jobID string,
) (*infomodels.ResponseRestoreState, bool, error) {
	status, err := restoreStatus(ctx, client, namespace, jobID)
	if err != nil {
		return nil, false, fmt.Errorf("failed to get restore status: %w", err)
	}

	switch {
	case endedRestoreStates[status.State]:
		return nil, false, restoreTerminalError(
			fmt.Errorf("%w, state %s", errRestoreNotPreparing, status.State),
			namespace, jobID, status.ErrorReason)
	case status.NsState == infomodels.NamespaceStateFailed:
		return nil, false, restoreTerminalError(errPrepareFailed, namespace, jobID, status.ErrorReason)
	case status.Clear == infomodels.ClearStateRequired:
		if err := client.ClearRestoreStorage(ctx, namespace, jobID); err != nil {
			return nil, false, err
		}

		return status, false, nil
	}

	ready := status.Clear == infomodels.ClearStateNone && status.NsState == infomodels.NamespaceStateReady

	return status, ready, nil
}

// describeClearState names the clear and namespace states of a status, for the log and
// for the error of a wait that ended before the namespace was ready.
func describeClearState(status *infomodels.ResponseRestoreState) string {
	return fmt.Sprintf("clear=%s ns-state=%s", status.Clear, status.NsState)
}
