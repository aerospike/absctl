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
	"fmt"
	"log/slog"
	"time"

	infomodels "github.com/aerospike/backup-go/pkg/asinfo/models"
)

const (
	// msgNamespaceReady announces a namespace that is ready for the restore.
	msgNamespaceReady = "namespace is ready for the restore"

	// msgClearStorageRequired tells how to get past a preparation that is held up by the
	// storage of the namespace on some nodes.
	msgClearStorageRequired = "the namespace storage must be cleared on some nodes before the restore: " +
		"run \"snapshot-restore clear-storage\""
)

// restorePreparer is the part of the info client that prepares a restore and follows the
// preparation.
type restorePreparer interface {
	stabilityChecker
	restoreStatusGetter
	PrepareRestore(ctx context.Context, request *infomodels.RequestPrepareRestore) error
}

// PrepareRestore initiates a restore preparation process for the configured job, and
// waits for the namespace to become ready for the restore. With Async set it returns as
// soon as the cluster accepts the request.
func (s *Service) PrepareRestore(ctx context.Context) error {
	client, err := s.newInfoClient(ctx)
	if err != nil {
		return err
	}
	defer client.Close()

	return s.prepareRestore(ctx, client)
}

// prepareRestore checks the cluster, requests the preparation, and follows it through the
// status of the job unless Async is set.
//
// The wait ends when every node reports the namespace READY. A node whose storage has to
// be cleared first never gets there on its own, so the wait also ends as soon as a node
// asks for the clear, and says how to run it.
func (s *Service) prepareRestore(ctx context.Context, client restorePreparer) error {
	namespace, jobID := s.restoreCfg.Prepare.Namespace, s.restoreCfg.Prepare.JobID

	if err := s.checkClusterStable(ctx, client, namespace); err != nil {
		return err
	}

	if err := client.PrepareRestore(ctx, s.prepareRestoreRequest()); err != nil {
		return fmt.Errorf("failed to prepare restore: %w", err)
	}

	s.logger.Info("restore preparation started",
		slog.String("job-id", jobID))

	if s.restoreCfg.Prepare.Async {
		return nil
	}

	var (
		clearRequired bool
		// reported is the state last logged, so that only a change is logged.
		reported string
	)

	w := s.newRestoreWatch(client, namespace, jobID, startedJobConfirmations,
		missingRestore(namespace, jobID))

	w.handle = func(_ context.Context, status *infomodels.ResponseRestoreState) (bool, time.Duration, error) {
		if err := preparationError(status, namespace, jobID); err != nil {
			return true, 0, err
		}

		switch {
		case status.Clear == infomodels.ClearStateRequired:
			clearRequired = true

			return true, 0, nil
		case namespaceReady(status),
			// No node reports a clear state, so nothing can hold the namespace back.
			status.Clear == "" && status.NsState == infomodels.NamespaceStateReady:
			return true, 0, nil
		}

		if state := describeClearState(status); state != reported {
			s.logger.Info("preparing namespace",
				slog.String("job-id", jobID),
				slog.String("clear", string(status.Clear)),
				slog.String("ns-state", string(status.NsState)))

			reported = state
		}

		return false, s.statePollInterval, nil
	}

	if err := w.run(ctx); err != nil {
		return err
	}

	if clearRequired {
		s.logger.Warn(msgClearStorageRequired,
			slog.String("namespace", namespace),
			slog.String("job-id", jobID))

		return nil
	}

	s.logger.Info(msgNamespaceReady,
		slog.String("namespace", namespace),
		slog.String("job-id", jobID))

	return nil
}

// prepareRestoreRequest maps the prepare configuration onto the request the cluster is sent.
func (s *Service) prepareRestoreRequest() *infomodels.RequestPrepareRestore {
	return &infomodels.RequestPrepareRestore{
		Namespace:      s.restoreCfg.Prepare.Namespace,
		JobID:          s.restoreCfg.Prepare.JobID,
		HydrateReplica: new(s.restoreCfg.Prepare.HydrateReplica),
	}
}
