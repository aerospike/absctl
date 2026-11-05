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

// OpenRestore releases the namespace held by the configured failed or aborted cold
// restore job, and waits for the namespace to open to clients. With Async set it returns as soon as
// the cluster accepts the request.
func (s *Service) OpenRestore(ctx context.Context) error {
	client, err := s.newInfoClient(ctx)
	if err != nil {
		return err
	}
	defer client.Close()

	return s.openRestore(ctx, client)
}

// openRestore requests the release of the namespace and confirms it through the status
// of the job.
func (s *Service) openRestore(ctx context.Context, client backup.ServerBackupInfo) error {
	namespace, jobID := s.restoreCfg.Open.Namespace, s.restoreCfg.Open.JobID

	// The error the cluster answers with already names the failed request, so it is
	// returned as it is.
	if err := client.OpenRestore(ctx, namespace, jobID); err != nil {
		return err
	}

	s.logger.Info("opening namespace",
		slog.String("namespace", namespace),
		slog.String("job-id", jobID))

	if s.restoreCfg.Open.Async {
		return nil
	}

	// The cluster keeps the state of the released job, so a job it does not report cannot
	// confirm the release, and is followed like any other job missing from the status.
	w := s.newRestoreWatch(client, namespace, jobID, startedJobConfirmations,
		missingRestore(namespace, jobID))

	// reported is the namespace state last logged, so that a wait that does not move on
	// shows where it stands without a line per lookup.
	var reported infomodels.NamespaceState

	// A failed or aborted job keeps its state through the release, which only moves the
	// namespace state on to OPENING and then NONE, so the namespace state alone says
	// whether the release is done. The cluster refuses to open a job that still runs.
	w.handle = func(_ context.Context, status *infomodels.ResponseRestoreState) (bool, time.Duration, error) {
		if namespaceOpened(status.NsState) {
			return true, 0, nil
		}

		if status.NsState != reported {
			s.logger.Info("opening namespace in progress",
				slog.String("job-id", jobID),
				slog.String("ns-state", string(status.NsState)))

			reported = status.NsState
		}

		return false, s.statePollInterval, nil
	}

	if err := w.run(ctx); err != nil {
		return err
	}

	s.logger.Info("namespace was successfully opened", slog.String("namespace", namespace))

	return nil
}

// namespaceOpened reports whether the merged namespace state nsState says the namespace
// is open to clients again. The merged state is NONE only when no node keeps the
// namespace closed, and it is left empty or reported as a stub when no node reports more
// than a stub row.
func namespaceOpened(nsState infomodels.NamespaceState) bool {
	switch nsState {
	case infomodels.NamespaceStateNone, infomodels.NamespaceStateStub, "":
		return true
	default:
		return false
	}
}
