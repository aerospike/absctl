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
	"log/slog"
	"time"

	"github.com/aerospike/backup-go"
	"github.com/aerospike/backup-go/pkg/asinfo"
	infomodels "github.com/aerospike/backup-go/pkg/asinfo/models"
)

// OpenRestore releases the namespace held by a failed or aborted cold restore. A
// namespace has at most one restore job, so the cluster finds the job by the namespace.
// With the job id given, it first checks that the cluster reports that job of the
// namespace, and waits for the namespace to open to clients, unless Async is set;
// without it, it returns as soon as the cluster accepts the request.
func (s *Service) OpenRestore(ctx context.Context) error {
	client, err := s.newInfoClient(ctx)
	if err != nil {
		return err
	}
	defer client.Close()

	return s.openRestore(ctx, client)
}

const (
	// msgOpeningNamespace announces an accepted open.
	msgOpeningNamespace = "opening namespace"

	// msgOpenNotFollowed announces an accepted open given no job id. The status of a
	// restore is read by its job id, so the release is not followed.
	msgOpenNotFollowed = msgOpeningNamespace + ", the release is not followed as no job id is given: " +
		"pass --job-id to wait until the namespace opens to clients"
)

// openRestore requests the release of the namespace and, when the job id is given,
// confirms it through the status of the job, which is read by the job id.
func (s *Service) openRestore(ctx context.Context, client backup.ServerBackupInfo) error {
	namespace, jobID := s.restoreCfg.Open.Namespace, s.restoreCfg.Open.JobID

	// The job id is not sent: the cluster finds the job by the namespace. So a job id
	// that names no job of the namespace is caught before the release, rather than by
	// a wait that fails after the namespace was released.
	var checked *infomodels.ResponseRestoreState

	if jobID != "" {
		status, err := checkOpenJob(ctx, client, namespace, jobID)
		if err != nil {
			return err
		}

		checked = status
	}

	// The error the cluster answers with already names the failed request, so it is
	// returned as it is.
	if err := client.OpenRestore(ctx, namespace); err != nil {
		return err
	}

	async := s.restoreCfg.Open.Async

	if jobID == "" {
		// Without the job id the release cannot be followed, which is worth telling
		// only to a user who did not ask to return right away.
		msg := msgOpenNotFollowed
		if async {
			msg = msgOpeningNamespace
		}

		s.logger.Info(msg, slog.String("namespace", namespace))

		return nil
	}

	s.logger.Info(msgOpeningNamespace,
		slog.String("namespace", namespace),
		slog.String("job-id", jobID))

	if async {
		return nil
	}

	// The cluster keeps the state of the released job, so a job it does not report cannot
	// confirm the release, and is followed like any other job missing from the status.
	// The check before the release has seen the job, so a job the wait does not find has
	// left the status, rather than not turned up yet, and the lookups of a job never
	// reported do not apply.
	w := s.newRestoreWatch(client, namespace, jobID, 1, missingRestore(namespace, jobID))
	w.initial = &checked
	// A job missing from the status for a moment is named by the label its progress
	// lines use.
	w.describe = openStateLabel

	// reported is the state last logged, so that a wait that does not move on shows where
	// it stands without a line per lookup.
	var reported string

	// A failed or aborted job keeps its state through the release, which only moves the
	// namespace state on to OPENING and then NONE, so the namespace state alone says
	// whether the release is done. The cluster refuses to open a job that still runs.
	w.handle = func(ctx context.Context, status *infomodels.ResponseRestoreState) (bool, time.Duration, error) {
		if namespaceOpened(status.NsState) {
			return true, 0, nil
		}

		if label := openStateLabel(status); label != reported {
			attrs := []any{slog.String("job-id", jobID), slog.String("state", label)}

			s.logger.InfoContext(ctx, "opening namespace in progress", withDebugAttrs(ctx, s.logger, attrs,
				func() []any { return restoreDebugAttrs(status, openStateDescription(status)) })...)

			reported = label
		}

		return false, s.statePollInterval, nil
	}

	if err := w.run(ctx); err != nil {
		return err
	}

	s.logger.Info("namespace was successfully opened", slog.String("namespace", namespace))

	return nil
}

// checkOpenJob checks that the cluster reports the restore job jobID of namespace, and
// returns its status. The status is filtered by the namespace, so a job of another
// namespace is not reported.
func checkOpenJob(
	ctx context.Context, client restoreStatusGetter, namespace, jobID string,
) (*infomodels.ResponseRestoreState, error) {
	status, err := restoreStatus(ctx, client, namespace, jobID)
	if err != nil {
		if errors.Is(err, asinfo.ErrNotFound) {
			return nil, restoreMissingError(namespace, jobID, false)
		}

		return nil, err
	}

	return status, nil
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
