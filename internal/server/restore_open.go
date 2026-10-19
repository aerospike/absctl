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
	"github.com/aerospike/backup-go/pkg/asinfo"
	infomodels "github.com/aerospike/backup-go/pkg/asinfo/models"
)

// openStatusTimeout bounds the wait for a released namespace to open.
const openStatusTimeout = 5 * time.Minute

// errOpenStatusTimeout is returned when the cluster accepted the release of a namespace
// but kept reporting it as closed until the wait ran out.
var errOpenStatusTimeout = errors.New("checking namespace state after opening timed out")

// OpenRestore releases the namespace held by the configured aborted cold restore job,
// and waits for the namespace to open to clients. With Async set it returns as soon as
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

	if err := s.waitForState(ctx, s.openTimeout,
		fmt.Errorf("%w: namespace %s, job-id %s", errOpenStatusTimeout, namespace, jobID),
		func(ctx context.Context) (bool, string, error) {
			return namespaceOpened(ctx, client, namespace, jobID)
		}); err != nil {
		return err
	}

	s.logger.Info("namespace was successfully opened", slog.String("namespace", namespace))

	return nil
}

// namespaceOpened reports whether the namespace held by the restore job jobID is open to
// clients again, along with the namespace state it read. A job the cluster does not
// report cannot confirm the release, so the wait goes on.
func namespaceOpened(
	ctx context.Context,
	client restoreStatusGetter,
	namespace, jobID string,
) (opened bool, nsState string, err error) {
	status, err := restoreStatus(ctx, client, namespace, jobID)
	if err != nil {
		if errors.Is(err, asinfo.ErrNotFound) {
			return false, "", nil
		}

		return false, "", fmt.Errorf("failed to get restore status: %w", err)
	}

	// The merged state is NONE only when no node keeps the namespace closed, and it is
	// left empty or not hosted when no node hosts the namespace at all.
	switch status.NsState {
	case infomodels.NamespaceStateNone, infomodels.NamespaceStateNotHosted, "":
		return true, string(status.NsState), nil
	default:
		return false, string(status.NsState), nil
	}
}
