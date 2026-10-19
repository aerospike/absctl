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

	infomodels "github.com/aerospike/backup-go/pkg/asinfo/models"
)

// PrepareRestore initiates a restore preparation process for the specified job ID.
func (s *Service) PrepareRestore(ctx context.Context) error {
	client, err := s.newInfoClient(ctx)
	if err != nil {
		return err
	}
	defer client.Close()

	if err := s.checkClusterStable(ctx, client, s.restoreCfg.Prepare.Namespace); err != nil {
		return err
	}

	err = client.PrepareRestore(ctx, s.prepareRestoreRequest())
	if err != nil {
		return fmt.Errorf("failed to prepare restore: %w", err)
	}

	s.logger.Info("restore preparation started",
		slog.String("job-id", s.restoreCfg.Prepare.JobID))

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
