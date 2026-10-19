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
	"path"

	"github.com/aerospike/absctl/internal/logging"
	"github.com/aerospike/absctl/internal/models"
	"github.com/aerospike/absctl/internal/storage"
	"github.com/aerospike/backup-go/pkg/server/segvalidator"
	"github.com/aerospike/backup-go/pkg/server/segvalidator/streamers"
)

// BackupValidate validates the backup identified by the configured job ID, read from
// under the configured path.
func (s *Service) BackupValidate(ctx context.Context) error {
	prefix := models.NormalizeS3Prefix(s.backupCfg.Validation.Path)

	// The manifest is read with the bounded manifest timeout, so that an unreachable
	// storage fails the command right away. Validation itself streams whole segments
	// and gets a client without that timeout.
	metaClient, err := s.metadataS3Client(ctx, s.backupCfg.AwsS3)
	if err != nil {
		return err
	}

	if err := s.checkBackupExists(ctx, metaClient, s.backupCfg.AwsS3.BucketName, prefix,
		s.backupCfg.Validation.JobID); err != nil {
		return err
	}

	client, err := storage.NewS3Client(ctx, s.backupCfg.AwsS3)
	if err != nil {
		return fmt.Errorf("failed to create s3 client: %w", err)
	}

	// The streamer addresses a backup by a single key, so the path it was written under
	// is part of that key.
	streamer, err := streamers.NewS3(
		client,
		s.backupCfg.AwsS3.BucketName,
		path.Join(prefix, s.backupCfg.Validation.JobID),
		streamers.WithLogger(s.logger),
	)
	if err != nil {
		return fmt.Errorf("failed to create s3 streamer: %w", err)
	}

	v, err := segvalidator.NewSegValidator(streamer, segvalidator.WithLogger(s.logger))
	if err != nil {
		return fmt.Errorf("failed to create seg validator: %w", err)
	}

	report, err := v.Validate(ctx, s.backupCfg.Validation.SampleSize)
	if err != nil {
		return fmt.Errorf("failed to validate: %w", err)
	}

	logging.PrintServerValidationReport(report, s.backupCfg.App.LogJSON, s.logger)

	return nil
}
