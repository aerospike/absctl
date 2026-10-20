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

	"github.com/aerospike/absctl/internal/logging"
	"github.com/aerospike/absctl/internal/models"
	"github.com/aerospike/absctl/internal/storage"
	"github.com/aerospike/backup-go/errclass"
	"github.com/aerospike/backup-go/pkg/server/lister"
	servermodels "github.com/aerospike/backup-go/pkg/server/lister/models"
)

// backupLister is the part of lister.Lister that listing needs: the manifests under the
// configured path, plus the one manifest a path naming a single backup holds.
type backupLister interface {
	metadataGetter
	FetchAllMetadata(ctx context.Context) ([]servermodels.Metadata, error)
}

// ListBackups lists all backups from the configured storage.
func (s *Service) ListBackups(ctx context.Context) error {
	client, err := storage.NewS3Client(ctx, s.backupCfg.AwsS3)
	if err != nil {
		return fmt.Errorf("failed to create s3 client: %w", err)
	}

	listPath := models.NormalizeS3Prefix(s.backupCfg.List.Path)
	l := lister.NewLister(client, s.backupCfg.AwsS3.BucketName, listPath, lister.WithLogger(s.logger))

	return s.listBackups(ctx, l, listPath)
}

// listBackups prints the manifests of the backups under listPath.
func (s *Service) listBackups(ctx context.Context, l backupLister, listPath string) error {
	mds, err := l.FetchAllMetadata(ctx)
	if err != nil {
		return fmt.Errorf("failed to list backups: %w", err)
	}

	// Nothing under the path can also mean that the path names a single backup rather
	// than a prefix holding several, so it is read as one before it is called empty.
	if len(mds) == 0 {
		md, err := l.GetMetadata(ctx, listPath)
		if err != nil {
			if errors.Is(err, errclass.ErrNotFound) {
				s.logger.Info("backups not found")

				return nil
			}

			return fmt.Errorf("failed to list backups: %w", err)
		}

		mds = []servermodels.Metadata{md}
	}

	return logging.PrintMetadata(mds, s.backupCfg.App.LogJSON, s.logger)
}
