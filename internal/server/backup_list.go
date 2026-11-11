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
	"path"

	"github.com/aerospike/absctl/internal/logging"
	"github.com/aerospike/absctl/internal/models"
	"github.com/aerospike/absctl/internal/storage"
	"github.com/aerospike/backup-go/errclass"
	"github.com/aerospike/backup-go/pkg/asinfo"
	"github.com/aerospike/backup-go/pkg/server/lister"
	servermodels "github.com/aerospike/backup-go/pkg/server/lister/models"
)

const msgNoBackups = "backups not found"

// backupLister is the part of lister.Lister that listing needs: the manifests under the
// configured path.
type backupLister interface {
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
	// A path naming a single backup is read as that backup's id under the path above it.
	parent := newMetadataGetter(client, s.backupCfg.AwsS3.BucketName, parentPrefix(listPath), s.logger)

	return s.listBackups(ctx, l, parent, listPath)
}

// listBackups prints the manifests of the backups under listPath. parent reads the
// manifests of the path one level up, where a path naming a single backup is found.
func (s *Service) listBackups(ctx context.Context, l backupLister, parent metadataGetter, listPath string) error {
	mds, err := l.FetchAllMetadata(ctx)
	if err != nil {
		return fmt.Errorf("failed to list backups: %w", err)
	}

	// Nothing under the path can also mean that the path names a single backup rather
	// than a prefix holding several, so it is read as one before it is called empty.
	// Only a path that ends in a job id can name a backup.
	if len(mds) == 0 {
		backupID := path.Base(listPath)
		if !asinfo.IsJobID(backupID) {
			s.logger.Info(msgNoBackups)

			return nil
		}

		md, err := parent.GetMetadata(ctx, backupID)
		if err != nil {
			if errors.Is(err, errclass.ErrNotFound) {
				s.logger.Info(msgNoBackups)

				return nil
			}

			return fmt.Errorf("failed to list backups: %w", err)
		}

		return logging.PrintMetadata([]*servermodels.Metadata{&md}, s.backupCfg.App.LogJSON, s.logger)
	}

	// The entries point into the listed slice, so no manifest is copied for printing.
	entries := make([]*servermodels.Metadata, len(mds))
	for i := range mds {
		entries[i] = &mds[i]
	}

	return logging.PrintMetadata(entries, s.backupCfg.App.LogJSON, s.logger)
}

// parentPrefix returns the prefix one level above p, empty for the bucket root.
func parentPrefix(p string) string {
	if dir := path.Dir(p); dir != "." {
		return dir
	}

	return ""
}
