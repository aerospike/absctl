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
	"path"
	"time"

	"github.com/aerospike/absctl/internal/models"
	"github.com/aerospike/absctl/internal/storage"
	"github.com/aerospike/backup-go/pkg/server/lister"
	servermodels "github.com/aerospike/backup-go/pkg/server/lister/models"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const (
	// metadataRequestTimeoutMillis bounds a single manifest request.
	metadataRequestTimeoutMillis = 10_000

	// The server uploads the manifest shortly after a backup completes, so it is polled
	// at a steady pace for about a minute instead of being backed off exponentially:
	// a growing delay would end in a long blind gap for a file that is due in seconds.
	metadataRetryBase       = 2 * time.Second
	metadataRetryMultiplier = 1.0
	metadataRetryAttempts   = 30
)

// s3API is the part of the S3 client that reading a backup manifest needs: the calls the
// lister built on top of it makes.
type s3API interface {
	ListObjectsV2(ctx context.Context, in *s3.ListObjectsV2Input, opts ...func(*s3.Options),
	) (*s3.ListObjectsV2Output, error)
	GetObject(ctx context.Context, in *s3.GetObjectInput, opts ...func(*s3.Options),
	) (*s3.GetObjectOutput, error)
}

// metadataGetter is the part of lister.Lister that the progress watcher needs.
type metadataGetter interface {
	GetMetadata(ctx context.Context, backupID string) (servermodels.Metadata, error)
}

// newMetadataS3Client opens an S3 client for reading backup manifests. Its requests get
// the bounded manifest timeout, so an unreachable storage fails the command instead of
// holding it.
func newMetadataS3Client(ctx context.Context, cfg *models.AwsS3) (s3API, error) {
	client, err := storage.NewS3Client(ctx, metadataS3Config(cfg))
	if err != nil {
		return nil, fmt.Errorf("failed to create s3 client: %w", err)
	}

	return client, nil
}

// newMetadataLister returns the reader of backup manifests stored under prefix in the
// configured bucket.
func (s *Service) newMetadataLister(ctx context.Context, prefix string) (metadataGetter, error) {
	s3Client, err := s.metadataS3Client(ctx, s.backupCfg.AwsS3)
	if err != nil {
		return nil, err
	}

	return newPrefixedMetadataGetter(s3Client, s.backupCfg.AwsS3.BucketName, prefix, s.logger), nil
}

// prefixedMetadataGetter reads the manifests of backups written under a key prefix.
// lister.Lister applies its prefix to listing only, so a single manifest is addressed
// by its full key.
type prefixedMetadataGetter struct {
	metadataGetter
	prefix string
}

// newPrefixedMetadataGetter returns the reader of manifests under prefix in bucket. The
// lister gets no prefix of its own, so that the prefix is applied in one place only.
func newPrefixedMetadataGetter(client s3API, bucket, prefix string, logger *slog.Logger) prefixedMetadataGetter {
	return prefixedMetadataGetter{
		metadataGetter: lister.NewLister(client, bucket, "", lister.WithLogger(logger)),
		prefix:         prefix,
	}
}

// GetMetadata reads the manifest of backupID under the prefix.
func (g prefixedMetadataGetter) GetMetadata(ctx context.Context, backupID string) (servermodels.Metadata, error) {
	return g.metadataGetter.GetMetadata(ctx, path.Join(g.prefix, backupID))
}

// metadataS3Config returns a copy of cfg with a bounded request timeout, so that reading
// a manifest never hangs on the storage and never mutates the configuration shared with
// the rest of the command.
func metadataS3Config(cfg *models.AwsS3) *models.AwsS3 {
	bounded := *cfg
	bounded.RequestTimeout = metadataRequestTimeoutMillis

	return &bounded
}
