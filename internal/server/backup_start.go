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
	"strconv"

	"github.com/aerospike/absctl/internal/models"
	infomodels "github.com/aerospike/backup-go/pkg/asinfo/models"
)

// StartBackup initiates a backup process using the service's configured backup settings,
// then follows the job it started until the backup reaches a terminal state. With Async
// set it returns as soon as the cluster accepts the job.
func (s *Service) StartBackup(ctx context.Context) error {
	client, err := s.newInfoClient(ctx)
	if err != nil {
		return err
	}
	defer client.Close()

	if err := s.checkClusterStable(ctx, client, s.backupCfg.Start.Namespace); err != nil {
		return err
	}

	bReq, err := s.backupRequest()
	if err != nil {
		return err
	}

	// Following the backup ends by reading its manifest, so the storage is opened before
	// the backup is requested: an unusable configuration has to fail the command now,
	// not once there is a running backup that cannot be followed. It is the same
	// configuration the cluster writes the backup with, so a backup started without it
	// would not get far either.
	l, err := s.startedBackupMetadataLister(ctx)
	if err != nil {
		return err
	}

	jobID, err := client.StartBackup(ctx, bReq)
	if err != nil {
		return fmt.Errorf("failed to start backup: %w", err)
	}

	s.logger.Info("server integrated backup started",
		slog.String("backup-id", jobID))

	if s.backupCfg.Start.Async {
		return nil
	}

	// The backup runs in the cluster, so watching it only reads its status: leaving the
	// watch, whether by an interrupt or by a failure here, does not stop the backup, and
	// "backup abort" is the only thing that does.
	return s.reportBackupProgress(ctx, client, l, jobID, followStartedJob)
}

// startedBackupMetadataLister returns the reader of the manifest of the backup being
// started, which is written under the configured path. An asynchronous start reads
// nothing, and gets nil.
func (s *Service) startedBackupMetadataLister(ctx context.Context) (metadataGetter, error) {
	if s.backupCfg.Start.Async {
		return nil, nil
	}

	return s.newMetadataLister(ctx, models.NormalizeS3Prefix(s.backupCfg.Start.Path))
}

// backupRequest maps the backup configuration onto the request the cluster is sent.
func (s *Service) backupRequest() (*infomodels.RequestBackup, error) {
	start := s.backupCfg.Start

	var mb, ma string

	if start.ModifiedBefore != "" {
		mbt, err := start.ModifiedBeforeTime()
		if err != nil {
			return nil, fmt.Errorf("failed to parse modified-before time: %w", err)
		}
		mb = strconv.FormatInt(mbt.Unix(), 10)
	}

	if start.ModifiedAfter != "" {
		mat, err := start.ModifiedAfterTime()
		if err != nil {
			return nil, fmt.Errorf("failed to parse modified-after time: %w", err)
		}
		ma = strconv.FormatInt(mat.Unix(), 10)
	}

	objectStorage, err := s.backupCfg.ObjectStorageType()
	if err != nil {
		return nil, fmt.Errorf("failed to resolve object storage type: %w", err)
	}

	return &infomodels.RequestBackup{
		Namespace:      start.Namespace,
		Storage:        objectStorage,
		Path:           models.NormalizeS3Prefix(start.Path),
		Bucket:         s.backupCfg.AwsS3.BucketName,
		Region:         s.backupCfg.AwsS3.Region,
		Profile:        s.backupCfg.AwsS3.Profile,
		Endpoint:       s.backupCfg.AwsS3.Endpoint,
		SetList:        start.SetList,
		FilterExp:      start.FilterExp,
		NoIndexes:      new(start.NoIndexes),
		NoUDFs:         new(start.NoUDFs),
		BinList:        start.BinList,
		ModifiedAfter:  ma,
		ModifiedBefore: mb,
		Policy:         infomodels.BackupPolicy(start.Policy),

		BackupRPS:                    start.RecordsPerSecond,
		BackupWorkerThreads:          start.ParallelRead,
		BackupUploadThreads:          start.ParallelWrite,
		BackupS3ThroughputTargetGbps: start.S3Throughput,
	}, nil
}
