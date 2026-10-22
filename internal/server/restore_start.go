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

	"github.com/aerospike/absctl/internal/models"
	infomodels "github.com/aerospike/backup-go/pkg/asinfo/models"
	servermodels "github.com/aerospike/backup-go/pkg/server/lister/models"
)

// StartRestore checks that the configured backup exists and is complete, then starts
// restoring it as the configured restore job, and follows the job until the restore
// reaches a terminal state. With Async set it returns as soon as the cluster accepts the
// job.
func (s *Service) StartRestore(ctx context.Context) error {
	client, err := s.newInfoClient(ctx)
	if err != nil {
		return err
	}
	defer client.Close()

	return s.startRestore(ctx, client, client.activeNodes)
}

// restoreStarter is the part of the info client that starts a restore and follows it.
type restoreStarter interface {
	stabilityChecker
	restoreStatusGetter
	StartRestore(ctx context.Context, request *infomodels.RequestRestore) error
}

// startRestore checks the cluster and the backups, requests the restore, and follows it
// unless Async is set. layoutNodes is passed on to reportRestoreProgress.
func (s *Service) startRestore(ctx context.Context, client restoreStarter, layoutNodes func() int) error {
	start := s.restoreCfg.Start

	if err := s.checkClusterStable(ctx, client, start.Namespace); err != nil {
		return err
	}

	// The backup is looked for before the restore is requested.
	s3Client, err := s.metadataS3Client(ctx, s.restoreCfg.AwsS3)
	if err != nil {
		return err
	}

	if err := s.checkRestoreBackupsExist(ctx, s3Client); err != nil {
		return err
	}

	// A prepare run before this start has to name the same job, so the fallback is
	// reported for the user to see which id that is.
	if start.JobID == "" {
		s.logger.Info("job-id is not set, the backup id is used as the restore job id",
			slog.String("job-id", start.RestoreJobID()))
	}

	rReq := s.restoreRequest()

	if err := client.StartRestore(ctx, rReq); err != nil {
		return fmt.Errorf("failed to start restore: %w", err)
	}

	s.logger.Info("server integrated restore started",
		slog.String("job-id", rReq.JobID),
		slog.String("backup-ids", rReq.BackupIDs))

	if start.Async {
		return nil
	}

	// As with the backup, watching the restore only reads its status: leaving the watch
	// does not stop the restore, and "restore abort" is the only thing that does.
	return s.reportRestoreProgress(ctx, client, layoutNodes, start.Namespace, rReq.JobID,
		followStartedJob)
}

// checkRestoreBackupsExist validates that every backup of the restore exists and is
// complete. The first backup is read from under the path, and the incremental backups
// that follow it from under the incremental path, when one is set.
func (s *Service) checkRestoreBackupsExist(ctx context.Context, client s3API) error {
	start := s.restoreCfg.Start
	bucket := s.restoreCfg.AwsS3.BucketName

	prefix := models.NormalizeS3Prefix(start.Path)

	incrementalPrefix := prefix
	if start.IncrementalPath != "" {
		incrementalPrefix = models.NormalizeS3Prefix(start.IncrementalPath)
	}

	// Validate rejects an empty backup id list, so there is always a first backup.
	ids := start.BackupIDList()

	if err := s.checkBackupsExist(ctx, client, bucket, prefix, ids[:1]); err != nil {
		return err
	}

	return s.checkBackupsExist(ctx, client, bucket, incrementalPrefix, ids[1:])
}

// restoreRequest maps the restore configuration onto the request the cluster is sent.
func (s *Service) restoreRequest() *infomodels.RequestRestore {
	start := s.restoreCfg.Start

	req := &infomodels.RequestRestore{
		Namespace:              start.Namespace,
		Storage:                start.StorageType,
		Path:                   models.NormalizeS3Prefix(start.Path),
		Bucket:                 s.restoreCfg.AwsS3.BucketName,
		Region:                 s.restoreCfg.AwsS3.Region,
		Profile:                s.restoreCfg.AwsS3.Profile,
		Endpoint:               s.restoreCfg.AwsS3.Endpoint,
		SetList:                start.SetList,
		FilterExp:              start.FilterExp,
		NoIndexes:              new(start.NoIndexes),
		NoUDFs:                 new(start.NoUDFs),
		JobID:                  start.RestoreJobID(),
		BackupIDs:              start.BackupIDs,
		IncrementalPath:        models.NormalizeS3Prefix(start.IncrementalPath),
		RestoreWorkerThreads:   start.ParallelWrite,
		RestoreDownloadThreads: start.ParallelRead,
		RestoreRPS:             start.RecordsPerSecond,
		ExtraTTL:               start.ExtraTTL,
		Policy:                 infomodels.RestorePolicy(start.Policy),
	}

	// The server refuses the fuzzy only settings, whatever their values, with any other
	// policy, so they are sent with the fuzzy policy only.
	if req.Policy == infomodels.RestorePolicyFuzzy {
		req.AllowUnhosted = new(start.AllowUnhosted)
		req.MaxInflight = start.MaxInflight
		req.RetryBaseIntervalMs = new(start.RetryBaseInterval)
		req.RetryMultiplier = start.RetryMultiplier
		req.RetryMaxAttempts = start.RetryMaxAttempts
		req.IgnoreRecordError = new(start.IgnoreRecordError)
		req.ApplyMetadataLast = applyMetadataLast(start.ApplyMetadataLast)
	}

	return req
}

// applyMetadataLast returns the apply-metadata-last setting to send. It is sent only
// when it is set: the server refuses the parameter, whatever its value, for a restore
// with neither indexes nor UDFs, and false is its default anyway.
func applyMetadataLast(v bool) *bool {
	if !v {
		return nil
	}

	return new(true)
}

// checkBackupsExist validates that every backup of backupIDs, written under prefix,
// exists and is complete. The metadata of a backup is addressed by its own id, so the
// backups are looked for one by one, and a single missing or incomplete one stops the
// restore before any of them is read.
func (s *Service) checkBackupsExist(
	ctx context.Context, client s3API, bucket, prefix string, backupIDs []string,
) error {
	for _, backupID := range backupIDs {
		if err := s.checkBackupExists(ctx, client, bucket, prefix, backupID); err != nil {
			return err
		}
	}

	return nil
}

// checkBackupExists validates that the backup backupID, written under prefix, exists and
// is complete.
func (s *Service) checkBackupExists(ctx context.Context, client s3API, bucket, prefix, backupID string) error {
	md, err := newPrefixedMetadataGetter(client, bucket, prefix, s.logger).GetMetadata(ctx, backupID)
	if err != nil {
		return fmt.Errorf("failed to check if backup exists: %w", err)
	}

	if md.Status != servermodels.MetadataStatusComplete {
		return fmt.Errorf("backup %s is not complete, has status %s", backupID, md.Status)
	}

	s.logger.Info("backup found",
		slog.String("backup-id", md.BackupID),
		slog.String("namespace", md.Namespace),
	)

	return nil
}
