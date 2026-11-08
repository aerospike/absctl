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
	"strings"

	"github.com/aerospike/absctl/internal/models"
	infomodels "github.com/aerospike/backup-go/pkg/asinfo/models"
	servermodels "github.com/aerospike/backup-go/pkg/server/lister/models"
)

// errBackupConsistency is returned when the restore policy cannot restore the
// consistency of a backup.
var errBackupConsistency = errors.New("backup consistency does not fit the restore policy")

// StartRestore checks that every backup of the restore exists, is complete and fits the
// restore policy, then starts the restore, logs the job id generated for it, and follows
// the job until the restore reaches a terminal state. With Async set it returns as soon as
// the cluster accepts the job.
//
// A cold restore is started by this single request: the cluster closes the namespace,
// prepares it on every node and starts the restore once every node is ready. A node
// whose storage is not empty waits for it to be cleared, in which case the command says
// how to clear it and keeps waiting: once the storage is cleared, for example with
// 'absctl snapshot-restore clear-storage' run from another session, the restore starts on its
// own and the command goes on reporting its progress.
func (s *Service) StartRestore(ctx context.Context) error {
	client, err := s.newInfoClient(ctx)
	if err != nil {
		return err
	}
	defer client.Close()

	return s.startRestore(ctx, client)
}

// restoreStarter is the part of the info client that starts a restore and follows it.
type restoreStarter interface {
	stabilityChecker
	restoreStatusGetter
	StartRestore(ctx context.Context, request *infomodels.RequestRestore) (string, error)
}

// startRestore checks the cluster and the backups, requests the restore, and follows it
// unless Async is set.
func (s *Service) startRestore(ctx context.Context, client restoreStarter) error {
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

	rReq, err := s.restoreRequest()
	if err != nil {
		return err
	}

	// The job id is generated for the restore, as it is for a backup. The other restore
	// commands name the job by it, so it is logged for the user to pass on.
	jobID, err := client.StartRestore(ctx, rReq)
	if err != nil {
		return startError("restore", jobID,
			restoreCommand(restoreCmdProgress, start.Namespace, flagJobID, jobID), err)
	}

	s.logger.Info("server integrated restore started",
		slog.String("job-id", jobID),
		slog.String("backup-ids", rReq.BackupIDs))

	if start.Async {
		return nil
	}

	// As with the backup, watching the restore only reads its status: leaving the watch
	// does not stop the restore, and "snapshot-restore abort" is the only thing that does.
	return s.reportRestoreProgress(ctx, client, start.Namespace, jobID, followStartedJob)
}

// checkRestoreBackupsExist validates that every backup of the restore exists and is
// complete, and that the policy can restore their consistency. The first backup is read
// from under the path, and the incremental backups that follow it from under the
// incremental path, when one is set.
func (s *Service) checkRestoreBackupsExist(ctx context.Context, client s3API) error {
	start := s.restoreCfg.Start
	bucket := s.restoreCfg.AwsS3.BucketName

	prefix := models.NormalizeS3Prefix(start.Path)

	// The server reads the incremental backups from under the path when no incremental
	// path is sent, which an empty prefix is not.
	incrementalPrefix := start.IncrementalPrefix()
	if incrementalPrefix == "" {
		incrementalPrefix = prefix
	}

	// Validate rejects an empty backup id list, so there is always a first backup.
	ids := start.BackupIDList()

	full, err := s.checkBackupsExist(ctx, client, bucket, prefix, ids[:1])
	if err != nil {
		return err
	}

	incremental, err := s.checkBackupsExist(ctx, client, bucket, incrementalPrefix, ids[1:])
	if err != nil {
		return err
	}

	return checkBackupsConsistency(start.RestorePolicy(), append(full, incremental...))
}

// restoreRequest maps the restore configuration onto the request the cluster is sent.
func (s *Service) restoreRequest() (*infomodels.RequestRestore, error) {
	start := s.restoreCfg.Start

	objectStorage, err := s.restoreCfg.ObjectStorageType()
	if err != nil {
		return nil, fmt.Errorf("failed to resolve object storage type: %w", err)
	}

	req := &infomodels.RequestRestore{
		Namespace:              start.Namespace,
		Storage:                infomodels.StorageType(objectStorage),
		Path:                   models.NormalizeS3Prefix(start.Path),
		Bucket:                 s.restoreCfg.AwsS3.BucketName,
		Region:                 s.restoreCfg.AwsS3.Region,
		Profile:                s.restoreCfg.AwsS3.Profile,
		Endpoint:               s.restoreCfg.AwsS3.Endpoint,
		SetList:                start.SetList,
		FilterExp:              start.FilterExp,
		NoIndexes:              new(start.NoIndexes),
		NoUDFs:                 new(start.NoUDFs),
		BackupIDs:              start.BackupIDs,
		IncrementalPath:        start.IncrementalPrefix(),
		RestoreWorkerThreads:   start.ParallelWrite,
		RestoreDownloadThreads: start.ParallelRead,
		RestoreRPS:             start.RecordsPerSecond,
		ExtraTTL:               start.ExtraTTL,
		Policy:                 start.RestorePolicy(),
		// The settings of one policy are sent as given: the validation refused the ones
		// given for the other policy, and an unset one is left to the server default.
		// The request leaves out a zero max-inflight or retry-multiplier as unset, which
		// is safe as the validation refuses both as given values.
		HydrateReplica:      start.HydrateReplica,
		MaxInflight:         derefOrZero(start.MaxInflight),
		RetryBaseIntervalMs: start.RetryBaseInterval,
		RetryMultiplier:     derefOrZero(start.RetryMultiplier),
		RetryMaxAttempts:    start.RetryMaxAttempts,
		IgnoreRecordError:   start.IgnoreRecordError,
		ApplyMetadataLast:   start.ApplyMetadataLast,
	}

	return req, nil
}

// derefOrZero returns *p, or the zero value, which the request leaves out, when p is nil.
func derefOrZero[T any](p *T) T {
	var zero T
	if p == nil {
		return zero
	}

	return *p
}

// checkBackupsConsistency checks that policy can restore backups, in the order of the
// restore. A strict (cold) restore reads strict backups only. A fuzzy restore reads a
// single fuzzy backup, or strict backups only. A backup whose metadata does not say what
// consistency it achieved is left for the cluster to check.
func checkBackupsConsistency(policy infomodels.RestorePolicy, backups []servermodels.Metadata) error {
	var fuzzyIDs []string

	for i := range backups {
		if backups[i].AchievedConsistency == servermodels.AchievedConsistencyFuzzy {
			fuzzyIDs = append(fuzzyIDs, backups[i].BackupID)
		}
	}

	// No policy restores a fuzzy backup along with others, so that is reported first: a
	// hint to switch the policy would only lead to this refusal.
	switch {
	case len(fuzzyIDs) == 0:
		return nil
	case len(backups) > 1:
		return fmt.Errorf("%w: fuzzy backups %s: a fuzzy backup is restored on its own",
			errBackupConsistency, strings.Join(fuzzyIDs, ", "))
	case policy != infomodels.RestorePolicyFuzzy:
		return fmt.Errorf("%w: fuzzy backup %s: restore it with policy %s",
			errBackupConsistency, fuzzyIDs[0], infomodels.RestorePolicyFuzzy)
	default:
		return nil
	}
}

// checkBackupsExist validates that every backup of backupIDs, written under prefix,
// exists and is complete, and returns their metadata in the same order. The metadata of
// a backup is addressed by its own id, so the backups are looked for one by one, and a
// single missing or incomplete one stops the restore before any of them is read.
func (s *Service) checkBackupsExist(
	ctx context.Context, client s3API, bucket, prefix string, backupIDs []string,
) ([]servermodels.Metadata, error) {
	backups := make([]servermodels.Metadata, 0, len(backupIDs))

	for _, backupID := range backupIDs {
		md, err := s.checkBackupExists(ctx, client, bucket, prefix, backupID)
		if err != nil {
			return nil, err
		}

		backups = append(backups, md)
	}

	return backups, nil
}

// checkBackupExists validates that the backup backupID, written under prefix, exists and
// is complete, and returns its metadata.
func (s *Service) checkBackupExists(
	ctx context.Context, client s3API, bucket, prefix, backupID string,
) (servermodels.Metadata, error) {
	md, err := newMetadataGetter(client, bucket, prefix, s.logger).GetMetadata(ctx, backupID)
	if err != nil {
		return servermodels.Metadata{}, fmt.Errorf("failed to check if backup exists: %w", err)
	}

	if md.Status != servermodels.BackupStatusComplete {
		return servermodels.Metadata{}, fmt.Errorf("backup %s is not complete, has status %s",
			backupID, md.Status)
	}

	s.logger.Info("backup found",
		slog.String("backup-id", md.BackupID),
		slog.String("namespace", md.Namespace),
	)

	return md, nil
}
