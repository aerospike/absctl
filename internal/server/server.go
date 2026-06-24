// Copyright 2024 Aerospike, Inc.
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
	"strconv"

	"github.com/aerospike/absctl/internal/config"
	"github.com/aerospike/absctl/internal/logging"
	"github.com/aerospike/absctl/internal/models"
	"github.com/aerospike/absctl/internal/storage"
	"github.com/aerospike/aerospike-client-go/v8"
	bModels "github.com/aerospike/backup-go/models"
	"github.com/aerospike/backup-go/pkg/asinfo"
	iModels "github.com/aerospike/backup-go/pkg/asinfo/models"
	"github.com/aerospike/backup-go/pkg/server"
	commonClient "github.com/aerospike/tools-common-go/client"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const messageNoRunningBackup = "No running backup found"

// S3API is an interface for the S3 client.
type S3API interface {
	ListObjectsV2(ctx context.Context, in *s3.ListObjectsV2Input, opts ...func(*s3.Options),
	) (*s3.ListObjectsV2Output, error)
	GetObject(ctx context.Context, in *s3.GetObjectInput, opts ...func(*s3.Options),
	) (*s3.GetObjectOutput, error)
	HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
}

// Service represents a server integrated backup and restore service.
type Service struct {
	backupCfg  *config.ServerBackupServiceConfig
	restoreCfg *config.ServerRestoreServiceConfig
	logger     *slog.Logger
}

// NewService initializes and returns a new Service instance.
func NewService(
	backupCfg *config.ServerBackupServiceConfig,
	restoreCfg *config.ServerRestoreServiceConfig,
	logger *slog.Logger,
) (*Service, error) {
	return &Service{
		backupCfg:  backupCfg,
		restoreCfg: restoreCfg,
		logger:     logger,
	}, nil
}

func (s *Service) clientConfig() *commonClient.AerospikeConfig {
	if s.backupCfg != nil {
		return s.backupCfg.ClientConfig
	}

	return s.restoreCfg.ClientConfig
}

func (s *Service) clientPolicy() *models.ClientPolicy {
	if s.backupCfg != nil {
		return s.backupCfg.ClientPolicy
	}

	return s.restoreCfg.ClientPolicy
}

// newInfoClient separate function for a lazy load.
func (s *Service) newInfoClient() (*asinfo.Client, error) {
	aerospikeClient, err := storage.NewAerospikeClient(
		s.clientConfig(),
		s.clientPolicy(),
		nil,
		0,
		s.logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create aerospike client: %w", err)
	}

	infoClient, err := asinfo.NewClient(
		aerospikeClient.Cluster(),
		aerospike.NewInfoPolicy(),
		bModels.NewDefaultRetryPolicy(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create info client: %w", err)
	}

	return infoClient, nil
}

// ListBackups lists all backups from the configured storage.
func (s *Service) ListBackups(ctx context.Context) error {
	client, err := storage.NewS3Client(ctx, s.backupCfg.AwsS3)
	if err != nil {
		return fmt.Errorf("failed to create s3 client: %w", err)
	}

	l := server.NewLister(client, s.backupCfg.AwsS3.BucketName, "", server.WithLogger(s.logger))

	mds, err := l.FetchAllMetadata(ctx)
	if err != nil {
		return fmt.Errorf("failed to list V2 backups: %w", err)
	}

	if err := logging.PrintMetadata(mds, s.backupCfg.App.LogJSON, s.logger); err != nil {
		return err
	}

	return nil
}

// StartBackup initiates a backup process using the service's configured backup settings
// and returns an error if it fails.
func (s *Service) StartBackup(ctx context.Context) error {
	client, err := s.newInfoClient()
	if err != nil {
		return err
	}

	if err = s.checkServerStatus(ctx, client, s.backupCfg.Start.Namespace); err != nil {
		return fmt.Errorf("failed to check server status: %w", err)
	}

	var mb, ma string

	if s.backupCfg.Start.ModifiedBefore != "" {
		mbt, err := s.backupCfg.Start.ModifiedBeforeTime()
		if err != nil {
			return fmt.Errorf("failed to parse modified-before time: %w", err)
		}
		mb = strconv.FormatInt(mbt.Unix(), 10)
	}

	if s.backupCfg.Start.ModifiedAfter != "" {
		mat, err := s.backupCfg.Start.ModifiedAfterTime()
		if err != nil {
			return fmt.Errorf("failed to parse modified-after time: %w", err)
		}
		ma = strconv.FormatInt(mat.Unix(), 10)
	}

	JobID, err := client.StartServerBackup(
		ctx,
		s.backupCfg.Start.Namespace,
		s.backupCfg.Start.StorageType,
		s.backupCfg.AwsS3.BucketName,
		s.backupCfg.AwsS3.Region,
		s.backupCfg.AwsS3.Profile,
		s.backupCfg.AwsS3.AccessKeyID,
		s.backupCfg.AwsS3.SecretAccessKey,
		mb,
		ma,
	)
	if err != nil {
		return fmt.Errorf("failed to start backup: %w", err)
	}

	s.logger.Info("Server integrated backup started",
		slog.String("backup-id", JobID))

	return nil
}

// StartRestore initiates a restore process for the specified job ID using the service's backup configuration.
func (s *Service) StartRestore(ctx context.Context) error {
	client, err := s.newInfoClient()
	if err != nil {
		return err
	}

	if err = s.checkServerStatus(ctx, client, s.restoreCfg.Start.Namespace); err != nil {
		return fmt.Errorf("failed to check server status: %w", err)
	}

	// Need clarification before uncomment
	// if err = s.checkBackupExists(ctx,
	// 	s.restoreCfg.AwsS3.BucketName, s.restoreCfg.Start.JobID, s.restoreCfg.Start.Namespace); err != nil {
	// 	return fmt.Errorf("failed to check if backup exists: %w", err)
	// }

	err = client.StartServerRestore(
		ctx,
		s.restoreCfg.Start.JobID,
		s.restoreCfg.Start.Namespace,
		s.restoreCfg.Start.StorageType,
		s.restoreCfg.AwsS3.BucketName,
		s.restoreCfg.AwsS3.Region,
		s.restoreCfg.AwsS3.Profile,
		s.restoreCfg.AwsS3.AccessKeyID,
		s.restoreCfg.AwsS3.SecretAccessKey,
	)
	if err != nil {
		return fmt.Errorf("failed to start restore: %w", err)
	}

	s.logger.Info("Server integrated restore started",
		slog.String("backup-id", s.restoreCfg.Start.JobID))

	return nil
}

// PrepareRestore initiates a restore preparation process for the specified job ID.
func (s *Service) PrepareRestore(ctx context.Context) error {
	client, err := s.newInfoClient()
	if err != nil {
		return err
	}

	if err = s.checkServerStatus(ctx, client, s.restoreCfg.Prepare.Namespace); err != nil {
		return fmt.Errorf("failed to check server status: %w", err)
	}

	// Need clarification before uncomment
	// if err = s.checkBackupExists(ctx,
	// 	s.restoreCfg.AwsS3.BucketName, s.restoreCfg.Prepare.JobID, s.restoreCfg.Prepare.Namespace); err != nil {
	// 	return fmt.Errorf("failed to check if backup exists: %w", err)
	// }

	err = client.PrepareServerRestore(
		ctx,
		s.restoreCfg.Prepare.JobID,
		s.restoreCfg.Prepare.Namespace,
	)
	if err != nil {
		return fmt.Errorf("failed to prepare restore: %w", err)
	}

	s.logger.Info("Restore preparation started",
		slog.String("backup-id", s.restoreCfg.Prepare.JobID))

	return nil
}

// BackupProgress returns the progress of the currently running backup.
func (s *Service) BackupProgress(ctx context.Context) error {
	client, err := s.newInfoClient()
	if err != nil {
		return err
	}

	result, err := client.GetBackupStatus(ctx)
	if err != nil {
		if errors.Is(err, asinfo.ErrNotFound) {
			slog.Info(messageNoRunningBackup)

			return nil
		}

		return fmt.Errorf("failed to get backup status: %w", err)
	}

	if result >= 1.0 {
		slog.Info(messageNoRunningBackup)

		return nil
	}

	s.logger.Info("Backup progress",
		slog.String("pct", fmt.Sprintf("%.1f%%", result*100)))

	return nil
}

// BackupValidate validates the backup identified by the configured job ID.
func (s *Service) BackupValidate(ctx context.Context) error {
	client, err := storage.NewS3Client(ctx, s.backupCfg.AwsS3)
	if err != nil {
		return fmt.Errorf("failed to create s3 client: %w", err)
	}

	if err := s.checkBackupExists(ctx, client, s.backupCfg.AwsS3.BucketName, s.backupCfg.Validation.JobID); err != nil {
		return fmt.Errorf("failed to check if backup exists: %w", err)
	}

	v := NewValidator(client, s.backupCfg.AwsS3.BucketName, s.logger)

	report, err := v.Validate(ctx, s.backupCfg.Validation.JobID, s.backupCfg.Validation.SampleSize)
	if err != nil {
		return fmt.Errorf("failed to validate: %w", err)
	}

	logging.PrintServerValidationReport(report, s.backupCfg.App.LogJSON, s.logger)

	return nil
}

// checkServerStatus validates the server status.
func (s *Service) checkServerStatus(ctx context.Context, client *asinfo.Client, namespace string) error {
	lowestVersion, err := client.GetVersion(ctx)
	if err != nil {
		return fmt.Errorf("failed to get server version: %w", err)
	}

	if !lowestVersion.IsGreaterOrEqual(iModels.AerospikeVersionSupportsIntegratedBackup) {
		return fmt.Errorf("server version %s does not support integrated backup", lowestVersion)
	}

	isStable, err := client.GetClusterStable(ctx, namespace)
	if err != nil {
		return fmt.Errorf("failed to check cluster stability: %w", err)
	}

	if !isStable {
		return fmt.Errorf("cluster is not stable")
	}

	return nil
}

// checkBackupExists validates the backup exists for RESTORE only.
func (s *Service) checkBackupExists(ctx context.Context, client S3API, bucket, jobID string) error {
	l := server.NewLister(client, bucket, "", server.WithLogger(s.logger))

	md, err := l.GetMetadata(ctx, jobID)
	if err != nil {
		return fmt.Errorf("failed to check if backup exists: %w", err)
	}

	s.logger.Info("Backup found",
		slog.String("backup-id", md.BackupID),
		slog.String("namespace", md.Namespace),
	)

	return nil
}
