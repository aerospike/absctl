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

	"github.com/aerospike/absctl/internal/config"
	"github.com/aerospike/absctl/internal/logging"
	"github.com/aerospike/absctl/internal/storage"
	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/backup-go"
	"github.com/aerospike/backup-go/models"
	"github.com/aerospike/backup-go/pkg/asinfo"
)

const messageNoRunningBackup = "No running backup found"

// Service represents a server integrated backup and restore service.
type Service struct {
	config *config.ServerBackupServiceConfig
	reader backup.StreamingReader

	// reportToLog bool
	logger *slog.Logger
}

// NewService initializes and returns a new Service instance.
func NewService(
	cfg *config.ServerBackupServiceConfig,
	logger *slog.Logger,
) (*Service, error) {
	return &Service{
		config: cfg,
		logger: logger,
	}, nil
}

func (s *Service) Run(ctx context.Context) error {
	switch {
	case s.config.ServerBackup.ListPath != "":
		return s.ListBackups(ctx)
	default:
		return s.StartBackup(ctx)
	}
}

func (s *Service) ListBackups(ctx context.Context) error {
	l := NewLister(s.reader, s.logger, s.config.App.LogJSON)

	if s.config.ServerBackup.ListPath == "/" || s.config.ServerBackup.ListPath == "\\" {
		s.config.ServerBackup.ListPath = ""
	}

	if err := l.listBackups(ctx, s.config.ServerBackup.ListPath); err != nil {
		return fmt.Errorf("failed to list backups: %w", err)
	}

	return nil
}

func (s *Service) ListBackupsV2(ctx context.Context) error {
	client, err := storage.NewS3Client(ctx, s.config.AwsS3)
	if err != nil {
		return fmt.Errorf("failed to create s3 client: %w", err)
	}

	w := logging.GetMetadataWriter(s.config.App.LogJSON)

	l := NewListerV2(client, s.config.AwsS3.BucketName, "", s.config.App.LogJSON, w, s.logger)

	if err := l.FetchAllMetadata(ctx); err != nil {
		return fmt.Errorf("failed to list V2 backups: %w", err)
	}

	if err := logging.CloseMetadataWriter(l.writer); err != nil {
		return fmt.Errorf("failed to close metadata writer: %w", err)
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

	JobID, err := client.StartServerBackup(
		ctx,
		s.config.ServerBackup.Namespace,
		s.config.ServerBackup.StorageType,
		s.config.AwsS3.BucketName,
		s.config.AwsS3.Region,
		s.config.AwsS3.Profile,
		s.config.AwsS3.AccessKeyID,
		s.config.AwsS3.SecretAccessKey,
	)
	if err != nil {
		return fmt.Errorf("failed to start backup: %w", err)
	}

	s.logger.Info("Server integrated backup started",
		//nolint:sloglint // Name parameter as flag.
		slog.String("backup-id", JobID))

	return nil
}

// StartRestore initiates a restore process for the specified job ID using the service's backup configuration.
func (s *Service) StartRestore(ctx context.Context) error {
	client, err := s.newInfoClient()
	if err != nil {
		return err
	}

	err = client.StartServerRestore(
		ctx,
		s.config.ServerBackup.JobID,
		s.config.ServerBackup.Namespace,
		s.config.ServerBackup.StorageType,
		s.config.AwsS3.BucketName,
		s.config.AwsS3.Region,
		s.config.AwsS3.Profile,
		s.config.AwsS3.AccessKeyID,
		s.config.AwsS3.SecretAccessKey,
	)
	if err != nil {
		return fmt.Errorf("failed to start restore: %w", err)
	}

	s.logger.Info("Server integrated restore started",
		//nolint:sloglint // Name parameter as flag.
		slog.String("backup-id", s.config.ServerBackup.JobID))

	return nil
}

// PrepareRestore initiates a restore preparation process for the specified job ID.
func (s *Service) PrepareRestore(ctx context.Context) error {
	client, err := s.newInfoClient()
	if err != nil {
		return err
	}

	fmt.Println("PrepareRestore", s.config.ServerBackup.JobID)

	err = client.PrepareServerRestore(
		ctx,
		s.config.ServerBackup.JobID,
		s.config.ServerBackup.Namespace,
	)
	if err != nil {
		return fmt.Errorf("failed to prepare restore: %w", err)
	}

	s.logger.Info("Restore preparation started",
		//nolint:sloglint // Name parameter as flag.
		slog.String("backup-id", s.config.ServerBackup.JobID))

	return nil
}

func (s *Service) GetStatus(ctx context.Context) error {
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

// newInfoClient separate function for a lazy load.
func (s *Service) newInfoClient() (*asinfo.Client, error) {
	aerospikeClient, err := storage.NewAerospikeClient(
		s.config.ClientConfig,
		s.config.ClientPolicy,
		nil,
		0,
		s.logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create aerospike client: %w", err)
	}

	infoClient, err := asinfo.NewClient(
		aerospikeClient.Cluster(),
		aerospike.NewInfoPolicy(),
		models.NewDefaultRetryPolicy(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create info client: %w", err)
	}

	return infoClient, nil
}
