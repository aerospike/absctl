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

package integrated

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/aerospike/absctl/internal/config"
	"github.com/aerospike/absctl/internal/lister"
	"github.com/aerospike/absctl/internal/storage"
	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/backup-go"
	"github.com/aerospike/backup-go/models"
	"github.com/aerospike/backup-go/pkg/asinfo"
)

// Service represents a server integrated backup and restore service.
type Service struct {
	config *config.IntegratedServiceConfig
	reader backup.StreamingReader

	// reportToLog bool

	logger *slog.Logger
}

// NewService initializes and returns a new Service instance.
func NewService(
	ctx context.Context,
	cfg *config.IntegratedServiceConfig,
	logger *slog.Logger,
) (*Service, error) {
	var (
		reader backup.StreamingReader
		err    error
	)

	// If list path is set, init reader.
	if cfg.IntegratedBackup.ListPath != "" {
		reader, err = storage.NewReader(
			ctx,
			&cfg.ServiceConfigCommon,
			cfg.IntegratedBackup.ListPath,
			"",
			"",
			"",
			0,
			false,
			true,
			logger,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create reader: %w", err)
		}
	}

	return &Service{
		config: cfg,
		reader: reader,
		logger: logger,
	}, nil
}

func (s *Service) Run(ctx context.Context) error {
	switch {
	case s.config.IntegratedBackup.ListPath != "":
		return s.ListBackups(ctx)
	default:
		return s.StartBackup(ctx)
	}
}

func (s *Service) ListBackups(ctx context.Context) error {
	l := lister.NewLister(s.reader)

	if s.config.IntegratedBackup.ListPath == "/" || s.config.IntegratedBackup.ListPath == "\\" {
		s.config.IntegratedBackup.ListPath = ""
	}

	if err := l.ListBackups(ctx, s.config.IntegratedBackup.ListPath); err != nil {
		return fmt.Errorf("failed to list backups: %w", err)
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

	err = client.StartBackup(
		ctx,
		s.config.IntegratedBackup.JobID,
		s.config.IntegratedBackup.Namespace,
		s.config.IntegratedBackup.StorageType,
		s.config.AwsS3.BucketName,
		s.config.AwsS3.Region,
		s.config.AwsS3.Profile,
		s.config.AwsS3.AccessKeyID,
		s.config.AwsS3.SecretAccessKey,
	)
	if err != nil {
		return fmt.Errorf("failed to start backup: %w", err)
	}

	s.logger.Info("Server integrated backup started")

	return nil
}

// StartRestore initiates a restore process for the specified job ID using the service's backup configuration.
func (s *Service) StartRestore(ctx context.Context) error {
	client, err := s.newInfoClient()
	if err != nil {
		return err
	}

	err = client.StartRestore(
		ctx,
		s.config.IntegratedBackup.JobID,
		s.config.IntegratedBackup.Namespace,
		s.config.IntegratedBackup.StorageType,
		s.config.AwsS3.BucketName,
		s.config.AwsS3.Region,
		s.config.AwsS3.Profile,
		s.config.AwsS3.AccessKeyID,
		s.config.AwsS3.SecretAccessKey,
	)
	if err != nil {
		return fmt.Errorf("failed to start restore: %w", err)
	}

	//nolint:sloglint // Log messages must looks like flags. So no camelCase here.
	s.logger.Info("Server integrated restore started",
		slog.String("job-id", s.config.IntegratedBackup.JobID))

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
