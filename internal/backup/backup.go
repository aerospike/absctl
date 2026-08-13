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

package backup

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/aerospike/absctl/internal/config"
	"github.com/aerospike/absctl/internal/logging"
	"github.com/aerospike/absctl/internal/models"
	"github.com/aerospike/absctl/internal/storage"
	"github.com/aerospike/backup-go"
)

// Service represents a struct that encapsulates components for backup and logging functionalities.
// It is responsible for managing backup clients, configurations,
// and related components with additional operational parameters.
type Service struct {
	backupClient *backup.Client
	config       *backup.ConfigBackup

	writer backup.Writer
	// reader is used to read a state file.
	reader backup.StreamingReader

	// Additional params.
	isEstimate       bool
	estimatesSamples int64

	reportToLog bool

	logger *slog.Logger
}

// NewService initializes and returns a new Service instance,
// configuring all necessary components for a backup process.
// It returns a nil Service without an error if the backup was launched
// with --remove-artifacts and no backup should run.
func NewService(
	ctx context.Context,
	cfg *config.BackupServiceConfig,
	logger *slog.Logger,
) (*Service, error) {
	backupConfig, err := config.NewBackupConfig(cfg, logger)
	if err != nil {
		return nil, err
	}

	// We don't need a writer for estimates.
	var writer backup.Writer
	if cfg.ShouldInitWriter() {
		writer, err = storage.NewBackupWriter(ctx, cfg, logger)
		if err != nil {
			return nil, err
		}

		// For --remove-artifacts we shouldn't start backup.
		if writer == nil {
			return nil, nil
		}
	}

	reader, err := storage.NewStateReader(ctx, cfg, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize state reader: %w", err)
	}

	aerospikeClient, err := storage.NewAerospikeClient(
		cfg.ClientConfig,
		cfg.ClientPolicy,
		backupConfig.RackList,
		0,
		logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create aerospike client: %w", err)
	}

	logger.Info("initializing backup client")

	backupClient, err := backup.NewClient(
		aerospikeClient,
		backup.WithLogger(logger),
		backup.WithInfoPolicies(cfg.Backup.InfoPolicy(), cfg.Backup.RetryPolicy()),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create backup client: %w", err)
	}

	svc := &Service{
		backupClient: backupClient,
		config:       backupConfig,
		writer:       writer,
		reader:       reader,
		logger:       logger,
		reportToLog:  cfg.App.LogJSON || cfg.App.LogFile != "",
	}

	if cfg.Backup != nil {
		svc.isEstimate = cfg.Backup.Estimate
		svc.estimatesSamples = cfg.Backup.EstimateSamples
	}

	return svc, nil
}

// Run executes the scan backup process for the Service
// based on its configuration and context.
// Returns an error if the backup process encounters issues.
func (s *Service) Run(ctx context.Context) error {
	if s.isEstimate {
		s.logger.Info("calculating backup estimate")

		estimates, err := s.backupClient.Estimate(ctx, s.config, s.estimatesSamples)
		if err != nil {
			return fmt.Errorf("failed to calculate backup estimate: %w", err)
		}

		logging.ReportEstimate(estimates, s.reportToLog, s.logger)

		return nil
	}

	s.logger.Info("starting scan backup")

	h, err := s.backupClient.Backup(ctx, s.config, s.writer, s.reader)
	if err != nil {
		return fmt.Errorf("failed to start backup: %w", errHumanize(err))
	}

	if err = h.Wait(ctx); err != nil {
		return fmt.Errorf("failed to backup: %w", err)
	}

	logging.ReportBackup(h.GetStats(), s.reportToLog, s.logger)

	return nil
}

// errHumanize simplifies technical error messages.
func errHumanize(err error) error {
	// This error is returned from the info command, so it is not aerospike.Error.
	// Because of that, we can check only the string.
	if strings.Contains(err.Error(), models.ErrNodeNotFoundText) {
		return models.ErrNodeNotFound
	}

	return err
}
