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
	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/backup-go"
)

// Service represents a struct that encapsulates components for backup and logging functionalities.
// It is responsible for managing backup clients, configurations,
// and related components with additional operational parameters.
type Service struct {
	// aerospikeClient, writer and reader are owned by the Service and released by Close.
	aerospikeClient *aerospike.Client
	backupClient    *backup.Client
	config          *backup.ConfigBackup

	// writer is nil for estimates.
	writer storage.Writer
	// reader is used to read a state file. It is nil when no state file is read.
	reader storage.Reader

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
// The caller owns the returned Service and must close it.
func NewService(
	ctx context.Context,
	cfg *config.BackupServiceConfig,
	logger *slog.Logger,
) (_ *Service, err error) {
	backupConfig, err := config.NewBackupConfig(cfg, logger)
	if err != nil {
		return nil, err
	}

	svc := &Service{
		config:      backupConfig,
		logger:      logger,
		reportToLog: cfg.App.LogJSON || cfg.App.LogFile != "",
	}

	// Release whatever was acquired before a later step failed.
	defer func() {
		if err != nil {
			svc.Close()
		}
	}()

	// We don't need a writer for estimates.
	if cfg.ShouldInitWriter() {
		svc.writer, err = storage.NewBackupWriter(ctx, cfg, logger)
		if err != nil {
			return nil, err
		}

		// For --remove-artifacts we shouldn't start backup.
		if svc.writer == nil {
			return nil, nil
		}
	}

	svc.reader, err = storage.NewStateReader(ctx, cfg, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize state reader: %w", err)
	}

	svc.aerospikeClient, err = storage.NewAerospikeClient(
		ctx,
		cfg.ClientConfig,
		cfg.ClientPolicy,
		backupConfig.RackList,
		0,
		logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create aerospike client: %w", err)
	}

	logger.Info("initializing backup client")

	svc.backupClient, err = backup.NewClient(
		svc.aerospikeClient,
		backup.WithLogger(logger),
		backup.WithInfoPolicies(cfg.Backup.InfoPolicy(), cfg.Backup.InfoRetryPolicy()),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create backup client: %w", err)
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

// Close releases the Aerospike client and the storage clients of the writer and
// the state reader. Call it after Run has returned. It is safe to call on a nil
// Service and more than once.
//
// The backup is over by then: backup-go has closed every file it wrote. A storage
// client that fails to release cannot affect the backup, so the failure is logged
// rather than returned.
func (s *Service) Close() {
	if s == nil {
		return
	}

	if s.aerospikeClient != nil {
		s.aerospikeClient.Close()
	}

	if s.writer != nil {
		if err := s.writer.Close(); err != nil {
			s.logger.Warn("failed to release backup writer", slog.Any("error", err))
		}
	}

	if s.reader != nil {
		if err := s.reader.Close(); err != nil {
			s.logger.Warn("failed to release state reader", slog.Any("error", err))
		}
	}
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
