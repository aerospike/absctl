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

package restore

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/aerospike/absctl/internal/config"
	"github.com/aerospike/absctl/internal/logging"
	"github.com/aerospike/absctl/internal/storage"
	"github.com/aerospike/backup-go"
)

// Service represents a type used to handle Aerospike data recovery operations with configurable restore settings.
type Service struct {
	backupClient *backup.Client
	config       *backup.ConfigRestore
	reader       backup.StreamingReader
	logger       *slog.Logger
	reportToLog  bool
}

// NewService initializes and returns a new Service instance,
// configuring all necessary components for a restore process.
func NewService(
	ctx context.Context,
	cfg *config.RestoreServiceConfig,
	logger *slog.Logger,
) (*Service, error) {
	// Important! Describe the variable as an interface, not the exact *aerospike.Client,
	// so we can run backup files validation with a nil aerospike client.
	var (
		aerospikeClient backup.AerospikeClient
		err             error
	)

	restoreConfig := config.NewRestoreConfig(cfg, logger)

	// Skip client initialization in validation mode.
	if !restoreConfig.ValidateOnly {
		warmUp := GetWarmUp(cfg.Restore.WarmUp, cfg.Restore.MaxAsyncBatches)
		logger.Debug("warm up is set", slog.Int("value", warmUp))

		aerospikeClient, err = storage.NewAerospikeClient(
			cfg.ClientConfig,
			cfg.ClientPolicy,
			nil,
			warmUp,
			logger,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create aerospike client: %w", err)
		}
	}

	reader, err := storage.NewRestoreReader(ctx, cfg, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create restore reader: %w", err)
	}

	logger.Info("initializing restore client")

	backupClient, err := backup.NewClient(
		aerospikeClient,
		backup.WithLogger(logger),
		backup.WithInfoPolicies(cfg.Restore.InfoPolicy(), cfg.Restore.RetryPolicy()),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create restore client: %w", err)
	}

	return &Service{
		backupClient: backupClient,
		config:       restoreConfig,
		reader:       reader,
		logger:       logger,
		reportToLog:  cfg.App.LogJSON || cfg.App.LogFile != "",
	}, nil
}

// Run executes the restore or validation process based on the configured mode.
func (r *Service) Run(ctx context.Context) error {
	// For restore and validation we use a different header for log messages.
	logMessage := "restore"
	if r.config.ValidateOnly {
		logMessage = "validation"
	}

	r.logger.Info("starting " + logMessage)

	r.config.EncoderType = backup.EncoderTypeASB

	h, err := r.backupClient.Restore(ctx, r.config, r.reader)
	if err != nil {
		return fmt.Errorf("failed to start %s: %w", logMessage, err)
	}

	if err = h.Wait(ctx); err != nil {
		return fmt.Errorf("failed to perform %s: %w", logMessage, err)
	}

	logging.ReportRestore(h.GetStats(), r.config.ValidateOnly, r.reportToLog, r.logger)

	return nil
}

// GetWarmUp calculates and returns the warm-up value based on the provided warmUp and maxAsyncBatches parameters.
// If warmUp is 0, it returns one greater than maxAsyncBatches. Otherwise, it returns the warmUp value.
func GetWarmUp(warmUp, maxAsyncBatches int) int {
	if warmUp == 0 {
		return maxAsyncBatches + 1
	}

	return warmUp
}
