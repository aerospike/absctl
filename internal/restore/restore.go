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

package restore

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/aerospike/absctl/internal/config"
	"github.com/aerospike/absctl/internal/logging"
	"github.com/aerospike/absctl/internal/storage"
	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/backup-go"
)

// Service represents a type used to handle Aerospike data recovery operations with configurable restore settings.
type Service struct {
	// aerospikeClient and reader are owned by the Service and released by Close.
	// aerospikeClient is nil in validation mode.
	aerospikeClient *aerospike.Client
	backupClient    *backup.Client
	config          *backup.ConfigRestore
	reader          storage.Reader
	logger          *slog.Logger
	reportToLog     bool
}

// NewService initializes and returns a new Service instance,
// configuring all necessary components for a restore process.
// The caller owns the returned Service and must close it.
func NewService(
	ctx context.Context,
	cfg *config.RestoreServiceConfig,
	logger *slog.Logger,
) (_ *Service, err error) {
	// Important! engineClient is what backup-go gets. It is an interface, not the exact
	// *aerospike.Client, and is assigned only a non-nil client: validation runs with
	// a nil client, and a typed nil pointer in the interface would not be nil.
	var engineClient backup.AerospikeClient

	restoreConfig := config.NewRestoreConfig(cfg, logger)

	svc := &Service{
		config:      restoreConfig,
		logger:      logger,
		reportToLog: cfg.App.LogJSON || cfg.App.LogFile != "",
	}

	// Release whatever was acquired before a later step failed.
	defer func() {
		if err != nil {
			svc.Close()
		}
	}()

	// Skip client initialization in validation mode.
	if !restoreConfig.ValidateOnly {
		warmUp := GetWarmUp(cfg.Restore.WarmUp, cfg.Restore.MaxAsyncBatches)
		logger.Debug("warm up is set", slog.Int("value", warmUp))

		svc.aerospikeClient, err = storage.NewAerospikeClient(
			ctx,
			cfg.ClientConfig,
			cfg.ClientPolicy,
			nil,
			warmUp,
			logger,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create aerospike client: %w", err)
		}

		engineClient = svc.aerospikeClient
	}

	svc.reader, err = storage.NewRestoreReader(ctx, cfg, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create restore reader: %w", err)
	}

	logger.Info("initializing restore client")

	svc.backupClient, err = backup.NewClient(
		engineClient,
		backup.WithLogger(logger),
		backup.WithInfoPolicies(cfg.Restore.InfoPolicy(), cfg.Restore.InfoRetryPolicy()),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create restore client: %w", err)
	}

	return svc, nil
}

// Run executes the restore or validation process based on the configured mode.
func (r *Service) Run(ctx context.Context) error {
	// For restore and validation we use a different header for log messages.
	logMessage := "restore"
	if r.config.ValidateOnly {
		logMessage = "validation"
	}

	r.logger.Info("starting " + logMessage)

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

// Close releases the Aerospike client and the storage client of the reader.
// Call it after Run has returned. It is safe to call on a Service created in
// validation mode, on a nil Service and more than once.
//
// The restore is over by then. A storage client that fails to release cannot
// affect it, so the failure is logged rather than returned.
func (r *Service) Close() {
	if r == nil {
		return
	}

	if r.aerospikeClient != nil {
		r.aerospikeClient.Close()
	}

	if r.reader != nil {
		if err := r.reader.Close(); err != nil {
			r.logger.Warn("failed to release restore reader", slog.Any("error", err))
		}
	}
}

// GetWarmUp calculates and returns the warm-up value based on the provided warmUp and maxAsyncBatches parameters.
// If warmUp is 0, it returns one greater than maxAsyncBatches. Otherwise, it returns the warmUp value.
func GetWarmUp(warmUp, maxAsyncBatches int) int {
	if warmUp == 0 {
		return maxAsyncBatches + 1
	}

	return warmUp
}
