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
	"sync"

	"github.com/aerospike/absctl/internal/config"
	"github.com/aerospike/absctl/internal/logging"
	"github.com/aerospike/absctl/internal/models"
	"github.com/aerospike/absctl/internal/storage"
	"github.com/aerospike/backup-go"
	bModels "github.com/aerospike/backup-go/models"
)

// Service represents a type used to handle Aerospike data recovery operations with configurable restore settings.
type Service struct {
	backupClient *backup.Client
	config       *backup.ConfigRestore

	reader    backup.StreamingReader
	readerXdr backup.StreamingReader
	// Restore Mode: auto, asb, asbx
	mode string

	reportToLog bool

	logger *slog.Logger
}

// NewService initializes and returns a new Service instance,
// configuring all necessary components for a restore process.
func NewService(
	ctx context.Context,
	cfg *config.RestoreServiceConfig,
	logger *slog.Logger,
) (*Service, error) {
	var (
		// Important! To describe variable as interface not exact *a.Client.
		// So we can run backup files validation with the 'nil' aerospike client.
		aerospikeClient backup.AerospikeClient
		err             error
	)
	// Set default restore mode to asb.
	// This should be removed once asbx is released.
	cfg.Restore.Mode = models.RestoreModeASB

	// Initializations.
	restoreConfig := config.NewRestoreConfig(cfg, logger)

	// Skip this part on validation.
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

	reader, xdrReader, err := storage.NewRestoreReader(ctx, cfg, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create restore reader: %w", err)
	}

	logger.Info("initializing restore client")

	infoRetryPolicy := cfg.Restore.RetryPolicy()

	infoPolicy := cfg.Restore.InfoPolicy()

	backupClient, err := backup.NewClient(
		aerospikeClient,
		backup.WithLogger(logger),
		backup.WithInfoPolicies(infoPolicy, infoRetryPolicy))
	if err != nil {
		return nil, fmt.Errorf("failed to create restore client: %w", err)
	}

	return &Service{
		backupClient: backupClient,
		config:       restoreConfig,
		reader:       reader,
		readerXdr:    xdrReader,
		mode:         cfg.Restore.Mode,
		logger:       logger,
		reportToLog:  cfg.App.LogJSON || cfg.App.LogFile != "",
	}, nil
}

// Run executes the restore process based on the configured mode, handling ASB, ASBX, or Auto restore modes.
func (r *Service) Run(ctx context.Context) error {
	if r == nil {
		return nil
	}

	// For restore and validation we init different header for log messages.
	logMessage := "restore"
	if r.config.ValidateOnly {
		logMessage = "validation"
	}

	switch r.mode {
	case models.RestoreModeASB, models.RestoreModeAuto:
		return r.run(ctx, backup.EncoderTypeASB, logMessage)
	case models.RestoreModeASBX:
		return r.run(ctx, backup.EncoderTypeASBX, logMessage)
	default:
		return r.runAuto(ctx)
	}
}

func (r *Service) run(ctx context.Context, encoderType backup.EncoderType, logMessage string) error {
	restoreType := "asb"
	if encoderType == backup.EncoderTypeASBX {
		restoreType = "asbx"
	}

	r.logger.Info(fmt.Sprintf("starting %s %s", restoreType, logMessage))

	r.config.EncoderType = encoderType
	// Run restore / validation.
	h, err := r.backupClient.Restore(ctx, r.config, r.reader)
	if err != nil {
		return fmt.Errorf("failed to start %s %s: %w", restoreType, logMessage, err)
	}
	// Run async printing files stats.
	var wg sync.WaitGroup

	wg.Go(func() {
		logging.PrintFilesNumber(ctx, r.reader.GetNumber, models.RestoreModeASB, r.logger)
	})

	go logging.PrintRestoreEstimate(ctx, h.GetStats(), h.GetMetrics, r.reader.GetSize, r.logger)

	// Wait for restore / validation to finish.
	if err = h.Wait(ctx); err != nil {
		return fmt.Errorf("failed to perform %s %s: %w", restoreType, logMessage, err)
	}

	wg.Wait()
	// Print report.
	logging.ReportRestore(h.GetStats(), r.config.ValidateOnly, r.reportToLog, r.logger)

	return nil
}

func (r *Service) runAuto(ctx context.Context) error {
	r.logger.Info("starting auto restore")
	// If one of restore operations fails, we cancel another.
	ctx, cancel := context.WithCancel(ctx)

	var (
		wg              sync.WaitGroup
		xdrStats, stats *bModels.RestoreStats
	)

	errChan := make(chan error, 2)

	if r.reader != nil {
		wg.Go(func() {
			restoreCfg := *r.config
			restoreCfg.EncoderType = backup.EncoderTypeASB

			h, err := r.backupClient.Restore(ctx, &restoreCfg, r.reader)
			if err != nil {
				errChan <- fmt.Errorf("failed to start asb restore: %w", err)

				cancel()

				return
			}

			go logging.PrintFilesNumber(ctx, r.reader.GetNumber, models.RestoreModeASB, r.logger)
			go logging.PrintRestoreEstimate(ctx, h.GetStats(), h.GetMetrics, r.reader.GetSize, r.logger)

			if err = h.Wait(ctx); err != nil {
				errChan <- fmt.Errorf("failed to perform asb restore: %w", err)

				cancel()

				return
			}

			stats = h.GetStats()
		})
	}

	if r.readerXdr != nil {
		wg.Go(func() {
			restoreXdrCfg := *r.config
			restoreXdrCfg.EncoderType = backup.EncoderTypeASBX

			hXdr, err := r.backupClient.Restore(ctx, &restoreXdrCfg, r.readerXdr)
			if err != nil {
				errChan <- fmt.Errorf("failed to start asbx restore: %w", err)

				cancel()

				return
			}

			go logging.PrintFilesNumber(ctx, r.readerXdr.GetNumber, models.RestoreModeASBX, r.logger)
			go logging.PrintRestoreEstimate(ctx, hXdr.GetStats(), hXdr.GetMetrics, r.readerXdr.GetSize, r.logger)

			if err = hXdr.Wait(ctx); err != nil {
				errChan <- fmt.Errorf("failed to perform asbx restore: %w", err)

				cancel()

				return
			}

			xdrStats = hXdr.GetStats()
		})
	}

	wg.Wait()
	close(errChan)

	// Return the first error encountered
	for err := range errChan {
		if err != nil {
			cancel()
			return err
		}
	}

	restStats := bModels.SumRestoreStats(xdrStats, stats)
	logging.ReportRestore(restStats, r.config.ValidateOnly, r.reportToLog, r.logger)

	// To prevent context leaking.
	cancel()

	return nil
}

// GetWarmUp calculates and returns the warm-up value based on the provided warmUp and maxAsyncBatches parameters.
// If warmUp is 0, it returns one greater than maxAsyncBatches. Otherwise, it returns the warmUp value.
func GetWarmUp(warmUp, maxAsyncBatches int) int {
	switch warmUp {
	case 0:
		return maxAsyncBatches + 1
	default:
		return warmUp
	}
}
