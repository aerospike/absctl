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
	"fmt"
	"log/slog"
	"math"
	"time"

	infomodels "github.com/aerospike/backup-go/pkg/asinfo/models"
	"github.com/aerospike/backup-go/pkg/estimates"
)

const (
	// heartbeatInterval is the longest silence allowed while watching a backup.
	// Adaptive printing goes quiet when the server side progress stalls, and without
	// a heartbeat the user cannot tell a slow backup from a hung one.
	heartbeatInterval = time.Minute
	// minPollInterval keeps the watcher responsive at the start of a backup and on
	// short ones, where the adaptive interval collapses to sub-second values.
	minPollInterval = time.Second
	// maxPollInterval bounds the back off on very long backups. It stays below
	// heartbeatInterval so that every heartbeat is backed by a fresh sample.
	maxPollInterval = 30 * time.Second
	// speedSmoothing is the weight of the latest sample in the exponentially weighted
	// moving average of the backup speed.
	speedSmoothing = 0.3
	// percentScale converts a server reported percentage into a [0, 1] ratio.
	percentScale = 100
)

// progressPrinter decides whether a backup status update carries enough new information
// to be worth a log line, and formats it. A single watch loop owns the printer, it is
// not safe for concurrent use.
type progressPrinter struct {
	logger *slog.Logger

	// state, lastRatio and lastPrinted describe the last line that was emitted.
	state       infomodels.BackupState
	lastRatio   float64
	lastPrinted time.Time

	// sampledAt, sampledRecs and recsPerSec hold the speed measurement.
	sampledAt   time.Time
	sampledRecs int
	recsPerSec  float64
}

// newProgressPrinter returns a printer that has not emitted anything yet.
func newProgressPrinter(logger *slog.Logger) *progressPrinter {
	return &progressPrinter{logger: logger}
}

// Print logs the given status if it carries new information, and reports whether it did.
// now is passed in so that the decision is reproducible under test.
func (p *progressPrinter) Print(status *infomodels.ResponseBackupState, now time.Time) bool {
	ratio := progressRatio(status)
	elapsed := elapsedSince(status.StartTime, now)

	p.sample(status, now)

	if !p.shouldPrint(status, ratio, elapsed, now) {
		return false
	}

	attrs := []any{
		slog.String("backup-id", status.JobID),
		slog.String("state", status.State.Describe()),
		slog.String("pct", fmt.Sprintf("%.1f%%", status.ProgressPct)),
		slog.Int("records", status.RecsBackedUp),
		slog.String("partitions", fmt.Sprintf("%d/%d", status.PartitionsScanned, status.PartitionsOwned)),
		slog.Uint64("records-per-second", uint64(math.Round(p.recsPerSec))),
	}

	if remaining := estimates.RemainingTime(elapsed, ratio); remaining > 0 {
		attrs = append(attrs, slog.String("remaining", remaining.Round(time.Second).String()))
	}

	p.logger.Info("backup progress", attrs...)

	p.state = status.State
	p.lastRatio = ratio
	p.lastPrinted = now

	return true
}

// shouldPrint reports whether the status is worth a log line. The first observation, a
// stage transition and an expired heartbeat are always printed. Otherwise the progress
// must have advanced by at least the threshold derived from the projected duration of
// the backup, so that a multi hour backup does not flood the log.
func (p *progressPrinter) shouldPrint(
	status *infomodels.ResponseBackupState,
	ratio float64,
	elapsed time.Duration,
	now time.Time,
) bool {
	switch {
	case p.lastPrinted.IsZero():
		return true
	case status.State != p.state:
		return true
	case now.Sub(p.lastPrinted) >= heartbeatInterval:
		return true
	}

	return ratio-p.lastRatio >= estimates.ProgressThreshold(elapsed, ratio)
}

// sample updates the exponentially weighted moving average of the backup speed. The
// record counters mean different things in different stages, so the measurement starts
// over whenever the backup moves on to the next stage.
func (p *progressPrinter) sample(status *infomodels.ResponseBackupState, now time.Time) {
	if p.sampledAt.IsZero() || status.State != p.state {
		p.sampledAt = now
		p.sampledRecs = status.RecsBackedUp
		p.recsPerSec = 0

		return
	}

	seconds := now.Sub(p.sampledAt).Seconds()
	if seconds <= 0 {
		return
	}

	// A counter can go backwards when a node drops out of the merged status.
	instant := max(float64(status.RecsBackedUp-p.sampledRecs)/seconds, 0)

	if p.recsPerSec == 0 {
		p.recsPerSec = instant
	} else {
		p.recsPerSec = speedSmoothing*instant + (1-speedSmoothing)*p.recsPerSec
	}

	p.sampledAt = now
	p.sampledRecs = status.RecsBackedUp
}

// nextPoll returns how long to wait before requesting the next status. It is derived
// from the same projection that gates printing, so the watcher asks the cluster only as
// often as it could produce a new line: about every estimates.TargetPrintInterval on an
// average backup, faster on a short one and backing off on a long one.
func nextPoll(status *infomodels.ResponseBackupState, now time.Time) time.Duration {
	ratio := progressRatio(status)

	elapsed := elapsedSince(status.StartTime, now)
	if elapsed < estimates.EstimateWarmup || ratio <= 0 {
		return minPollInterval
	}

	totalDuration := float64(elapsed) / ratio
	interval := time.Duration(estimates.ProgressThreshold(elapsed, ratio) * totalDuration)

	return min(max(interval, minPollInterval), maxPollInterval)
}

// progressRatio converts the server reported completion percentage into a [0, 1] ratio.
func progressRatio(status *infomodels.ResponseBackupState) float64 {
	return status.ProgressPct / percentScale
}

// elapsedSince returns how long the backup has been running, or zero when the cluster
// did not report a usable start time.
func elapsedSince(startTime, now time.Time) time.Duration {
	if startTime.IsZero() || now.Before(startTime) {
		return 0
	}

	return now.Sub(startTime)
}
