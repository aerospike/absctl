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
	"log/slog"
	"math"
	"time"

	"github.com/aerospike/backup-go/pkg/estimates"
)

const (
	// heartbeatInterval is the longest silence allowed while watching a job.
	// Adaptive printing goes quiet when the server side progress stalls, and without
	// a heartbeat the user cannot tell a slow job from a hung one.
	heartbeatInterval = time.Minute
	// minPollInterval keeps the watcher responsive at the start of a job and on
	// short ones, where the adaptive interval collapses to sub-second values.
	minPollInterval = time.Second
	// maxPollInterval bounds the back off on very long jobs. It stays below
	// heartbeatInterval so that every heartbeat is backed by a fresh sample.
	maxPollInterval = 30 * time.Second
	// speedSmoothing is the weight of the latest sample in the exponentially weighted
	// moving average of the job speed.
	speedSmoothing = 0.3
	// percentScale converts a server reported percentage into a [0, 1] ratio.
	percentScale = 100
	// pctPrecision rounds the reported percentage to two decimals, the same precision
	// the client side backup progress is printed with.
	pctPrecision = 100
)

// progressSnapshot is what the progress printer needs to know about a job at one point
// in time, whichever kind of job it is.
type progressSnapshot struct {
	// attrs name the job and its state. They lead every line printed for the snapshot.
	attrs []any
	// stage changes whenever the job moves on to a stage in which its counters mean
	// something else, which restarts the speed measurement.
	stage string
	// ratio is the completion of the job, in [0, 1].
	ratio float64
	// records is the counter the speed is measured with, when hasRecords is set. A job
	// that counts no records in its current stage reports no speed.
	records    uint64
	hasRecords bool
	// startTime is zero when the cluster reported no usable start time.
	startTime time.Time
}

// progressPrinter decides whether a progress update carries enough new information to
// be worth a log line, and formats it. A single watch loop owns the printer, it is not
// safe for concurrent use.
type progressPrinter struct {
	logger *slog.Logger
	// msg is the message every line is logged under.
	msg string

	// stage, lastRatio and lastPrinted describe the last line that was emitted.
	stage       string
	lastRatio   float64
	lastPrinted time.Time

	// sampledAt, sampledRecs and recsPerSec hold the speed measurement.
	sampledAt   time.Time
	sampledRecs uint64
	recsPerSec  float64
}

// newProgressPrinter returns a printer that has not emitted anything yet.
func newProgressPrinter(logger *slog.Logger, msg string) *progressPrinter {
	return &progressPrinter{logger: logger, msg: msg}
}

// Print logs the given snapshot if it carries new information, and reports whether it
// did. now is passed in so that the decision is reproducible under test.
func (p *progressPrinter) Print(snap *progressSnapshot, now time.Time) bool {
	elapsed := elapsedSince(snap.startTime, now)

	p.sample(snap, now)

	if !p.shouldPrint(snap.stage, snap.ratio, elapsed, now) {
		return false
	}

	attrs := append(make([]any, 0, len(snap.attrs)+3), snap.attrs...)
	attrs = append(attrs, slog.Float64("pct", math.Round(snap.ratio*percentScale*pctPrecision)/pctPrecision))

	if snap.hasRecords {
		attrs = append(attrs, slog.Uint64("rec/s", uint64(math.Round(p.recsPerSec))))
	}

	if remaining := estimates.RemainingTime(elapsed, snap.ratio); remaining > 0 {
		attrs = append(attrs, slog.String("remaining", remaining.Round(time.Second).String()))
	}

	p.logger.Info(p.msg, attrs...)

	p.stage = snap.stage
	p.lastRatio = snap.ratio
	p.lastPrinted = now

	return true
}

// shouldPrint reports whether the snapshot is worth a log line. The first observation, a
// stage transition and an expired heartbeat are always printed. Otherwise the progress
// must have advanced by at least the threshold derived from the projected duration of
// the job, so that a multi hour job does not flood the log.
func (p *progressPrinter) shouldPrint(stage string, ratio float64, elapsed time.Duration, now time.Time) bool {
	switch {
	case p.lastPrinted.IsZero():
		return true
	case stage != p.stage:
		return true
	case now.Sub(p.lastPrinted) >= heartbeatInterval:
		return true
	}

	return ratio-p.lastRatio >= estimates.ProgressThreshold(elapsed, ratio)
}

// sample updates the exponentially weighted moving average of the job speed. The
// record counters mean different things in different stages, so the measurement starts
// over whenever the job moves on to the next stage.
func (p *progressPrinter) sample(snap *progressSnapshot, now time.Time) {
	if p.sampledAt.IsZero() || snap.stage != p.stage {
		p.sampledAt = now
		p.sampledRecs = snap.records
		p.recsPerSec = 0

		return
	}

	seconds := now.Sub(p.sampledAt).Seconds()
	if seconds <= 0 {
		return
	}

	// The counter can go backwards: the backup one sums the base, incremental and
	// change lines instead of counting distinct records, and a node dropping out of the
	// merged status takes its share with it. Subtracting unsigned counters would wrap
	// around, so a drop is read as no progress at all.
	var instant float64
	if snap.records > p.sampledRecs {
		instant = float64(snap.records-p.sampledRecs) / seconds
	}

	if p.recsPerSec == 0 {
		p.recsPerSec = instant
	} else {
		p.recsPerSec = speedSmoothing*instant + (1-speedSmoothing)*p.recsPerSec
	}

	p.sampledAt = now
	p.sampledRecs = snap.records
}

// nextPoll returns how long to wait before requesting the next status. It is derived
// from the same projection that gates printing, so the watcher asks the cluster only as
// often as it could produce a new line: about every estimates.TargetPrintInterval on an
// average job, faster on a short one and backing off on a long one.
func nextPoll(snap *progressSnapshot, now time.Time) time.Duration {
	elapsed := elapsedSince(snap.startTime, now)
	if elapsed < estimates.EstimateWarmup || snap.ratio <= 0 {
		return minPollInterval
	}

	totalDuration := float64(elapsed) / snap.ratio
	interval := time.Duration(estimates.ProgressThreshold(elapsed, snap.ratio) * totalDuration)

	return min(max(interval, minPollInterval), maxPollInterval)
}

// elapsedSince returns how long the job has been running, or zero when the cluster
// did not report a usable start time.
func elapsedSince(startTime, now time.Time) time.Duration {
	if startTime.IsZero() || now.Before(startTime) {
		return 0
	}

	return now.Sub(startTime)
}

// jobDuration returns how long the job took, and whether the cluster reported both
// ends of it: the finish time is only set once the job reaches a terminal state.
func jobDuration(startTime, finishTime time.Time) (time.Duration, bool) {
	if startTime.IsZero() || finishTime.IsZero() || finishTime.Before(startTime) {
		return 0, false
	}

	return finishTime.Sub(startTime), true
}
