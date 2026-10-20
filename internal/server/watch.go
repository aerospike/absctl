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
	"context"
	"errors"
	"log/slog"
	"slices"
	"time"

	"github.com/aerospike/backup-go/pkg/asinfo"
)

const (
	// startedJobConfirmations is how many lookups a job the cluster has not reported yet
	// is given to turn up in the cluster status, whenever there is a reason to expect it
	// there: the cluster has just accepted a request for it, or the job is about to be
	// asked to stop. The merged status misses a job as soon as a single node does not
	// report it, and failing a command on that would report a job that exists as one
	// that does not. A job only named on the command line to report on gets no such
	// grace: the first answer about it is final.
	startedJobConfirmations = 5

	// vanishedJobConfirmations is how many lookups in a row must miss a job that was
	// reported before, before the watch accepts that it is gone. The status of a job can
	// be unavailable for a moment - between stages, or while a node is busy - and a
	// single miss must not decide the outcome of a command.
	vanishedJobConfirmations = 5
	// vanishedJobPollInterval paces the lookups made while a job is missing. A lookup also
	// costs the retries the info client makes internally, so a confirmation takes longer
	// than this interval alone: the client is built with the default retry policy, which
	// backs off over about three seconds before it reports a job as missing.
	vanishedJobPollInterval = 3 * time.Second

	// statePollInterval paces the status lookups made while a command waits for the
	// state its request leads to. The status is refreshed every 300 ms on the server,
	// but every such change takes seconds at least.
	statePollInterval = 5 * time.Second

	// statusGapLogEvery paces the reminders logged while a job is missing from the
	// cluster status, so that a long wait can be followed in a debug log without a
	// line per lookup.
	statusGapLogEvery = 20
)

// errIncompleteJobWatch is returned by a jobWatch that was built without one of the
// functions it cannot run without.
var errIncompleteJobWatch = errors.New("job watch requires lookup, handle and missing")

// progressMode says how long the watcher stays with a job, and how much it trusts the
// cluster not reporting the job at all.
type progressMode int

const (
	// reportOnce logs the status the cluster reports now and returns.
	reportOnce progressMode = iota + 1
	// followJob keeps reporting a job named on the command line until it reaches a
	// terminal state.
	followJob
	// followStartedJob does the same for the job the cluster has just accepted, and is
	// the only mode that waits for a job the status does not list yet.
	followStartedJob
)

// unseenLookups returns how many lookups a job the cluster has not reported yet is given
// in mode, see startedJobConfirmations.
func (m progressMode) unseenLookups() int {
	if m == followStartedJob {
		return startedJobConfirmations
	}

	return 1
}

// waitFor sleeps for d and returns the context error if the watch is canceled first.
func waitFor(ctx context.Context, d time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
		return nil
	}
}

// abortOutcome is what a status read while waiting for an abort says about the job.
type abortOutcome int

const (
	// abortPending means the job is still winding down, and the wait goes on.
	abortPending abortOutcome = iota + 1
	// abortStopped means the job is no longer running: the abort has landed.
	abortStopped
	// abortTooLate means the job reached the end of its work before the abort did, so
	// there was nothing left to stop.
	abortTooLate
)

// jobWatch follows the status of a single job until the caller is satisfied with what it
// reads, and decides what a job missing from the status means. Every command that sends
// a request to the cluster and then waits for the state it leads to, and every command
// that follows a job, goes through it, so that a job missing from the merged status is
// treated the same way by all of them.
//
// A lookup that does not find the job is a miss. A job that has never been reported is
// given unseenLookups lookups to turn up; a job that was reported before has to be missed
// several times in a row, see vanished. Either way, the lookups made while the job is
// missing are paced by gapInterval, and once the misses run out the outcome is up to
// missing.
//
// The wait is not bounded: a command that must not wait is run with --async, and one that
// waits does so until the job reaches the state it waits for, the job is gone, or the
// command is interrupted.
type jobWatch[S any] struct {
	logger *slog.Logger
	// attrs name the job in the lines logged while it is missing.
	attrs []any
	// gapInterval paces the lookups made while the job is missing.
	gapInterval time.Duration
	// unseenLookups is how many lookups a job that has never been reported is given.
	unseenLookups int

	// lookup reads the status of the job. A job the cluster does not report comes back
	// as an error that wraps asinfo.ErrNotFound; any other error ends the watch as it is.
	lookup func(ctx context.Context) (S, error)
	// handle is given every status that was read. It reports whether the watch is over
	// and, when it is not, how long to wait before the next lookup.
	handle func(ctx context.Context, status S) (done bool, next time.Duration, err error)
	// vanished reports whether a job that was reported before, last as last, has been
	// missing long enough to be accepted as gone. It defaults to vanishedJobConfirmations
	// misses in a row.
	vanished func(last S, misses int) bool
	// missing decides how the watch ends once the job is accepted as missing. seen tells
	// a job that was reported before from one that never was; last is the zero value in
	// the latter case.
	missing func(ctx context.Context, last S, seen bool) error
	// describe names the state of a status in the lines logged while the job is missing.
	// It is optional.
	describe func(status S) string
}

// run follows the job until handle says the watch is over, the job is accepted as
// missing, or ctx is done.
func (w *jobWatch[S]) run(ctx context.Context) error {
	if w.lookup == nil || w.handle == nil || w.missing == nil {
		return errIncompleteJobWatch
	}

	var (
		// last is the status read most recently, and seen says whether there is one.
		last S
		seen bool
		// misses counts the lookups in a row that did not find the job.
		misses int
	)

	for {
		status, err := w.lookup(ctx)

		switch {
		case err != nil && !errors.Is(err, asinfo.ErrNotFound):
			return err
		case err != nil:
			misses++

			if w.gone(last, seen, misses) {
				return w.missing(ctx, last, seen)
			}

			w.logStatusGap(last, seen, misses)

			if err := waitFor(ctx, w.gapInterval); err != nil {
				return err
			}

			continue
		}

		last, seen, misses = status, true, 0

		done, next, err := w.handle(ctx, status)
		if err != nil || done {
			return err
		}

		if err := waitFor(ctx, next); err != nil {
			return err
		}
	}
}

// gone reports whether a job missed by misses lookups in a row is to be accepted as
// missing.
func (w *jobWatch[S]) gone(last S, seen bool, misses int) bool {
	switch {
	case !seen:
		return misses >= w.unseenLookups
	case w.vanished != nil:
		return w.vanished(last, misses)
	default:
		return misses >= vanishedJobConfirmations
	}
}

// logStatusGap reports a job that is temporarily missing from the cluster status. A gap
// is a normal part of watching a job - the status is unavailable between stages, or
// while a node is busy - so none of it reaches the user: both lines are logged on the
// debug level, where a gap that outlives the routine confirmations says so, and is
// repeated while it lasts.
func (w *jobWatch[S]) logStatusGap(last S, seen bool, misses int) {
	attrs := append(slices.Clone(w.attrs), slog.Int("attempt", misses))

	if seen && w.describe != nil {
		attrs = append(attrs, slog.String("last-state", w.describe(last)))
	}

	if misses == vanishedJobConfirmations || misses%statusGapLogEvery == 0 {
		w.logger.Debug("job status is unavailable, still waiting for the job", attrs...)

		return
	}

	w.logger.Debug("job status is temporarily unavailable", attrs...)
}
