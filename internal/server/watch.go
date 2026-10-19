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
	"fmt"
	"time"
)

const (
	// startedJobConfirmations is how many lookups a job the cluster has just accepted is
	// given to turn up in the cluster status. "backup start" follows the job the cluster
	// handed it, so that job does exist, and reporting it as missing because the status
	// does not list it yet would fail a command that has just succeeded. A job named on
	// the command line gets no such grace: the first answer about it is final.
	startedJobConfirmations = 5

	// vanishedJobConfirmations is how many lookups in a row must miss a job that was
	// being watched and had reached its last stages, before the watcher accepts that the
	// cluster dropped the state of a finished backup. The status of a running job can be
	// unavailable for a moment - between backup stages, or while a node is busy - and a
	// single miss must not be reported as a finished backup.
	//
	// A job that has never been reported is not confirmed this way: it has no state to
	// resolve, and startedJobConfirmations decides how long it is waited for.
	vanishedJobConfirmations = 5
	// vanishedJobPollInterval paces those confirmation lookups. A lookup also costs the
	// retries the info client makes internally, so a confirmation takes longer than this
	// interval alone: the client is built with the default retry policy, which backs off
	// over about three seconds before it reports a job as missing.
	vanishedJobPollInterval = 3 * time.Second

	// abortStatusTimeout bounds the wait for an aborted job to stop running, and
	// abortStatusPollInterval paces the status lookups made while waiting.
	abortStatusTimeout      = 5 * time.Minute
	abortStatusPollInterval = 5 * time.Second

	// statePollInterval paces the status lookups of "snapshot-restore open" and
	// "snapshot-restore clear-storage". The status is refreshed every 300 ms on the
	// server, but both commands wait for a change that takes seconds at least.
	statePollInterval = 5 * time.Second
)

// progressMode says how long the watcher stays with a job, and how much it trusts the
// cluster not reporting the job at all.
type progressMode int

const (
	// reportOnce logs the status the cluster reports now and returns.
	reportOnce progressMode = iota + 1
	// followJob keeps reporting a job named on the command line until the backup reaches
	// a terminal state.
	followJob
	// followStartedJob does the same for the job the cluster has just accepted, and is
	// the only mode that waits for a job the status does not list yet.
	followStartedJob
)

// startingJobPending reports whether a job the cluster has not listed yet is still worth
// waiting for. Only the job the cluster has just handed out is: it exists, and the status
// it is missing from is the one that has to catch up. A job named on the command line is
// not, however many times it is looked up.
func startingJobPending(mode progressMode, misses int) bool {
	return mode == followStartedJob && misses < startedJobConfirmations
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

// waitForAbort polls the state of an aborted job until it stops running, and reports how
// it stopped. The cluster accepts an abort request before the job actually winds down, so
// the abort is confirmed by the status that follows it.
//
// The wait is bounded: a job that never reacts to the abort has to end the command instead
// of being polled forever, and the deadline also cuts short a status request that is
// already in flight. timeoutErr is what a job that kept reporting itself as running until
// the wait ran out is reported as; poll returns the state it read along with its meaning,
// so that the failure can name what the job was last doing.
func (s *Service) waitForAbort(
	ctx context.Context,
	timeoutErr error,
	poll func(ctx context.Context) (abortOutcome, string, error),
) (abortOutcome, error) {
	statusCtx, cancel := context.WithTimeout(ctx, s.abortTimeout)
	defer cancel()

	for {
		outcome, state, err := poll(statusCtx)

		// The terminal outcomes are named rather than taken as "anything but pending", so
		// that an outcome no poll is supposed to return ends in the bounded wait running
		// out instead of announcing a job that never stopped as stopped.
		switch {
		case errors.Is(err, context.DeadlineExceeded):
			return 0, timeoutErr
		case err != nil:
			return 0, err
		case outcome == abortStopped, outcome == abortTooLate:
			return outcome, nil
		}

		// waitFor returns as soon as the command is canceled or the wait runs out, so
		// the abort never keeps polling a job the caller stopped waiting for.
		if err := waitFor(statusCtx, s.abortPollInterval); err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				return 0, fmt.Errorf("%w, last reported state %q", timeoutErr, state)
			}

			return 0, err
		}
	}
}

// waitForState polls until poll reports that the awaited state was reached. poll returns
// the state it read along with whether it is the awaited one, so that a wait that runs
// out can name what the job was last doing. A timeout of zero leaves the wait unbounded;
// otherwise timeoutErr is what a wait that runs out is reported as, and the deadline
// also cuts short a status request that is already in flight.
func (s *Service) waitForState(
	ctx context.Context,
	timeout time.Duration,
	timeoutErr error,
	poll func(ctx context.Context) (bool, string, error),
) error {
	if timeout > 0 {
		var cancel context.CancelFunc

		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	for {
		done, state, err := poll(ctx)

		switch {
		case timeout > 0 && errors.Is(err, context.DeadlineExceeded):
			return timeoutErr
		case err != nil:
			return err
		case done:
			return nil
		}

		if err := waitFor(ctx, s.statePollInterval); err != nil {
			if timeout > 0 && errors.Is(err, context.DeadlineExceeded) {
				return fmt.Errorf("%w, last reported state %q", timeoutErr, state)
			}

			return err
		}
	}
}
