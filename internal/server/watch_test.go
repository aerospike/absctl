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
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/aerospike/backup-go/pkg/asinfo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	// testWatchRunning, testWatchDone and testWatchBroken are the states of the job the
	// watch tests follow: one that keeps the watch going, one that ends it, and one the
	// handler fails on.
	testWatchRunning = "running"
	testWatchDone    = "done"
	testWatchBroken  = "broken"
)

// watchAnswer is a single canned answer of the lookup the watch tests run.
type watchAnswer struct {
	status string
	err    error
}

// repeatWatchNotFound builds n answers that report the job as missing.
func repeatWatchNotFound(n int) []watchAnswer {
	answers := make([]watchAnswer, n)
	for i := range answers {
		answers[i] = watchAnswer{err: asinfo.ErrNotFound}
	}

	return answers
}

// watchRecorder replays canned lookup answers, one per call, keeps reporting the job as
// missing once they run out, and records how the watch ended.
type watchRecorder struct {
	answers []watchAnswer
	calls   int

	missingErr    error
	missingCalled bool
	missingSeen   bool
	missingLast   string
}

func (r *watchRecorder) lookup(context.Context) (string, error) {
	call := r.calls
	r.calls++

	if call >= len(r.answers) {
		// The info client wraps the missing job, so the watch must not rely on the
		// bare sentinel.
		return "", fmt.Errorf("failed to get status: %w", asinfo.ErrNotFound)
	}

	return r.answers[call].status, r.answers[call].err
}

func (r *watchRecorder) missing(_ context.Context, last string, seen bool) error {
	r.missingCalled = true
	r.missingSeen = seen
	r.missingLast = last

	return r.missingErr
}

// newTestWatch builds a watch over r that polls fast enough not to slow the tests down,
// ends on testWatchDone and fails on testWatchBroken.
func newTestWatch(r *watchRecorder, unseenLookups int) *jobWatch[string] {
	return &jobWatch[string]{
		logger:        slog.New(slog.DiscardHandler),
		gapInterval:   time.Millisecond,
		unseenLookups: unseenLookups,
		lookup:        r.lookup,
		handle: func(_ context.Context, status string) (bool, time.Duration, error) {
			if status == testWatchBroken {
				return true, 0, errBoom
			}

			return status == testWatchDone, time.Millisecond, nil
		},
		missing:  r.missing,
		describe: func(status string) string { return status },
	}
}

func TestJobWatchRun(t *testing.T) {
	t.Parallel()

	running := watchAnswer{status: testWatchRunning}
	done := watchAnswer{status: testWatchDone}
	notFound := watchAnswer{err: asinfo.ErrNotFound}

	// vanishedAfterTwo replaces the default confirmations of a job that was reported
	// before.
	vanishedAfterTwo := func(_ string, misses int) bool { return misses >= 2 }
	vanishedAfterOne := func(_ string, misses int) bool { return misses >= 1 }

	tests := []struct {
		name          string
		unseenLookups int
		// vanished is left nil for the default confirmations.
		vanished    func(last string, misses int) bool
		answers     []watchAnswer
		missingErr  error
		wantErr     error
		wantCalls   int
		wantMissing bool
		wantSeen    bool
		wantLast    string
	}{
		{
			name:          "ends when the handler is satisfied",
			unseenLookups: 1,
			answers:       []watchAnswer{running, running, done},
			wantCalls:     3,
		},
		{
			// A job named on the command line gets no grace: the first answer about
			// it is final.
			name:          "gives up on a job never reported after a single lookup",
			unseenLookups: 1,
			wantCalls:     1,
			wantMissing:   true,
		},
		{
			name:          "waits for a job to turn up in the status",
			unseenLookups: startedJobConfirmations,
			answers:       append(repeatWatchNotFound(startedJobConfirmations-1), done),
			wantCalls:     startedJobConfirmations,
		},
		{
			name:          "gives up on a job that never turns up",
			unseenLookups: startedJobConfirmations,
			wantCalls:     startedJobConfirmations,
			wantMissing:   true,
		},
		{
			name:          "accepts a reported job as gone after the confirmations",
			unseenLookups: 1,
			answers:       []watchAnswer{running},
			wantCalls:     1 + vanishedJobConfirmations,
			wantMissing:   true,
			wantSeen:      true,
			wantLast:      testWatchRunning,
		},
		{
			name:          "asks vanished how long a reported job is waited for",
			unseenLookups: 1,
			vanished:      vanishedAfterTwo,
			answers:       []watchAnswer{running},
			wantCalls:     3,
			wantMissing:   true,
			wantSeen:      true,
			wantLast:      testWatchRunning,
		},
		{
			// Only misses in a row count: a job that turns up again starts over, so
			// gaps shorter than the confirmations never end the watch.
			name:          "starts the confirmations over when the job turns up again",
			unseenLookups: 1,
			answers: append(
				append(
					append([]watchAnswer{running}, repeatWatchNotFound(vanishedJobConfirmations-1)...),
					running,
				),
				append(repeatWatchNotFound(vanishedJobConfirmations-1), done)...,
			),
			wantCalls: 2*vanishedJobConfirmations + 1,
		},
		{
			name:          "passes on what missing decides",
			unseenLookups: 1,
			answers:       []watchAnswer{running, notFound},
			vanished:      vanishedAfterOne,
			missingErr:    errBoom,
			wantErr:       errBoom,
			wantCalls:     2,
			wantMissing:   true,
			wantSeen:      true,
			wantLast:      testWatchRunning,
		},
		{
			// An error that is not a missing job says nothing about the job, so the
			// watch neither waits it out nor asks missing about it.
			name:          "propagates an unexpected lookup error",
			unseenLookups: startedJobConfirmations,
			answers:       []watchAnswer{{err: errBoom}},
			wantErr:       errBoom,
			wantCalls:     1,
		},
		{
			name:          "propagates a handler error",
			unseenLookups: 1,
			answers:       []watchAnswer{running, {status: testWatchBroken}},
			wantErr:       errBoom,
			wantCalls:     2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r := &watchRecorder{answers: tt.answers, missingErr: tt.missingErr}
			w := newTestWatch(r, tt.unseenLookups)
			w.vanished = tt.vanished

			err := w.run(t.Context())

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}

			assert.Equal(t, tt.wantCalls, r.calls)
			assert.Equal(t, tt.wantMissing, r.missingCalled)
			assert.Equal(t, tt.wantSeen, r.missingSeen)
			assert.Equal(t, tt.wantLast, r.missingLast)
		})
	}
}

// TestJobWatchRunIncomplete makes sure a watch built without one of the functions it
// cannot run without is refused before it reads anything, instead of panicking on the
// first status or the first miss.
func TestJobWatchRunIncomplete(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		remove func(w *jobWatch[string])
	}{
		{name: "without lookup", remove: func(w *jobWatch[string]) { w.lookup = nil }},
		{name: "without handle", remove: func(w *jobWatch[string]) { w.handle = nil }},
		{name: "without missing", remove: func(w *jobWatch[string]) { w.missing = nil }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r := &watchRecorder{answers: []watchAnswer{{status: testWatchDone}}}
			w := newTestWatch(r, 1)
			tt.remove(w)

			err := w.run(t.Context())

			require.ErrorIs(t, err, errIncompleteJobWatch)
			assert.Zero(t, r.calls)
		})
	}
}

// TestJobWatchRunCanceled covers the only thing that ends a wait the job itself does not
// end: the watch is not bounded, so a command the user interrupted has to end at once,
// as canceled, whether it waits out a gap in the status or the next poll.
func TestJobWatchRunCanceled(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		answers []watchAnswer
	}{
		{
			name: "while the job is missing",
		},
		{
			name:    "while waiting for the next poll",
			answers: []watchAnswer{{status: testWatchRunning}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r := &watchRecorder{answers: tt.answers}
			w := newTestWatch(r, startedJobConfirmations)
			// Intervals longer than the test would tolerate: only the cancellation can
			// end this wait.
			w.gapInterval = time.Minute
			w.handle = func(context.Context, string) (bool, time.Duration, error) {
				return false, time.Minute, nil
			}

			ctx, cancel := context.WithCancel(t.Context())
			cancel()

			err := w.run(ctx)

			require.ErrorIs(t, err, context.Canceled)
			assert.Equal(t, 1, r.calls)
			assert.False(t, r.missingCalled)
		})
	}
}

func TestProgressModeUnseenLookups(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		give progressMode
		want int
	}{
		{name: "report once", give: reportOnce, want: 1},
		{name: "follow a job named on the command line", give: followJob, want: 1},
		{name: "follow a just-started job", give: followStartedJob, want: startedJobConfirmations},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, tt.give.unseenLookups())
		})
	}
}
