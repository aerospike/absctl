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
	"testing"
	"time"

	"github.com/aerospike/backup-go/pkg/asinfo"
	infomodels "github.com/aerospike/backup-go/pkg/asinfo/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	logAbortingRestore = "aborting restore"
	logRestoreAborted  = "restore was successfully aborted"
	logRestoreTooLate  = "restore finished before it could be aborted"
)

// TestServiceAbortRestore covers the outcomes the restore abort reaches within a few
// polls. A canceled command is driven separately below.
//
// The first status lookup is the pre-abort check, so every case that gets as far as
// requesting the abort opens with a status that reports the job as running.
func TestServiceAbortRestore(t *testing.T) {
	t.Parallel()

	// coldActive and fuzzyActive are the answers the pre-abort check needs to let the
	// abort through.
	coldActive := restoreAnswer{status: testColdStatus(infomodels.RestoreStateActive, infomodels.NamespaceStateRestoring)}
	fuzzyActive := restoreAnswer{status: testFuzzyStatus(infomodels.RestoreStateActive)}
	coldAborted := restoreAnswer{status: testColdStatus(infomodels.RestoreStateAborted, infomodels.NamespaceStateRestoring)}
	fuzzyAborted := restoreAnswer{status: testFuzzyStatus(infomodels.RestoreStateAborted)}
	notFound := restoreAnswer{err: asinfo.ErrNotFound}

	tests := []struct {
		name           string
		async          bool
		abortErr       error
		answers        []restoreAnswer
		wantErr        error
		wantCalls      int
		wantAbortCalls int
		wantLogs       []string
		notWantLogs    []string
	}{
		{
			// An aborted cold restore keeps the namespace closed, so the abort ends by
			// saying how to release it.
			name:           "confirms the abort of a cold restore and tells how to open the namespace",
			answers:        []restoreAnswer{coldActive, coldAborted},
			wantCalls:      2,
			wantAbortCalls: 1,
			wantLogs:       []string{logAbortingRestore, logRestoreAborted, logOpenHint},
		},
		{
			name:           "confirms the abort of a fuzzy restore without the open hint",
			answers:        []restoreAnswer{fuzzyActive, fuzzyAborted},
			wantCalls:      2,
			wantAbortCalls: 1,
			wantLogs:       []string{logAbortingRestore, logRestoreAborted},
			notWantLogs:    []string{logOpenHint},
		},
		{
			// A single node that does not report the job hides it from the status,
			// which says nothing about whether the job stopped.
			name:           "waits through a job missing from the status",
			answers:        []restoreAnswer{fuzzyActive, notFound, fuzzyAborted},
			wantCalls:      3,
			wantAbortCalls: 1,
			wantLogs:       []string{logRestoreAborted},
		},
		{
			// The same holds before the abort: a job a few lookups missed is still
			// running on the other nodes, and still has to be aborted.
			name: "aborts a job the pre-abort check missed a few times",
			answers: append(
				repeatRestoreNotFound(startedJobConfirmations-1),
				fuzzyActive,
				fuzzyAborted,
			),
			wantCalls:      startedJobConfirmations + 1,
			wantAbortCalls: 1,
			wantLogs:       []string{logAbortingRestore, logRestoreAborted},
		},
		{
			// The abort is not confirmed, but the namespace of a cold restore is left
			// closed either way, so the hint is not held back.
			name:           "returns as soon as the abort of a cold restore is accepted when async",
			async:          true,
			answers:        []restoreAnswer{coldActive},
			wantCalls:      1,
			wantAbortCalls: 1,
			wantLogs:       []string{logAbortingRestore, logOpenHint},
			notWantLogs:    []string{logRestoreAborted},
		},
		{
			name:           "returns as soon as the abort of a fuzzy restore is accepted when async",
			async:          true,
			answers:        []restoreAnswer{fuzzyActive},
			wantCalls:      1,
			wantAbortCalls: 1,
			wantLogs:       []string{logAbortingRestore},
			notWantLogs:    []string{logRestoreAborted, logOpenHint},
		},
		{
			// The cluster keeps the state of a stopped restore job, so a job the status
			// no longer reports was not confirmed as aborted, and must not be announced
			// as such.
			name:           "fails when the job is not reported after the abort",
			answers:        []restoreAnswer{fuzzyActive},
			wantErr:        errRestoreStatusLost,
			wantCalls:      1 + startedJobConfirmations,
			wantAbortCalls: 1,
			notWantLogs:    []string{logRestoreAborted},
		},
		{
			name: "fails when the job stops reporting while winding down",
			answers: append(
				[]restoreAnswer{fuzzyActive, {status: testFuzzyStatus(infomodels.RestoreStateAborting)}},
				repeatRestoreNotFound(vanishedJobConfirmations)...,
			),
			wantErr:        errRestoreStatusLost,
			wantCalls:      2 + vanishedJobConfirmations,
			wantAbortCalls: 1,
			notWantLogs:    []string{logRestoreAborted},
		},
		{
			name: "waits for a job that is still winding down",
			answers: []restoreAnswer{
				coldActive,
				{status: testColdStatus(infomodels.RestoreStateAborting, infomodels.NamespaceStateRestoring)},
				coldAborted,
			},
			wantCalls:      3,
			wantAbortCalls: 1,
			wantLogs:       []string{logRestoreAborted, logOpenHint},
		},
		{
			// A job that reports FAILED stopped working, so there is nothing left to
			// wait for.
			name: "stops waiting for a job that failed after the abort",
			answers: []restoreAnswer{
				coldActive,
				{status: testColdStatus(infomodels.RestoreStateAborting, infomodels.NamespaceStateRestoring)},
				{status: testColdStatus(infomodels.RestoreStateFailed, infomodels.NamespaceStateRestoring)},
			},
			wantCalls:      3,
			wantAbortCalls: 1,
			wantLogs:       []string{logRestoreAborted, logOpenHint},
		},
		{
			// A prepared cold restore is something to abort as well.
			name: "aborts a cold restore that is only prepared",
			answers: []restoreAnswer{
				{status: testColdStatus(infomodels.RestoreStateActive, infomodels.NamespaceStateReady)},
				coldAborted,
			},
			wantCalls:      2,
			wantAbortCalls: 1,
			wantLogs:       []string{logAbortingRestore, logRestoreAborted},
		},
		{
			// The data of a job that completed before the abort landed is in the
			// namespace, so the job was neither aborted nor needs opening.
			name: "reports a job that completed before the abort landed",
			answers: []restoreAnswer{
				coldActive,
				{status: testColdStatus(infomodels.RestoreStateComplete, infomodels.NamespaceStateNone)},
			},
			wantCalls:      2,
			wantAbortCalls: 1,
			wantLogs:       []string{logRestoreTooLate},
			notWantLogs:    []string{logRestoreAborted, logOpenHint},
		},
		{
			// The cluster accepts the abort of a job it knows nothing about, and the
			// status that follows would announce a restore that never ran as aborted.
			name:           "does not abort a job the cluster does not report",
			wantCalls:      startedJobConfirmations,
			wantAbortCalls: 0,
			wantLogs:       []string{msgNoRunningRestore},
			notWantLogs:    []string{logAbortingRestore, logRestoreAborted},
		},
		{
			name:           "does not abort a job that completed",
			answers:        []restoreAnswer{{status: testFuzzyStatus(infomodels.RestoreStateComplete)}},
			wantCalls:      1,
			wantAbortCalls: 0,
			wantLogs:       []string{msgNoRunningRestore},
			notWantLogs:    []string{logAbortingRestore, logRestoreAborted},
		},
		{
			name:           "does not abort a job that was aborted already",
			answers:        []restoreAnswer{fuzzyAborted},
			wantCalls:      1,
			wantAbortCalls: 0,
			wantLogs:       []string{msgNoRunningRestore},
			notWantLogs:    []string{logAbortingRestore, logRestoreAborted},
		},
		{
			// A failed job still runs until it is aborted, which is what releases its
			// namespace for the open.
			name:           "aborts a job that failed",
			answers:        []restoreAnswer{{status: testColdStatus(infomodels.RestoreStateFailed, infomodels.NamespaceStateRestoring)}, coldAborted},
			wantCalls:      2,
			wantAbortCalls: 1,
			wantLogs:       []string{logRestoreAborted, logOpenHint},
		},
		{
			// An answer that carries no status is as good as no answer.
			name:           "does not abort on an empty status",
			answers:        []restoreAnswer{{}},
			wantCalls:      startedJobConfirmations,
			wantAbortCalls: 0,
			wantLogs:       []string{msgNoRunningRestore},
		},
		{
			// Nothing was aborted, so there is nothing to confirm.
			name:           "propagates a rejected abort request without polling",
			answers:        []restoreAnswer{fuzzyActive},
			abortErr:       errBoom,
			wantErr:        errBoom,
			wantCalls:      1,
			wantAbortCalls: 1,
			notWantLogs:    []string{logAbortingRestore, logRestoreAborted},
		},
		{
			// A check that cannot tell whether a restore runs must not guess, and must
			// not send the abort either.
			name:           "propagates an unexpected error from the pre-abort check",
			answers:        []restoreAnswer{{err: errBoom}},
			wantErr:        errBoom,
			wantCalls:      1,
			wantAbortCalls: 0,
			notWantLogs:    []string{logAbortingRestore, logRestoreAborted},
		},
		{
			name:           "propagates an unexpected status error",
			answers:        []restoreAnswer{fuzzyActive, {err: errBoom}},
			wantErr:        errBoom,
			wantCalls:      2,
			wantAbortCalls: 1,
			notWantLogs:    []string{logRestoreAborted},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			svc, buf := newTestService(t)
			svc.restoreCfg.Abort.Async = tt.async
			client := &fakeRestoreInfoClient{answers: tt.answers, abortErr: tt.abortErr}

			err := svc.abortRestore(t.Context(), client)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}

			assert.Equal(t, tt.wantAbortCalls, client.abortCalls)
			assert.Equal(t, tt.wantCalls, client.calls)

			// A restore job is named by its namespace as well as by its id, so both have
			// to reach the cluster.
			if tt.wantAbortCalls > 0 {
				assert.Equal(t, testNamespace, client.abortNamespace)
				assert.Equal(t, testJobID, client.abortJobID)
			}

			for _, want := range tt.wantLogs {
				assert.Contains(t, buf.String(), want)
			}

			for _, notWant := range tt.notWantLogs {
				assert.NotContains(t, buf.String(), notWant)
			}
		})
	}
}

// TestServiceAbortRestoreCanceled covers the only thing that ends the wait for a job that
// keeps winding down: the wait is not bounded, so a command the user interrupted has to
// end at once, as canceled, instead of sitting out the poll interval.
func TestServiceAbortRestoreCanceled(t *testing.T) {
	t.Parallel()

	svc, buf := newTestService(t)
	// A poll interval longer than the test would tolerate: only the cancellation can
	// end this wait.
	svc.statePollInterval = time.Minute

	ctx, cancel := context.WithCancel(t.Context())

	client := &fakeRestoreInfoClient{
		answers:    []restoreAnswer{{status: testFuzzyStatus(infomodels.RestoreStateActive)}},
		repeatLast: true,
	}

	cancel()

	err := svc.abortRestore(ctx, client)

	require.ErrorIs(t, err, context.Canceled)
	assert.NotContains(t, buf.String(), logRestoreAborted)
}

// TestServiceAbortRestoreCanceledWhileConfirming makes sure a command interrupted while
// the pre-abort check confirms a missing job ends at once, as canceled, and neither
// sends the abort nor reports that there was nothing to abort.
func TestServiceAbortRestoreCanceledWhileConfirming(t *testing.T) {
	t.Parallel()

	svc, buf := newTestService(t)
	// A confirmation interval longer than the test would tolerate: only the
	// cancellation can end this wait.
	svc.vanishedPollInterval = time.Minute

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	client := &fakeRestoreInfoClient{}

	err := svc.abortRestore(ctx, client)

	require.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, 1, client.calls)
	assert.Zero(t, client.abortCalls)
	assert.NotContains(t, buf.String(), msgNoRunningRestore)
}
