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
	"testing"
	"time"

	infomodels "github.com/aerospike/backup-go/pkg/asinfo/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	logAborting     = "aborting"
	logAborted      = "backup was successfully aborted"
	logAbortTooLate = "backup finished before it could be aborted"
)

// TestServiceAbortBackup covers the outcomes the abort reaches within a few polls. The
// two that only time can produce - the bounded wait and a canceled command - are driven
// separately below.
//
// The first status lookup is the pre-abort check, so every case that gets as far as
// requesting the abort opens with a status that reports the job as running.
func TestServiceAbortBackup(t *testing.T) {
	t.Parallel()

	// runningStatus is the answer the pre-abort check needs to let the abort through.
	runningStatus := statusAnswer{status: testStatus(infomodels.BackupStateBaseScanActive, 12)}

	tests := []struct {
		name           string
		abortErr       error
		answers        []statusAnswer
		wantErr        error
		wantCalls      int
		wantAbortCalls int
		wantLogs       []string
		notWantLogs    []string
	}{
		{
			name: "confirms the abort through the aborted state",
			answers: []statusAnswer{
				runningStatus,
				{status: testFinishedStatus(infomodels.BackupStateAborted, 12)},
			},
			wantCalls:      2,
			wantAbortCalls: 1,
			wantLogs:       []string{logAborting, logAborted},
		},
		{
			// A job that hit a failure on its way out reports that instead of the abort,
			// and is just as stopped.
			name: "confirms the abort through the failed state",
			answers: []statusAnswer{
				runningStatus,
				{status: testStatus(infomodels.BackupStateFailed, 12)},
			},
			wantCalls:      2,
			wantAbortCalls: 1,
			wantLogs:       []string{logAborting, logAborted},
		},
		{
			// The cluster drops the state of a job that is no longer running, so a job
			// that goes missing from the status after the abort has stopped too.
			name:           "confirms the abort for a job the cluster no longer reports",
			answers:        []statusAnswer{runningStatus},
			wantCalls:      2,
			wantAbortCalls: 1,
			wantLogs:       []string{logAborted},
		},
		{
			// The cluster keeps reporting the job while it winds down, so the state that
			// confirms the abort only arrives a few polls later.
			name: "waits for a job that is still winding down",
			answers: []statusAnswer{
				runningStatus,
				{status: testStatus(infomodels.BackupStateBaseScanActive, 12)},
				{status: testStatus(infomodels.BackupStateAborting, 91)},
				{status: testFinishedStatus(infomodels.BackupStateAborted, 91)},
			},
			wantCalls:      4,
			wantAbortCalls: 1,
			wantLogs:       []string{logAborted},
		},
		{
			// A job winding down from an earlier abort is still running, so a repeated
			// abort goes through and waits for it the same way.
			name: "aborts a job that is already winding down from an earlier abort",
			answers: []statusAnswer{
				{status: testStatus(infomodels.BackupStateAborting, 91)},
				{status: testFinishedStatus(infomodels.BackupStateAborted, 91)},
			},
			wantCalls:      2,
			wantAbortCalls: 1,
			wantLogs:       []string{logAborted},
		},
		{
			// A backup that reached the end between the check and the confirmation was
			// not aborted, and announcing it as aborted would hide a backup that is in
			// storage.
			name: "reports a backup that finished before the abort landed",
			answers: []statusAnswer{
				runningStatus,
				{status: testStatus(infomodels.BackupStateComplete, 100)},
			},
			wantCalls:      2,
			wantAbortCalls: 1,
			wantLogs:       []string{logAbortTooLate},
			notWantLogs:    []string{logAborted},
		},
		{
			// The cluster answers an abort for a job it knows nothing about without
			// complaining, so a job it does not report must not be asked to stop: the
			// status right after such a request looks just like a job that was aborted.
			name:           "does not abort a job the cluster does not report",
			wantCalls:      1,
			wantAbortCalls: 0,
			wantLogs:       []string{msgNoRunningBackup},
			notWantLogs:    []string{logAborting, logAborted},
		},
		{
			// A status with neither a state nor an error says nothing about the job, and
			// must not be read as a job to abort - the state of it would be dereferenced.
			name:           "does not abort a job whose status comes back empty",
			answers:        []statusAnswer{{}},
			wantCalls:      1,
			wantAbortCalls: 0,
			wantLogs:       []string{msgNoRunningBackup},
			notWantLogs:    []string{logAborting, logAborted},
		},
		{
			// The same empty answer during the confirmation says the job is no longer
			// reported, which is what the cluster does to a job that stopped.
			name:           "confirms the abort for a job whose status comes back empty",
			answers:        []statusAnswer{runningStatus, {}},
			wantCalls:      2,
			wantAbortCalls: 1,
			wantLogs:       []string{logAborted},
		},
		{
			// The state of a finished job is kept for a while, and there is nothing left
			// in it to abort.
			name:           "does not abort a job that already completed",
			answers:        []statusAnswer{{status: testFinishedStatus(infomodels.BackupStateComplete, 100)}},
			wantCalls:      1,
			wantAbortCalls: 0,
			wantLogs:       []string{msgNoRunningBackup},
			notWantLogs:    []string{logAborting, logAborted},
		},
		{
			name:           "does not abort a job that already stopped on an earlier abort",
			answers:        []statusAnswer{{status: testFinishedStatus(infomodels.BackupStateAborted, 12)}},
			wantCalls:      1,
			wantAbortCalls: 0,
			wantLogs:       []string{msgNoRunningBackup},
			notWantLogs:    []string{logAborting, logAborted},
		},
		{
			// Nothing was aborted, so there is nothing to confirm.
			name:           "propagates a rejected abort request without polling",
			answers:        []statusAnswer{runningStatus},
			abortErr:       errBoom,
			wantErr:        errBoom,
			wantCalls:      1,
			wantAbortCalls: 1,
			notWantLogs:    []string{logAborting, logAborted},
		},
		{
			// A check that cannot tell whether the job runs must not guess, and must not
			// send the abort either.
			name:           "propagates an unexpected error from the pre-abort check",
			answers:        []statusAnswer{{err: errBoom}},
			wantErr:        errBoom,
			wantCalls:      1,
			wantAbortCalls: 0,
			notWantLogs:    []string{logAborting, logAborted},
		},
		{
			name:           "propagates an unexpected status error",
			answers:        []statusAnswer{runningStatus, {err: errBoom}},
			wantErr:        errBoom,
			wantCalls:      2,
			wantAbortCalls: 1,
			notWantLogs:    []string{logAborted},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			svc, buf := newTestService(t)
			client := &fakeInfoClient{answers: tt.answers, abortErr: tt.abortErr}

			err := svc.abortBackup(t.Context(), client)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}

			// The abort is requested at most once, and only for the configured job.
			assert.Equal(t, tt.wantAbortCalls, client.abortCalls)
			assert.Equal(t, tt.wantCalls, client.calls)

			if tt.wantAbortCalls > 0 {
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

// TestServiceAbortBackupTimesOut pins down the bound on the wait: a job that never reacts
// to the abort has to end the command instead of being polled forever, and the failure
// has to say so rather than claim the backup stopped.
func TestServiceAbortBackupTimesOut(t *testing.T) {
	t.Parallel()

	svc, buf := newTestService(t)

	client := &fakeInfoClient{
		answers:    []statusAnswer{{status: testStatus(infomodels.BackupStateIncrScanActive, 80)}},
		repeatLast: true,
	}

	err := svc.abortBackup(t.Context(), client)

	require.ErrorIs(t, err, errAbortStatusTimeout)
	// The last state the cluster reported belongs in the error: it is the only clue
	// about what the job was still doing.
	require.ErrorContains(t, err, infomodels.BackupStateIncrScanActive.Describe())
	require.Greater(t, client.calls, 1, "the abort must keep polling until the wait runs out")
	require.NotContains(t, buf.String(), logAborted)
}

// TestServiceAbortBackupStatusDeadline covers the wait running out while a status lookup
// is in flight: the lookup comes back with the deadline instead of a state, and the
// command has to report the wait it gave up on rather than pass a bare deadline on.
func TestServiceAbortBackupStatusDeadline(t *testing.T) {
	t.Parallel()

	svc, buf := newTestService(t)

	client := &fakeInfoClient{
		answers: []statusAnswer{
			{status: testStatus(infomodels.BackupStateIncrScanActive, 80)},
			{err: fmt.Errorf("status request: %w", context.DeadlineExceeded)},
		},
	}

	err := svc.abortBackup(t.Context(), client)

	require.ErrorIs(t, err, errAbortStatusTimeout)
	require.ErrorContains(t, err, testJobID)
	assert.Equal(t, 2, client.calls, "the wait must end with the lookup that ran out of time")
	assert.NotContains(t, buf.String(), logAborted)
}

// TestServiceAbortBackupCanceled makes sure a command the user interrupted is reported as
// canceled and not as a timeout, and that the wait ends at once instead of sitting out the
// poll interval.
func TestServiceAbortBackupCanceled(t *testing.T) {
	t.Parallel()

	svc, buf := newTestService(t)
	// A poll interval longer than the test would tolerate: only the cancellation can
	// end this wait.
	svc.abortPollInterval = time.Minute

	ctx, cancel := context.WithCancel(t.Context())

	client := &fakeInfoClient{
		answers:    []statusAnswer{{status: testStatus(infomodels.BackupStateBaseScanActive, 40)}},
		repeatLast: true,
	}

	cancel()

	err := svc.abortBackup(ctx, client)

	require.ErrorIs(t, err, context.Canceled)
	require.NotErrorIs(t, err, errAbortStatusTimeout)
	assert.NotContains(t, buf.String(), logAborted)
}
