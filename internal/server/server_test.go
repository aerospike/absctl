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
	"bytes"
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/aerospike/absctl/internal/config"
	"github.com/aerospike/absctl/internal/models"
	"github.com/aerospike/backup-go/mocks"
	backupmodels "github.com/aerospike/backup-go/models"
	"github.com/aerospike/backup-go/pkg/asinfo"
	infomodels "github.com/aerospike/backup-go/pkg/asinfo/models"
	"github.com/aerospike/backup-go/pkg/server/lister"
	servermodels "github.com/aerospike/backup-go/pkg/server/lister/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testBucket      = "asbackup"
	logNoRunningJob = "no running backup found"
	logBackupEntry  = "backup entry"

	logAborting     = "aborting"
	logAborted      = "backup was successfully aborted"
	logAbortTooLate = "backup finished before it could be aborted"

	logAbortingRestore = "aborting restore"
	logRestoreAborted  = "restore was successfully aborted"

	// testAbortTimeout bounds the abort wait in the tests. It has to outlast a handful
	// of one-millisecond polls, and still expire quickly when a test wants it to.
	testAbortTimeout = 50 * time.Millisecond

	// testBackupDuration is how long a finished job ran in the tests, and logDuration is
	// how the completion line reports it.
	testBackupDuration = time.Minute
	logDuration        = "duration=1m0s"

	// testErrorReason stands for the reason the cluster gives for a terminal state.
	testErrorReason = "storage unreachable"
)

// errBoom stands for any failure that is not a missing job or a missing manifest.
var errBoom = errors.New("boom")

// statusAnswer is a single canned answer of fakeInfoClient.
type statusAnswer struct {
	status *infomodels.ResponseBackupState
	err    error
}

// fakeInfoClient replays canned answers to the status lookups, one per call, and keeps
// reporting the job as missing once they run out - unless repeatLast is set, which makes
// the last answer stand forever, for the loops that only a timeout can end. Every call is
// counted, so that a test which expects a loop to stop also proves that it stopped
// polling.
//
// The embedded mock supplies the rest of backup.ServerBackupInfo: reaching one of those
// methods means the code under test called something it has no business calling, and the
// mock panics instead of quietly answering.
type fakeInfoClient struct {
	mocks.MockServerBackupInfo

	answers    []statusAnswer
	repeatLast bool
	calls      int

	abortErr   error
	abortJobID string
	abortCalls int
}

func (f *fakeInfoClient) GetBackupStatus(
	_ context.Context, _ string,
) (*infomodels.ResponseBackupState, error) {
	call := f.calls
	f.calls++

	if call >= len(f.answers) {
		if !f.repeatLast || len(f.answers) == 0 {
			return nil, asinfo.ErrNotFound
		}

		call = len(f.answers) - 1
	}

	return f.answers[call].status, f.answers[call].err
}

func (f *fakeInfoClient) AbortBackup(_ context.Context, jobID string) error {
	f.abortCalls++
	f.abortJobID = jobID

	return f.abortErr
}

// restoreAnswer is a single canned answer of fakeRestoreInfoClient.
type restoreAnswer struct {
	state string
	err   error
}

// fakeRestoreInfoClient is the restore counterpart of fakeInfoClient: it replays canned
// restore status answers, one per call, and reports the namespace as carrying no restore
// state once they run out - unless repeatLast is set, which makes the last answer stand
// forever, for the loops that only a timeout can end.
type fakeRestoreInfoClient struct {
	mocks.MockServerBackupInfo

	answers    []restoreAnswer
	repeatLast bool
	calls      int

	abortErr       error
	abortNamespace string
	abortJobID     string
	abortCalls     int
}

func (f *fakeRestoreInfoClient) GetRestoreStatus(_ context.Context, _ string) (string, error) {
	call := f.calls
	f.calls++

	if call >= len(f.answers) {
		if !f.repeatLast || len(f.answers) == 0 {
			return "", asinfo.ErrNotFound
		}

		call = len(f.answers) - 1
	}

	return f.answers[call].state, f.answers[call].err
}

func (f *fakeRestoreInfoClient) AbortRestore(_ context.Context, namespace, jobID string) error {
	f.abortCalls++
	f.abortNamespace = namespace
	f.abortJobID = jobID

	return f.abortErr
}

// fakeMetadataGetter answers "not found" a given number of times before it either
// returns the manifest or fails permanently.
type fakeMetadataGetter struct {
	notFound int
	md       servermodels.Metadata
	err      error
	calls    int
}

func (f *fakeMetadataGetter) GetMetadata(_ context.Context, _ string) (servermodels.Metadata, error) {
	f.calls++

	if f.calls <= f.notFound {
		return servermodels.Metadata{}, lister.ErrMetadataNotFound
	}

	if f.err != nil {
		return servermodels.Metadata{}, f.err
	}

	return f.md, nil
}

// newTestService builds a service that logs into the returned buffer and polls and
// retries fast enough not to slow the tests down.
func newTestService(t *testing.T) (*Service, *bytes.Buffer) {
	t.Helper()

	buf := &bytes.Buffer{}

	return &Service{
		backupCfg: &config.ServerBackupServiceConfig{
			Abort: &models.ServerBackupAbort{JobID: testJobID},
			App:   &models.App{LogJSON: true},
			AwsS3: &models.AwsS3{BucketName: testBucket},
		},
		restoreCfg: &config.ServerRestoreServiceConfig{
			Abort: &models.ServerRestoreAbort{Namespace: testNamespace, JobID: testJobID},
		},
		logger:               slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelInfo})),
		metadataRetryPolicy:  backupmodels.NewRetryPolicy(time.Millisecond, 1.0, 3),
		vanishedPollInterval: time.Millisecond,
		abortPollInterval:    time.Millisecond,
		abortTimeout:         testAbortTimeout,
	}, buf
}

// repeatNotFound builds n answers that report the job as missing.
func repeatNotFound(n int) []statusAnswer {
	answers := make([]statusAnswer, n)
	for i := range answers {
		answers[i] = statusAnswer{err: asinfo.ErrNotFound}
	}

	return answers
}

func testStatus(state infomodels.BackupState, pct float64) *infomodels.ResponseBackupState {
	return &infomodels.ResponseBackupState{
		JobID:       testJobID,
		Namespace:   testNamespace,
		State:       state,
		ProgressPct: pct,
		StartTime:   time.Now().Add(-testBackupDuration),
	}
}

// testFinishedStatus reports a job that reached a terminal state, which is the only case
// in which the cluster fills in the finish time.
func testFinishedStatus(state infomodels.BackupState, pct float64) *infomodels.ResponseBackupState {
	status := testStatus(state, pct)
	status.FinishTime = status.StartTime.Add(testBackupDuration)

	return status
}

// testFailedStatus reports a job that ended in a terminal state with a reason attached.
func testFailedStatus(state infomodels.BackupState) *infomodels.ResponseBackupState {
	status := testFinishedStatus(state, 42)
	status.ErrorReason = testErrorReason

	return status
}

func TestServiceReportBackupProgress(t *testing.T) {
	t.Parallel()

	md := servermodels.Metadata{BackupID: testJobID, Namespace: testNamespace}

	tests := []struct {
		name        string
		watch       bool
		answers     []statusAnswer
		metadata    *fakeMetadataGetter
		wantErr     error
		wantErrMsg  string
		wantCalls   int
		wantLogs    []string
		notWantLogs []string
	}{
		{
			// The cluster reports both ends of a finished job, so the completion line
			// can say how long the backup took.
			name:      "reports a completed backup with the time it took",
			watch:     true,
			answers:   []statusAnswer{{status: testFinishedStatus(infomodels.BackupStateComplete, 100)}},
			wantCalls: 1,
			wantLogs:  []string{msgBackupComplete, logDuration, logBackupEntry},
		},
		{
			// The cluster drops the job state right after finishing the backup. The job
			// was last seen draining, so this is a completion and not a missing backup.
			name:  "treats a job vanished while draining as a completed backup",
			watch: true,
			answers: []statusAnswer{
				{status: testStatus(infomodels.BackupStateFinalDraining, 97)},
				{err: asinfo.ErrNotFound},
				{err: asinfo.ErrNotFound},
				{err: asinfo.ErrNotFound},
				{err: asinfo.ErrNotFound},
				{err: asinfo.ErrNotFound},
			},
			wantCalls:   6,
			wantLogs:    []string{msgBackupComplete, logBackupEntry},
			notWantLogs: []string{logNoRunningJob},
		},
		{
			// The status of a running job can be unavailable for a moment. Reporting
			// that gap as a completed backup is the bug this case guards against.
			name:  "keeps watching after a transient status gap",
			watch: true,
			answers: []statusAnswer{
				{status: testStatus(infomodels.BackupStateBaseScanActive, 3)},
				{err: asinfo.ErrNotFound},
				{status: testStatus(infomodels.BackupStateBaseScanActive, 47)},
				{status: testStatus(infomodels.BackupStateComplete, 100)},
			},
			wantCalls: 4,
			wantLogs:  []string{msgBackupComplete, logBackupEntry},
		},
		{
			// A scan that stops reporting for longer than the confirmations of a
			// finished job still has to be waited for: it is nowhere near the stages a
			// backup can complete from, so the watch keeps the job until it comes back.
			name:  "keeps watching a scan that stops reporting for a long time",
			watch: true,
			answers: append(
				append(
					[]statusAnswer{{status: testStatus(infomodels.BackupStateBaseScanActive, 4)}},
					repeatNotFound(vanishedJobConfirmations*3)...,
				),
				statusAnswer{status: testStatus(infomodels.BackupStateIncrScanActive, 95)},
				statusAnswer{status: testStatus(infomodels.BackupStateComplete, 100)},
			),
			wantCalls: vanishedJobConfirmations*3 + 3,
			wantLogs:  []string{msgBackupComplete, logBackupEntry},
		},
		{
			// The watch can be between polls while the job runs through its last stages.
			// The manifest proves the backup finished even though the last state seen
			// was not one a backup completes from.
			name:  "reports a backup that finished between polls",
			watch: true,
			answers: append(
				[]statusAnswer{{status: testStatus(infomodels.BackupStateIncrScanActive, 95)}},
				repeatNotFound(midBackupGapConfirmations)...,
			),
			wantCalls: midBackupGapConfirmations + 1,
			wantLogs:  []string{msgBackupComplete, logBackupEntry},
		},
		{
			// A job that stopped reporting in the middle of a backup and left no
			// manifest behind did not complete, and must not be announced as complete.
			name:  "fails when a job vanished mid-backup left no manifest",
			watch: true,
			answers: append(
				[]statusAnswer{{status: testStatus(infomodels.BackupStateBaseScanActive, 3)}},
				repeatNotFound(midBackupGapConfirmations)...,
			),
			metadata:    &fakeMetadataGetter{notFound: 10},
			wantErr:     errBackupStatusLost,
			wantCalls:   midBackupGapConfirmations + 1,
			notWantLogs: []string{msgBackupComplete},
		},
		{
			// A job the cluster never reported is looked up a few more times before it
			// is called missing: a backup that was just started may not be in the status
			// yet, and reporting it as missing would end the watch before it began.
			name:        "reports a job that was never running",
			watch:       true,
			answers:     repeatNotFound(vanishedJobConfirmations),
			wantCalls:   vanishedJobConfirmations,
			wantLogs:    []string{logNoRunningJob, logBackupEntry},
			notWantLogs: []string{msgBackupComplete},
		},
		{
			// Without --watch there is nothing to wait for, so a missing job is answered
			// by the single lookup the user asked for.
			name:        "reports a missing job once when watching is disabled",
			watch:       false,
			answers:     repeatNotFound(1),
			wantCalls:   1,
			wantLogs:    []string{logNoRunningJob, logBackupEntry},
			notWantLogs: []string{msgBackupComplete},
		},
		{
			// A manifest that cannot be read for a reason other than being absent is a
			// storage failure, and must not be reported as a job that stopped reporting.
			name:  "propagates a storage failure for a job vanished mid-backup",
			watch: true,
			answers: append(
				[]statusAnswer{{status: testStatus(infomodels.BackupStateBaseScanActive, 3)}},
				repeatNotFound(midBackupGapConfirmations)...,
			),
			metadata:    &fakeMetadataGetter{err: errBoom},
			wantErr:     errBoom,
			wantCalls:   midBackupGapConfirmations + 1,
			notWantLogs: []string{msgBackupComplete},
		},
		{
			// COMMITTING is the last stage before COMPLETE, so a job that stops
			// reporting from it has finished just as much as one that was draining.
			name:  "treats a job vanished while committing as a completed backup",
			watch: true,
			answers: append(
				[]statusAnswer{{status: testStatus(infomodels.BackupStateCommitting, 99)}},
				repeatNotFound(vanishedJobConfirmations)...,
			),
			wantCalls:   vanishedJobConfirmations + 1,
			wantLogs:    []string{msgBackupComplete, logBackupEntry},
			notWantLogs: []string{logNoRunningJob},
		},
		{
			name:      "fails on a failed backup",
			watch:     true,
			answers:   []statusAnswer{{status: testStatus(infomodels.BackupStateFailed, 42)}},
			wantErr:   errBackupFailed,
			wantCalls: 1,
		},
		{
			// The reason is the only thing that says why the job died, so it belongs in
			// the error rather than in a log line the caller may never see.
			name:       "names the reason the cluster gave for a failure",
			watch:      true,
			answers:    []statusAnswer{{status: testFailedStatus(infomodels.BackupStateFailed)}},
			wantErr:    errBackupFailed,
			wantErrMsg: testErrorReason,
			wantCalls:  1,
		},
		{
			// An aborted job stopped on request and left no usable backup behind, so it
			// must not be reported as a completion.
			name:        "fails on an aborted backup",
			watch:       true,
			answers:     []statusAnswer{{status: testFinishedStatus(infomodels.BackupStateAborted, 42)}},
			wantErr:     errBackupAborted,
			wantCalls:   1,
			notWantLogs: []string{msgBackupComplete},
		},
		{
			// ABORTING is not terminal: the job is still winding down, and the watch has
			// to stay on it until it reports the abort.
			name:  "keeps watching a job that is still aborting",
			watch: true,
			answers: []statusAnswer{
				{status: testStatus(infomodels.BackupStateAborting, 42)},
				{status: testFinishedStatus(infomodels.BackupStateAborted, 42)},
			},
			wantErr:   errBackupAborted,
			wantCalls: 2,
		},
		{
			name:      "propagates an unexpected status error",
			watch:     true,
			answers:   []statusAnswer{{err: errBoom}},
			wantErr:   errBoom,
			wantCalls: 1,
		},
		{
			name:        "prints once when watching is disabled",
			watch:       false,
			answers:     []statusAnswer{{status: testStatus(infomodels.BackupStateBaseScanActive, 5)}},
			wantCalls:   1,
			wantLogs:    []string{"backup progress"},
			notWantLogs: []string{msgBackupComplete, logBackupEntry},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			svc, buf := newTestService(t)
			status := &fakeInfoClient{answers: tt.answers}

			metadata := tt.metadata
			if metadata == nil {
				metadata = &fakeMetadataGetter{md: md}
			}

			err := svc.reportBackupProgress(t.Context(), status, metadata, testJobID, tt.watch)

			if tt.wantErr == nil && tt.wantErrMsg == "" {
				require.NoError(t, err)
			}

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			}

			if tt.wantErrMsg != "" {
				require.ErrorContains(t, err, tt.wantErrMsg)
			}

			assert.Equal(t, tt.wantCalls, status.calls)

			for _, want := range tt.wantLogs {
				assert.Contains(t, buf.String(), want)
			}

			for _, notWant := range tt.notWantLogs {
				assert.NotContains(t, buf.String(), notWant)
			}
		})
	}
}

// TestServiceJobVanished pins down what a job disappearing from the cluster status means
// for every stage of the backup lifecycle: only the last stages may be read as a finished
// backup, and every earlier one has to be waited out.
func TestServiceJobVanished(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		watch        bool
		lastState    infomodels.BackupState
		wantAfter    int
		wantNotAfter int
	}{
		{
			name:         "never reported",
			watch:        true,
			wantAfter:    vanishedJobConfirmations,
			wantNotAfter: vanishedJobConfirmations - 1,
		},
		{
			name:         "init",
			watch:        true,
			lastState:    infomodels.BackupStateInit,
			wantAfter:    midBackupGapConfirmations,
			wantNotAfter: midBackupGapConfirmations - 1,
		},
		{
			name:         "base scan active",
			watch:        true,
			lastState:    infomodels.BackupStateBaseScanActive,
			wantAfter:    midBackupGapConfirmations,
			wantNotAfter: midBackupGapConfirmations - 1,
		},
		{
			name:         "base scan done",
			watch:        true,
			lastState:    infomodels.BackupStateBaseScanDone,
			wantAfter:    midBackupGapConfirmations,
			wantNotAfter: midBackupGapConfirmations - 1,
		},
		{
			name:         "incremental scan active",
			watch:        true,
			lastState:    infomodels.BackupStateIncrScanActive,
			wantAfter:    midBackupGapConfirmations,
			wantNotAfter: midBackupGapConfirmations - 1,
		},
		{
			name:         "unknown state",
			watch:        true,
			lastState:    infomodels.BackupStateUnknown,
			wantAfter:    midBackupGapConfirmations,
			wantNotAfter: midBackupGapConfirmations - 1,
		},
		{
			name:         "stopping change stream",
			watch:        true,
			lastState:    infomodels.BackupStateStoppingChangeStream,
			wantAfter:    vanishedJobConfirmations,
			wantNotAfter: vanishedJobConfirmations - 1,
		},
		{
			name:         "final draining",
			watch:        true,
			lastState:    infomodels.BackupStateFinalDraining,
			wantAfter:    vanishedJobConfirmations,
			wantNotAfter: vanishedJobConfirmations - 1,
		},
		{
			name:         "committing",
			watch:        true,
			lastState:    infomodels.BackupStateCommitting,
			wantAfter:    vanishedJobConfirmations,
			wantNotAfter: vanishedJobConfirmations - 1,
		},
		{
			name:         "complete",
			watch:        true,
			lastState:    infomodels.BackupStateComplete,
			wantAfter:    vanishedJobConfirmations,
			wantNotAfter: vanishedJobConfirmations - 1,
		},
		{
			name:      "one-shot lookup does not wait",
			watch:     false,
			lastState: infomodels.BackupStateBaseScanActive,
			wantAfter: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if tt.wantNotAfter > 0 {
				assert.False(t, jobVanished(tt.lastState, tt.wantNotAfter, tt.watch),
					"the job must still be waited for after %d misses", tt.wantNotAfter)
			}

			assert.True(t, jobVanished(tt.lastState, tt.wantAfter, tt.watch),
				"the job must be given up after %d misses", tt.wantAfter)
		})
	}
}

func TestServiceFetchMetadata(t *testing.T) {
	t.Parallel()

	md := servermodels.Metadata{BackupID: testJobID, Namespace: testNamespace}

	tests := []struct {
		name      string
		wait      bool
		getter    *fakeMetadataGetter
		wantErr   string
		wantCalls int
	}{
		{
			name:      "returns the manifest right away",
			wait:      true,
			getter:    &fakeMetadataGetter{md: md},
			wantCalls: 1,
		},
		{
			name:      "waits until the manifest is uploaded",
			wait:      true,
			getter:    &fakeMetadataGetter{notFound: 2, md: md},
			wantCalls: 3,
		},
		{
			name:      "gives up after the last attempt",
			wait:      true,
			getter:    &fakeMetadataGetter{notFound: 10, md: md},
			wantErr:   "timed out waiting for backup metadata",
			wantCalls: 3,
		},
		{
			name:      "does not retry a permanent failure",
			wait:      true,
			getter:    &fakeMetadataGetter{err: errBoom},
			wantErr:   "failed to get backup metadata",
			wantCalls: 1,
		},
		{
			name:      "does not wait when the job was never running",
			wait:      false,
			getter:    &fakeMetadataGetter{notFound: 1, md: md},
			wantErr:   "failed to get backup metadata",
			wantCalls: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			svc, _ := newTestService(t)

			got, err := svc.fetchMetadata(t.Context(), tt.getter, testJobID, tt.wait)

			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
				assert.Equal(t, md, got)
			}

			assert.Equal(t, tt.wantCalls, tt.getter.calls)
		})
	}
}

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

// TestServiceAbortRestore covers the outcomes the restore abort reaches within a few
// polls. The two that only time can produce are driven separately below.
//
// The first status lookup is the pre-abort check, so every case that gets as far as
// requesting the abort opens with a state that reports the namespace as restoring.
func TestServiceAbortRestore(t *testing.T) {
	t.Parallel()

	// restoringState is the answer the pre-abort check needs to let the abort through.
	restoringState := restoreAnswer{state: infomodels.RestoreStateRestoring}

	tests := []struct {
		name           string
		abortErr       error
		answers        []restoreAnswer
		wantErr        error
		wantCalls      int
		wantAbortCalls int
		wantLogs       []string
		notWantLogs    []string
	}{
		{
			// Nothing restores into the namespace anymore, so the abort has landed.
			name:           "confirms the abort through the none state",
			answers:        []restoreAnswer{restoringState, {state: infomodels.RestoreStateNone}},
			wantCalls:      2,
			wantAbortCalls: 1,
			wantLogs:       []string{logAbortingRestore, logRestoreAborted},
		},
		{
			// A restore that hit a failure on its way out is just as stopped.
			name:           "confirms the abort through the failed state",
			answers:        []restoreAnswer{restoringState, {state: infomodels.RestoreStateFailed}},
			wantCalls:      2,
			wantAbortCalls: 1,
			wantLogs:       []string{logRestoreAborted},
		},
		{
			// The cluster answers with no state at all for a namespace nothing restores
			// into, which says the same thing as the NONE state.
			name:           "confirms the abort for a namespace with no restore state",
			answers:        []restoreAnswer{restoringState},
			wantCalls:      2,
			wantAbortCalls: 1,
			wantLogs:       []string{logRestoreAborted},
		},
		{
			// The cluster keeps reporting the namespace as restoring while the job winds
			// down, so the state that confirms the abort only arrives a few polls later.
			name: "waits for a restore that is still winding down",
			answers: []restoreAnswer{
				restoringState,
				{state: infomodels.RestoreStateRestoring},
				{state: infomodels.RestoreStateRestoring},
				{state: infomodels.RestoreStateNone},
			},
			wantCalls:      4,
			wantAbortCalls: 1,
			wantLogs:       []string{logRestoreAborted},
		},
		{
			// A job aborted before it started writing records is still preparing, which
			// is something to abort and not a stopped restore.
			name: "waits for a restore that is still preparing",
			answers: []restoreAnswer{
				{state: infomodels.RestoreStatePreparing},
				{state: infomodels.RestoreStatePreparing},
				{state: infomodels.RestoreStateNone},
			},
			wantCalls:      3,
			wantAbortCalls: 1,
			wantLogs:       []string{logRestoreAborted},
		},
		{
			// A prepared namespace blocks writes until the restore ends, so the abort has
			// to release that block - and only the state that follows says it did. Until
			// then the namespace keeps reporting itself as prepared.
			name: "aborts a namespace that is only prepared and waits for the block to lift",
			answers: []restoreAnswer{
				{state: infomodels.RestoreStateReady},
				{state: infomodels.RestoreStateReady},
				{state: infomodels.RestoreStateNone},
			},
			wantCalls:      3,
			wantAbortCalls: 1,
			wantLogs:       []string{logAbortingRestore, logRestoreAborted},
		},
		{
			// The cluster accepts the abort of a namespace nothing restores into, and the
			// idle status that follows would announce a restore that never ran as
			// aborted. This is what a wrong backup id used to look like.
			name:           "does not abort a namespace that carries no restore state",
			wantCalls:      1,
			wantAbortCalls: 0,
			wantLogs:       []string{msgNoRunningRestore},
			notWantLogs:    []string{logAbortingRestore, logRestoreAborted},
		},
		{
			name:           "does not abort a namespace nothing restores into",
			answers:        []restoreAnswer{{state: infomodels.RestoreStateNone}},
			wantCalls:      1,
			wantAbortCalls: 0,
			wantLogs:       []string{msgNoRunningRestore},
			notWantLogs:    []string{logAbortingRestore, logRestoreAborted},
		},
		{
			// A restore that ended in a failure holds nothing anymore.
			name:           "does not abort a namespace whose restore failed",
			answers:        []restoreAnswer{{state: infomodels.RestoreStateFailed}},
			wantCalls:      1,
			wantAbortCalls: 0,
			wantLogs:       []string{msgNoRunningRestore},
			notWantLogs:    []string{logAbortingRestore, logRestoreAborted},
		},
		{
			// Nothing was aborted, so there is nothing to confirm.
			name:           "propagates a rejected abort request without polling",
			answers:        []restoreAnswer{restoringState},
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
			answers:        []restoreAnswer{restoringState, {err: errBoom}},
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

// TestServiceAbortRestoreTimesOut pins down the bound on the wait: a namespace that never
// stops reporting a running restore has to end the command instead of being polled
// forever, and the failure has to say so rather than claim the restore stopped.
func TestServiceAbortRestoreTimesOut(t *testing.T) {
	t.Parallel()

	svc, buf := newTestService(t)

	client := &fakeRestoreInfoClient{
		answers:    []restoreAnswer{{state: infomodels.RestoreStateRestoring}},
		repeatLast: true,
	}

	err := svc.abortRestore(t.Context(), client)

	require.ErrorIs(t, err, errAbortRestoreStatusTimeout)
	// The last state the cluster reported belongs in the error: it is the only clue
	// about what the namespace was still doing.
	require.ErrorContains(t, err, infomodels.RestoreStateRestoring)
	require.Greater(t, client.calls, 1, "the abort must keep polling until the wait runs out")
	require.NotContains(t, buf.String(), logRestoreAborted)
}

// TestServiceAbortRestoreCanceled makes sure a command the user interrupted is reported
// as canceled and not as a timeout, and that the wait ends at once instead of sitting out
// the poll interval.
func TestServiceAbortRestoreCanceled(t *testing.T) {
	t.Parallel()

	svc, buf := newTestService(t)
	// A poll interval longer than the test would tolerate: only the cancellation can
	// end this wait.
	svc.abortPollInterval = time.Minute

	ctx, cancel := context.WithCancel(t.Context())

	client := &fakeRestoreInfoClient{
		answers:    []restoreAnswer{{state: infomodels.RestoreStateRestoring}},
		repeatLast: true,
	}

	cancel()

	err := svc.abortRestore(ctx, client)

	require.ErrorIs(t, err, context.Canceled)
	require.NotErrorIs(t, err, errAbortRestoreStatusTimeout)
	assert.NotContains(t, buf.String(), logRestoreAborted)
}

func TestServiceMetadataS3Config(t *testing.T) {
	t.Parallel()

	svc, _ := newTestService(t)

	cfg := svc.metadataS3Config()

	assert.Equal(t, testBucket, cfg.BucketName)
	assert.Equal(t, metadataRequestTimeout, cfg.RequestTimeout)
	// The shared configuration must stay untouched.
	assert.Equal(t, 0, svc.backupCfg.AwsS3.RequestTimeout)
}
