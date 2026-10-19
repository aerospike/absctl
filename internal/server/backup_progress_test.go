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
	"log/slog"
	"testing"
	"time"

	"github.com/aerospike/backup-go/pkg/asinfo"
	infomodels "github.com/aerospike/backup-go/pkg/asinfo/models"
	"github.com/aerospike/backup-go/pkg/server/lister"
	servermodels "github.com/aerospike/backup-go/pkg/server/lister/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

func TestServiceReportBackupProgress(t *testing.T) {
	t.Parallel()

	md := servermodels.Metadata{BackupID: testJobID, Namespace: testNamespace}

	tests := []struct {
		name        string
		mode        progressMode
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
			mode:      followJob,
			answers:   []statusAnswer{{status: testFinishedStatus(infomodels.BackupStateComplete, 100)}},
			wantCalls: 1,
			wantLogs:  []string{msgBackupComplete, logDuration, logBackupEntry},
		},
		{
			// The cluster drops the job state right after finishing the backup. The job
			// was last seen draining, so this is a completion and not a missing backup.
			name: "treats a job vanished while draining as a completed backup",
			mode: followJob,
			answers: []statusAnswer{
				{status: testStatus(infomodels.BackupStateFinalDraining, 97)},
				{err: asinfo.ErrNotFound},
				{err: asinfo.ErrNotFound},
				{err: asinfo.ErrNotFound},
				{err: asinfo.ErrNotFound},
				{err: asinfo.ErrNotFound},
			},
			wantCalls: 6,
			wantLogs:  []string{msgBackupComplete, logBackupEntry},
		},
		{
			// The status of a running job can be unavailable for a moment. Reporting
			// that gap as a completed backup is the bug this case guards against.
			name: "keeps watching after a transient status gap",
			mode: followJob,
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
			name: "keeps watching a scan that stops reporting for a long time",
			mode: followJob,
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
			name: "reports a backup that finished between polls",
			mode: followJob,
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
			name: "fails when a job vanished mid-backup left no manifest",
			mode: followJob,
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
			// A job the cluster does not report has no status to wait for: the lookup
			// itself already gives a job that is about to appear a few seconds of grace,
			// so the first answer is final. Polling it instead left the command silent
			// for half a minute before it said the same thing.
			name:        "fails right away when the cluster does not report the job",
			mode:        followJob,
			answers:     repeatNotFound(1),
			wantErr:     errBackupNotFound,
			wantErrMsg:  testJobID,
			wantCalls:   1,
			notWantLogs: []string{msgBackupComplete, logBackupEntry},
		},
		{
			name:        "fails right away when the cluster does not report the job and watching is disabled",
			mode:        reportOnce,
			answers:     repeatNotFound(1),
			wantErr:     errBackupNotFound,
			wantCalls:   1,
			notWantLogs: []string{msgBackupComplete, logBackupEntry},
		},
		{
			// The job "backup start" follows exists: the cluster answered with its id.
			// Failing the command because the status does not list it in the first
			// moment would report a backup that is running as one that was never there.
			name: "waits for a just-started job to turn up in the status",
			mode: followStartedJob,
			answers: append(
				repeatNotFound(startedJobConfirmations-1),
				statusAnswer{status: testStatus(infomodels.BackupStateBaseScanActive, 7)},
				statusAnswer{status: testStatus(infomodels.BackupStateComplete, 100)},
			),
			wantCalls: startedJobConfirmations + 1,
			wantLogs:  []string{msgBackupComplete, logBackupEntry},
		},
		{
			// The grace is bounded: a job that never turns up ends the command rather
			// than leaving it watching a job the cluster will not report.
			name:        "fails when a just-started job never turns up",
			mode:        followStartedJob,
			answers:     repeatNotFound(startedJobConfirmations),
			wantErr:     errBackupNotFound,
			wantErrMsg:  testJobID,
			wantCalls:   startedJobConfirmations,
			notWantLogs: []string{msgBackupComplete, logBackupEntry},
		},
		{
			// A status the cluster answers with neither a state nor an error says as
			// little about the job as one that was not found.
			name:      "fails right away on a status without a state or an error",
			mode:      followJob,
			answers:   []statusAnswer{{}},
			wantErr:   errBackupNotFound,
			wantCalls: 1,
		},
		{
			// A manifest that cannot be read for a reason other than being absent is a
			// storage failure, and must not be reported as a job that stopped reporting.
			name: "propagates a storage failure for a job vanished mid-backup",
			mode: followJob,
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
			name: "treats a job vanished while committing as a completed backup",
			mode: followJob,
			answers: append(
				[]statusAnswer{{status: testStatus(infomodels.BackupStateCommitting, 99)}},
				repeatNotFound(vanishedJobConfirmations)...,
			),
			wantCalls: vanishedJobConfirmations + 1,
			wantLogs:  []string{msgBackupComplete, logBackupEntry},
		},
		{
			name:      "fails on a failed backup",
			mode:      followJob,
			answers:   []statusAnswer{{status: testStatus(infomodels.BackupStateFailed, 42)}},
			wantErr:   errBackupFailed,
			wantCalls: 1,
		},
		{
			// The reason is the only thing that says why the job died, so it belongs in
			// the error rather than in a log line the caller may never see.
			name:       "names the reason the cluster gave for a failure",
			mode:       followJob,
			answers:    []statusAnswer{{status: testFailedStatus(infomodels.BackupStateFailed)}},
			wantErr:    errBackupFailed,
			wantErrMsg: testErrorReason,
			wantCalls:  1,
		},
		{
			// An aborted job stopped on request and left no usable backup behind, so it
			// must not be reported as a completion.
			name:        "fails on an aborted backup",
			mode:        followJob,
			answers:     []statusAnswer{{status: testFinishedStatus(infomodels.BackupStateAborted, 42)}},
			wantErr:     errBackupAborted,
			wantCalls:   1,
			notWantLogs: []string{msgBackupComplete},
		},
		{
			// ABORTING is not terminal: the job is still winding down, and the watch has
			// to stay on it until it reports the abort.
			name: "keeps watching a job that is still aborting",
			mode: followJob,
			answers: []statusAnswer{
				{status: testStatus(infomodels.BackupStateAborting, 42)},
				{status: testFinishedStatus(infomodels.BackupStateAborted, 42)},
			},
			wantErr:   errBackupAborted,
			wantCalls: 2,
		},
		{
			name:      "propagates an unexpected status error",
			mode:      followJob,
			answers:   []statusAnswer{{err: errBoom}},
			wantErr:   errBoom,
			wantCalls: 1,
		},
		{
			name:        "prints once when watching is disabled",
			mode:        reportOnce,
			answers:     []statusAnswer{{status: testStatus(infomodels.BackupStateBaseScanActive, 5)}},
			wantCalls:   1,
			wantLogs:    []string{msgBackupProgress},
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

			err := svc.reportBackupProgress(t.Context(), status, metadata, testJobID, tt.mode)

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
//
// Every case describes a job that was reported at least once, which is the only kind
// jobVanished is asked about: a job the cluster has never reported ends the watch without
// any confirmation, and is covered by TestServiceReportBackupProgress.
func TestServiceJobVanished(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		lastState    infomodels.BackupState
		wantAfter    int
		wantNotAfter int
	}{
		{
			name:         "init",
			lastState:    infomodels.BackupStateInit,
			wantAfter:    midBackupGapConfirmations,
			wantNotAfter: midBackupGapConfirmations - 1,
		},
		{
			name:         "base scan active",
			lastState:    infomodels.BackupStateBaseScanActive,
			wantAfter:    midBackupGapConfirmations,
			wantNotAfter: midBackupGapConfirmations - 1,
		},
		{
			name:         "base scan done",
			lastState:    infomodels.BackupStateBaseScanDone,
			wantAfter:    midBackupGapConfirmations,
			wantNotAfter: midBackupGapConfirmations - 1,
		},
		{
			name:         "incremental scan active",
			lastState:    infomodels.BackupStateIncrScanActive,
			wantAfter:    midBackupGapConfirmations,
			wantNotAfter: midBackupGapConfirmations - 1,
		},
		{
			name:         "unknown state",
			lastState:    infomodels.BackupStateUnknown,
			wantAfter:    midBackupGapConfirmations,
			wantNotAfter: midBackupGapConfirmations - 1,
		},
		{
			name:         "stopping change stream",
			lastState:    infomodels.BackupStateStoppingChangeStream,
			wantAfter:    vanishedJobConfirmations,
			wantNotAfter: vanishedJobConfirmations - 1,
		},
		{
			name:         "final draining",
			lastState:    infomodels.BackupStateFinalDraining,
			wantAfter:    vanishedJobConfirmations,
			wantNotAfter: vanishedJobConfirmations - 1,
		},
		{
			name:         "committing",
			lastState:    infomodels.BackupStateCommitting,
			wantAfter:    vanishedJobConfirmations,
			wantNotAfter: vanishedJobConfirmations - 1,
		},
		{
			name:         "complete",
			lastState:    infomodels.BackupStateComplete,
			wantAfter:    vanishedJobConfirmations,
			wantNotAfter: vanishedJobConfirmations - 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.False(t, jobVanished(tt.lastState, tt.wantNotAfter),
				"the job must still be waited for after %d misses", tt.wantNotAfter)

			assert.True(t, jobVanished(tt.lastState, tt.wantAfter),
				"the job must be given up after %d misses", tt.wantAfter)
		})
	}
}

// TestServiceReportBackupProgressSkipsMetadataForMissingJob makes sure a job the cluster
// does not report is answered from the cluster alone. Reading a manifest for it turned
// "no such backup" into whatever the storage had to say about a backup that was never
// there - an unreadable bucket answered a question about a job id with a credentials
// failure.
func TestServiceReportBackupProgressSkipsMetadataForMissingJob(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		mode progressMode
	}{
		{
			name: "watching",
			mode: followJob,
		},
		{
			name: "reporting once",
			mode: reportOnce,
		},
		{
			// A just-started job is waited for, but once the wait runs out it is still
			// the cluster that answers, not the storage.
			name: "following a just-started job",
			mode: followStartedJob,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			svc, _ := newTestService(t)
			client := &fakeInfoClient{answers: repeatNotFound(1)}
			// A getter that fails on any call, so that reading the manifest at all
			// would be reported instead of the missing job.
			metadata := &fakeMetadataGetter{err: errBoom}

			err := svc.reportBackupProgress(t.Context(), client, metadata, testJobID, tt.mode)

			require.ErrorIs(t, err, errBackupNotFound)
			require.NotErrorIs(t, err, errBoom)
			assert.Equal(t, 0, metadata.calls, "the manifest of a missing job must not be read")
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

// TestProgressPrinterPrint pins down the progress line itself. It carries the same
// fields as the client side backup progress - completion, estimate and speed - so that a
// server side backup reads the same way.
func TestProgressPrinterPrint(t *testing.T) {
	t.Parallel()

	now := time.Now()

	tests := []struct {
		name        string
		printer     *progressPrinter
		status      *infomodels.ResponseBackupState
		wantLogs    []string
		notWantLogs []string
	}{
		{
			name: "reports the progress of a running backup",
			status: &infomodels.ResponseBackupState{
				JobID:        testJobID,
				Namespace:    testNamespace,
				State:        infomodels.BackupStateBaseScanActive,
				ProgressPct:  50,
				RecsBackedUp: 1000,
				RecsBase:     1000,
				StartTime:    now.Add(-time.Hour),
			},
			wantLogs: []string{
				msgBackupProgress,
				"backup-id=" + testJobID,
				"pct=50",
				"rec/s=0",
				"remaining=1h0m0s",
			},
		},
		{
			name: "rounds the percentage to two decimals",
			status: &infomodels.ResponseBackupState{
				JobID:       testJobID,
				State:       infomodels.BackupStateBaseScanActive,
				ProgressPct: 33.33333,
				StartTime:   now.Add(-time.Hour),
			},
			wantLogs: []string{"pct=33.33"},
		},
		{
			// Without a start time there is nothing to project the estimate from, and a
			// made up one is worse than none.
			name: "omits the estimate when the cluster reported no start time",
			status: &infomodels.ResponseBackupState{
				JobID:       testJobID,
				State:       infomodels.BackupStateInit,
				ProgressPct: 0,
			},
			wantLogs:    []string{msgBackupProgress},
			notWantLogs: []string{"remaining="},
		},
		{
			name: "reports the speed measured since the previous sample",
			printer: &progressPrinter{
				stage:       string(infomodels.BackupStateBaseScanActive),
				sampledAt:   now.Add(-10 * time.Second),
				sampledRecs: 1000,
			},
			status: &infomodels.ResponseBackupState{
				JobID:        testJobID,
				State:        infomodels.BackupStateBaseScanActive,
				ProgressPct:  50,
				RecsBackedUp: 3000,
				StartTime:    now.Add(-time.Hour),
			},
			wantLogs: []string{"rec/s=200"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var buf bytes.Buffer

			printer := tt.printer
			if printer == nil {
				printer = &progressPrinter{}
			}

			printer.logger = slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
			printer.msg = msgBackupProgress

			require.True(t, printer.Print(backupSnapshot(tt.status), now))

			for _, want := range tt.wantLogs {
				assert.Contains(t, buf.String(), want)
			}

			for _, notWant := range tt.notWantLogs {
				assert.NotContains(t, buf.String(), notWant)
			}
		})
	}
}
