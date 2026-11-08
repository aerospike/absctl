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
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/aerospike/absctl/internal/models"
	"github.com/aerospike/backup-go/pkg/asinfo"
	infomodels "github.com/aerospike/backup-go/pkg/asinfo/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The parts of the commands the hints tell the user to run: the command the
// subcommand follows, the flag that names the namespace, the flags that name
// testNamespace and the restore job testJobID of it, and the end that stands for the
// connection flags.
const (
	cmdRestore       = "absctl snapshot-restore "
	cmdNamespace     = " --namespace "
	cmdTestNamespace = cmdNamespace + testNamespace
	cmdTestJob       = cmdTestNamespace + " " + flagJobID + " " + testJobID
	cmdMore          = " ..."
)

// logClearHint is part of the warning about a storage that must be cleared, and
// logClearWait the part a watch adds to say it waits for the clear.
const (
	logClearHint = "'" + cmdRestore + restoreCmdClearStorage + cmdNamespace
	logClearWait = "waiting for the clear"
)

// The commands the hints of a restore job of testNamespace and testJobID tell the user
// to run, quoted as the hints print them.
const (
	logClearCmd  = "'" + cmdRestore + restoreCmdClearStorage + cmdTestJob + cmdMore + "'"
	logFollowCmd = "'" + cmdRestore + restoreCmdProgress + cmdTestJob + " " + flagWatch + cmdMore + "'"
	// The open names the job, which it needs to follow the release.
	logOpenCmd = "'" + cmdRestore + restoreCmdOpen + cmdTestJob + cmdMore + "'"
)

// The state labels of a restore line, as the info level prints them.
const (
	logStateRestoring = "state=" + stateRestoring
	logStateComplete  = "state=" + stateComplete
)

// TestServiceRestoreProgress covers the mode the progress command picks: a single report,
// or a watch that lasts until the restore ends.
func TestServiceRestoreProgress(t *testing.T) {
	t.Parallel()

	answers := []restoreAnswer{
		{status: testFuzzyStatus(infomodels.RestoreStateActive)},
		{status: testCompleteRestore(testFuzzyStatus(infomodels.RestoreStateComplete))},
	}

	tests := []struct {
		name      string
		giveWatch bool
		wantCalls int
	}{
		{name: "reports once", wantCalls: 1},
		{name: "watches until the restore ends", giveWatch: true, wantCalls: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			svc, _ := newTestService(t)
			svc.restoreCfg.Progress = &models.ServerRestoreProgress{
				Namespace: testNamespace,
				JobID:     testJobID,
				Watch:     tt.giveWatch,
			}
			client := &fakeRestoreInfoClient{answers: answers}

			require.NoError(t, svc.restoreProgress(t.Context(), client))
			assert.Equal(t, tt.wantCalls, client.calls)
		})
	}
}

func TestServiceReportRestoreProgress(t *testing.T) {
	t.Parallel()

	fuzzyActive := restoreAnswer{status: testFuzzyStatus(infomodels.RestoreStateActive)}
	fuzzyComplete := restoreAnswer{status: testCompleteRestore(testFuzzyStatus(infomodels.RestoreStateComplete))}
	coldClearRequired := restoreAnswer{status: testClearStatus(infomodels.ClearStateRequired, infomodels.NamespaceStatePreparing)}
	coldRestoring := restoreAnswer{status: testColdStatus(infomodels.RestoreStateActive, infomodels.NamespaceStateRestoring)}
	coldComplete := restoreAnswer{
		status: testCompleteRestore(testColdStatus(infomodels.RestoreStateComplete, infomodels.NamespaceStateNone)),
	}

	tests := []struct {
		name        string
		mode        progressMode
		answers     []restoreAnswer
		wantErr     error
		wantCalls   int
		wantLogs    []string
		notWantLogs []string
	}{
		{
			name:      "follows a fuzzy restore to completion",
			mode:      followJob,
			answers:   []restoreAnswer{fuzzyActive, fuzzyComplete},
			wantCalls: 2,
			wantLogs:  []string{msgRestoreProgress, "pct=100", msgRestoreComplete, logDuration},
		},
		{
			name: "follows a cold restore to completion",
			mode: followJob,
			answers: []restoreAnswer{
				{status: testColdStatus(infomodels.RestoreStateActive, infomodels.NamespaceStateRestoring)},
				{status: testCompleteRestore(testColdStatus(infomodels.RestoreStateComplete, infomodels.NamespaceStateNone))},
			},
			wantCalls: 2,
			wantLogs:  []string{logStateRestoring, logStateComplete, "pct=100", msgRestoreComplete},
		},
		{
			name: "keeps watching a cold restore that waits for a clear",
			mode: followJob,
			answers: []restoreAnswer{
				coldClearRequired,
				coldRestoring,
				coldComplete,
			},
			wantCalls: 3,
			wantLogs:  []string{logClearHint, logClearWait, msgRestoreComplete},
			// The watch itself goes on following the restore, so it is not advised.
			notWantLogs: []string{logFollowRestore},
		},
		{
			// The restore cannot start until the user clears the storage, so the
			// command that started it says how and waits for the clear done in another
			// session, then follows the restore.
			name: "waits for a clear of a just-started cold restore and follows it",
			mode: followStartedJob,
			answers: []restoreAnswer{
				coldClearRequired,
				coldClearRequired,
				coldRestoring,
				coldComplete,
			},
			wantCalls:   4,
			wantLogs:    []string{logClearHint, logClearWait, logStateRestoring, msgRestoreComplete},
			notWantLogs: []string{logFollowRestore},
		},
		{
			// A single report does not stay with the job, so it tells how to follow it.
			name:        "reports once a cold restore that waits for a clear",
			mode:        reportOnce,
			answers:     []restoreAnswer{coldClearRequired},
			wantCalls:   1,
			wantLogs:    []string{logClearCmd, logFollowCmd},
			notWantLogs: []string{logClearWait},
		},
		{
			name:      "reports once without waiting for the end",
			mode:      reportOnce,
			answers:   []restoreAnswer{fuzzyActive, fuzzyComplete},
			wantCalls: 1,
			wantLogs:  []string{msgRestoreProgress, "job-id=" + testJobID},
			notWantLogs: []string{
				msgRestoreComplete,
			},
		},
		{
			name:        "reports a job the cluster does not report",
			mode:        reportOnce,
			wantErr:     errRestoreNotFound,
			wantCalls:   1,
			notWantLogs: []string{msgRestoreProgress},
		},
		{
			name:      "reports a job named on the command line the cluster does not report",
			mode:      followJob,
			wantErr:   errRestoreNotFound,
			wantCalls: 1,
		},
		{
			// The status of a job the cluster has just accepted may not list it yet.
			name:      "waits for a job that has just been started",
			mode:      followStartedJob,
			answers:   append(repeatRestoreNotFound(startedJobConfirmations-1), fuzzyComplete),
			wantCalls: startedJobConfirmations,
			wantLogs:  []string{msgRestoreComplete},
		},
		{
			name:      "gives up on a started job the cluster never reports",
			mode:      followStartedJob,
			wantErr:   errRestoreNotFound,
			wantCalls: startedJobConfirmations,
		},
		{
			// The cluster keeps the state of a finished job, so a job that stops being
			// reported has not completed.
			name:      "reports a job that stopped being reported",
			mode:      followJob,
			answers:   []restoreAnswer{fuzzyActive},
			wantErr:   errRestoreStatusLost,
			wantCalls: 1 + vanishedJobConfirmations,
		},
		{
			// The abort of a fuzzy restore removes the job, so an aborting fuzzy job that
			// leaves the status was aborted.
			name:      "reports an aborting fuzzy job that leaves the status as aborted",
			mode:      followJob,
			answers:   []restoreAnswer{{status: testFuzzyStatus(infomodels.RestoreStateAborting)}},
			wantErr:   errRestoreAborted,
			wantCalls: 1 + vanishedJobConfirmations,
		},
		{
			// A cold job keeps its state when it is aborted, so its leaving the status is
			// not an abort.
			name: "reports an aborting cold job that leaves the status as lost",
			mode: followJob,
			answers: []restoreAnswer{
				{status: testColdStatus(infomodels.RestoreStateAborting, infomodels.NamespaceStateRestoring)},
			},
			wantErr:   errRestoreStatusLost,
			wantCalls: 1 + vanishedJobConfirmations,
		},
		{
			name:      "rides out a job missing from a single status",
			mode:      followJob,
			answers:   []restoreAnswer{fuzzyActive, {err: asinfo.ErrNotFound}, fuzzyComplete},
			wantCalls: 3,
			wantLogs:  []string{msgRestoreComplete},
		},
		{
			name:        "reports a failed fuzzy restore without the open hint",
			mode:        followJob,
			answers:     []restoreAnswer{{status: testFailedRestore(testFuzzyStatus(infomodels.RestoreStateFailed))}},
			wantErr:     errRestoreFailed,
			wantCalls:   1,
			notWantLogs: []string{logOpenHint},
		},
		{
			// A failed cold restore keeps the namespace closed until it is opened.
			name: "reports a failed cold restore and tells how to release the namespace",
			mode: followJob,
			answers: []restoreAnswer{{status: testFailedRestore(
				testColdStatus(infomodels.RestoreStateFailed, infomodels.NamespaceStateRestoring))}},
			wantErr:   errRestoreFailed,
			wantCalls: 1,
			wantLogs:  []string{logOpenCmd},
		},
		{
			name: "reports an aborted cold restore and tells how to open the namespace",
			mode: followJob,
			answers: []restoreAnswer{{status: testFinishedRestore(
				testColdStatus(infomodels.RestoreStateAborted, infomodels.NamespaceStateRestoring))}},
			wantErr:   errRestoreAborted,
			wantCalls: 1,
			wantLogs:  []string{logOpenCmd},
		},
		{
			name:        "reports an aborted fuzzy restore",
			mode:        followJob,
			answers:     []restoreAnswer{{status: testFinishedRestore(testFuzzyStatus(infomodels.RestoreStateAborted))}},
			wantErr:     errRestoreAborted,
			wantCalls:   1,
			notWantLogs: []string{logOpenHint},
		},
		{
			name:      "propagates an unexpected status error",
			mode:      followJob,
			answers:   []restoreAnswer{{err: errBoom}},
			wantErr:   errBoom,
			wantCalls: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			svc, buf := newTestService(t)
			client := &fakeRestoreInfoClient{answers: tt.answers}

			err := svc.reportRestoreProgress(t.Context(), client, testNamespace, testJobID, tt.mode)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}

			assert.Equal(t, tt.wantCalls, client.calls)

			for _, want := range tt.wantLogs {
				assert.Contains(t, buf.String(), want)
			}

			for _, notWant := range tt.notWantLogs {
				assert.NotContains(t, buf.String(), notWant)
			}
		})
	}
}

// TestServiceReportRestoreProgressNamesTheFailure makes sure the reason the cluster gave
// for a failed job reaches the error.
func TestServiceReportRestoreProgressNamesTheFailure(t *testing.T) {
	t.Parallel()

	svc, _ := newTestService(t)
	client := &fakeRestoreInfoClient{answers: []restoreAnswer{
		{status: testFailedRestore(testFuzzyStatus(infomodels.RestoreStateFailed))},
	}}

	err := svc.reportRestoreProgress(t.Context(), client, testNamespace, testJobID, followJob)

	require.ErrorIs(t, err, errRestoreFailed)
	require.ErrorContains(t, err, testErrorReason)
	require.ErrorContains(t, err, testJobID)
}

// TestServiceReportRestoreProgressWarnsOnceAboutClear makes sure a watch that keeps
// seeing the same pending clear does not repeat the warning on every poll, whether it
// follows a job named on the command line or the one the start has just requested.
func TestServiceReportRestoreProgressWarnsOnceAboutClear(t *testing.T) {
	t.Parallel()

	clearRequired := restoreAnswer{
		status: testClearStatus(infomodels.ClearStateRequired, infomodels.NamespaceStatePreparing),
	}

	tests := []struct {
		name string
		mode progressMode
	}{
		{name: "follow a job named on the command line", mode: followJob},
		{name: "follow a just-started job", mode: followStartedJob},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			svc, buf := newTestService(t)
			client := &fakeRestoreInfoClient{answers: []restoreAnswer{
				clearRequired,
				clearRequired,
				{status: testCompleteRestore(testColdStatus(infomodels.RestoreStateComplete, infomodels.NamespaceStateNone))},
			}}

			require.NoError(t, svc.reportRestoreProgress(t.Context(), client, testNamespace, testJobID, tt.mode))

			// The warning is the only line of the watch at the warning level, and it names
			// the command to run with the job, in a message the user can copy it from.
			require.Equal(t, 1, strings.Count(buf.String(), "level=WARN"))

			var warning string

			for line := range strings.Lines(buf.String()) {
				if strings.Contains(line, "level=WARN") {
					warning = line
				}
			}

			assert.Contains(t, warning, logClearCmd)
			// The job is named in the command only, not repeated as attributes.
			assert.NotContains(t, warning, "command=")
			assert.NotContains(t, warning, "namespace=")
			assert.NotContains(t, warning, "job-id=")
		})
	}
}

func TestRestoreSnapshot(t *testing.T) {
	t.Parallel()

	const (
		recsPerSec = 1000
		heldReason = "truncate-state-live"
		// attrJobID and attrState are the keys every restore line opens with.
		attrJobID         = "job-id"
		attrState         = "state"
		attrHeldReason    = "held-reason"
		attrBackupID      = "backup-id"
		attrBackupEntry   = "backup-entry"
		attrBackupEntries = "backup-entries"
		// The keys of the full state, printed at the debug level only.
		attrDescription = "description"
		attrServerState = "server-state"
		attrNsState     = "ns-state"
		attrClear       = "clear"
	)

	tests := []struct {
		name           string
		status         *infomodels.ResponseRestoreState
		wantStage      string
		wantRatio      float64
		wantRecsPerSec uint64
		wantAttrs      []string
		wantDebugAttrs []string
	}{
		{
			name: "fuzzy restore reports the progress and the rate of the cluster",
			status: &infomodels.ResponseRestoreState{
				JobID:         testJobID,
				Kind:          infomodels.RestoreKindFuzzy,
				State:         infomodels.RestoreStateActive,
				BackupEntries: 1,
				ProgressPct:   65.9,
				RecsPerSec:    recsPerSec,
			},
			wantStage:      stateRestoring,
			wantRatio:      0.659,
			wantRecsPerSec: recsPerSec,
			wantAttrs:      []string{attrJobID, attrState},
			wantDebugAttrs: []string{attrDescription, attrServerState},
		},
		{
			// The namespace state tells the stages of a cold restore apart, and it is
			// printed in full at the debug level.
			name: "cold restore is labeled after the namespace state",
			status: &infomodels.ResponseRestoreState{
				JobID:         testJobID,
				Kind:          infomodels.RestoreKindCold,
				State:         infomodels.RestoreStateActive,
				NsState:       infomodels.NamespaceStateRestoring,
				BackupEntries: 1,
				ProgressPct:   61.2,
				RecsPerSec:    recsPerSec,
			},
			wantStage:      stateRestoring,
			wantRatio:      0.612,
			wantRecsPerSec: recsPerSec,
			wantAttrs:      []string{attrJobID, attrState},
			wantDebugAttrs: []string{attrDescription, attrServerState, attrNsState, attrClear},
		},
		{
			name: "cold restore held in preparation names the reason",
			status: &infomodels.ResponseRestoreState{
				JobID:         testJobID,
				Kind:          infomodels.RestoreKindCold,
				State:         infomodels.RestoreStateActive,
				NsState:       infomodels.NamespaceStatePreparing,
				HeldReason:    heldReason,
				BackupEntries: 1,
			},
			wantStage:      "preparing",
			wantAttrs:      []string{attrJobID, attrState, attrHeldReason},
			wantDebugAttrs: []string{attrDescription, attrServerState, attrNsState, attrClear},
		},
		{
			name: "restore of several backups names the current one",
			status: &infomodels.ResponseRestoreState{
				JobID:         testJobID,
				Kind:          infomodels.RestoreKindFuzzy,
				State:         infomodels.RestoreStateActive,
				BackupID:      testJobID,
				BackupEntries: 2,
			},
			wantStage:      stateRestoring,
			wantAttrs:      []string{attrJobID, attrState, attrBackupID, attrBackupEntry, attrBackupEntries},
			wantDebugAttrs: []string{attrDescription, attrServerState},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			snap := restoreSnapshot(tt.status)

			assert.Equal(t, tt.wantStage, snap.stage)
			assert.InDelta(t, tt.wantRatio, snap.ratio, 1e-9)
			assert.Equal(t, tt.wantRecsPerSec, snap.recsPerSec)
			assert.Equal(t, tt.wantAttrs, attrKeys(t, snap.attrs))
			assert.Equal(t, tt.wantDebugAttrs, attrKeys(t, snap.debugAttrs()))
		})
	}
}

// attrKeys returns the keys of the slog attributes attrs.
func attrKeys(t *testing.T, attrs []any) []string {
	t.Helper()

	keys := make([]string, 0, len(attrs))

	for _, attr := range attrs {
		a, ok := attr.(slog.Attr)
		require.True(t, ok)

		keys = append(keys, a.Key)
	}

	return keys
}

// TestProgressPrinterPrintRestore pins down the restore progress line, which reports the
// progress and the record rate the cluster reports for both kinds of restore.
func TestProgressPrinterPrintRestore(t *testing.T) {
	t.Parallel()

	now := time.Now()

	tests := []struct {
		name     string
		status   *infomodels.ResponseRestoreState
		wantLogs []string
	}{
		{
			name: "fuzzy restore",
			status: &infomodels.ResponseRestoreState{
				JobID:         testJobID,
				Kind:          infomodels.RestoreKindFuzzy,
				State:         infomodels.RestoreStateActive,
				BackupEntries: 1,
				ProgressPct:   50,
				RecsPerSec:    200,
				StartTime:     now.Add(-time.Hour),
			},
			wantLogs: []string{msgRestoreProgress, "job-id=" + testJobID, "pct=50", "rec/s=200", "remaining=1h0m0s"},
		},
		{
			name: "cold restore",
			status: &infomodels.ResponseRestoreState{
				JobID:         testJobID,
				Kind:          infomodels.RestoreKindCold,
				State:         infomodels.RestoreStateActive,
				NsState:       infomodels.NamespaceStateRestoring,
				BackupEntries: 1,
				ProgressPct:   25,
				RecsPerSec:    300,
				StartTime:     now.Add(-time.Hour),
			},
			wantLogs: []string{msgRestoreProgress, logStateRestoring, "pct=25", "rec/s=300", "remaining=3h0m0s"},
		},
		{
			// The job cannot finish until the nodes that left come back, which is the
			// only reason the operator gets for a stalled restore.
			name: "cold restore missing layout nodes",
			status: &infomodels.ResponseRestoreState{
				JobID:              testJobID,
				Kind:               infomodels.RestoreKindCold,
				State:              infomodels.RestoreStateActive,
				NsState:            infomodels.NamespaceStateRestoring,
				BackupEntries:      1,
				MissingLayoutNodes: 2,
				ProgressPct:        25,
				StartTime:          now.Add(-time.Hour),
			},
			wantLogs: []string{"missing-layout-nodes=2"},
		},
		{
			name: "complete cold restore without a merged percentage",
			status: &infomodels.ResponseRestoreState{
				JobID:         testJobID,
				Kind:          infomodels.RestoreKindCold,
				State:         infomodels.RestoreStateComplete,
				NsState:       infomodels.NamespaceStateNone,
				BackupEntries: 1,
				StartTime:     now.Add(-time.Hour),
			},
			wantLogs: []string{msgRestoreProgress, logStateComplete, "pct=100"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var buf bytes.Buffer

			logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
			printer := newProgressPrinter(logger, msgRestoreProgress)

			require.True(t, printer.Print(t.Context(), restoreSnapshot(tt.status), now))

			for _, want := range tt.wantLogs {
				assert.Contains(t, buf.String(), want)
			}
		})
	}
}

func TestWaitsForPreparation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		give *infomodels.ResponseRestoreState
		want bool
	}{
		{
			name: "cold restore preparing",
			give: testColdStatus(infomodels.RestoreStateActive, infomodels.NamespaceStatePreparing),
			want: true,
		},
		{
			name: "cold restore prepared",
			give: testColdStatus(infomodels.RestoreStateActive, infomodels.NamespaceStateReady),
			want: true,
		},
		{
			name: "cold restore hydrating",
			give: testColdStatus(infomodels.RestoreStateActive, infomodels.NamespaceStateRestoring),
		},
		{
			name: "cold restore stopping while preparing",
			give: testColdStatus(infomodels.RestoreStateAborting, infomodels.NamespaceStatePreparing),
		},
		{
			name: "fuzzy restore",
			give: testFuzzyStatus(infomodels.RestoreStateActive),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, waitsForPreparation(tt.give))
		})
	}
}

func TestRestoreCommand(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		giveSubcommand string
		giveExtra      []string
		want           string
	}{
		{
			name:           "names the namespace only",
			giveSubcommand: restoreCmdOpen,
			want:           cmdRestore + restoreCmdOpen + cmdTestNamespace + cmdMore,
		},
		{
			name:           "puts the extra flags after the namespace",
			giveSubcommand: restoreCmdProgress,
			giveExtra:      []string{flagJobID, testJobID, flagWatch},
			want:           cmdRestore + restoreCmdProgress + cmdTestJob + " " + flagWatch + cmdMore,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, restoreCommand(tt.giveSubcommand, testNamespace, tt.giveExtra...))
		})
	}
}

// TestServiceLogOpenHint pins down that the open hint names the job when it is known, so
// that the copied command follows the release, and the namespace alone otherwise.
func TestServiceLogOpenHint(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		giveJobID string
		want      string
	}{
		{name: "with the job id", giveJobID: testJobID, want: logOpenCmd},
		{name: "without the job id", want: "'" + cmdRestore + restoreCmdOpen + cmdTestNamespace + cmdMore + "'"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			svc, buf := newTestService(t)

			svc.logOpenHint(testNamespace, tt.giveJobID)

			assert.Contains(t, buf.String(), tt.want)
		})
	}
}
