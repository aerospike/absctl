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
	"testing"
	"time"

	"github.com/aerospike/absctl/internal/models"
	"github.com/aerospike/backup-go/pkg/asinfo"
	infomodels "github.com/aerospike/backup-go/pkg/asinfo/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const logAbortOpenHint = "snapshot-restore abort"

// TestServiceRestoreProgress covers the mode the progress command picks: a single report,
// or a watch that lasts until the restore ends.
func TestServiceRestoreProgress(t *testing.T) {
	t.Parallel()

	answers := []restoreAnswer{
		{status: testFuzzyStatus(infomodels.RestoreStateActive)},
		{status: testFinishedRestore(testFuzzyStatus(infomodels.RestoreStateComplete))},
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

			require.NoError(t, svc.restoreProgress(t.Context(), client, testLayout))
			assert.Equal(t, tt.wantCalls, client.calls)
		})
	}
}

func TestServiceReportRestoreProgress(t *testing.T) {
	t.Parallel()

	fuzzyActive := restoreAnswer{status: testFuzzyStatus(infomodels.RestoreStateActive)}
	fuzzyComplete := restoreAnswer{status: testFinishedRestore(testFuzzyStatus(infomodels.RestoreStateComplete))}

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
				{status: testFinishedRestore(testColdStatus(infomodels.RestoreStateComplete, infomodels.NamespaceStateNone))},
			},
			wantCalls: 2,
			wantLogs:  []string{"ns-state=RESTORING", "ns-state=NONE", msgRestoreComplete},
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
			notWantLogs: []string{logAbortOpenHint, logOpenHint},
		},
		{
			// A failed cold restore keeps the namespace closed until it is aborted and
			// opened.
			name: "reports a failed cold restore and tells how to release the namespace",
			mode: followJob,
			answers: []restoreAnswer{{status: testFailedRestore(
				testColdStatus(infomodels.RestoreStateFailed, infomodels.NamespaceStateRestoring))}},
			wantErr:   errRestoreFailed,
			wantCalls: 1,
			wantLogs:  []string{logAbortOpenHint},
		},
		{
			name: "reports an aborted cold restore and tells how to open the namespace",
			mode: followJob,
			answers: []restoreAnswer{{status: testFinishedRestore(
				testColdStatus(infomodels.RestoreStateAborted, infomodels.NamespaceStateRestoring))}},
			wantErr:     errRestoreAborted,
			wantCalls:   1,
			wantLogs:    []string{logOpenHint},
			notWantLogs: []string{logAbortOpenHint},
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

			err := svc.reportRestoreProgress(t.Context(), client, testLayout, testNamespace, testJobID, tt.mode)

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

	err := svc.reportRestoreProgress(t.Context(), client, testLayout, testNamespace, testJobID, followJob)

	require.ErrorIs(t, err, errRestoreFailed)
	require.ErrorContains(t, err, testErrorReason)
	require.ErrorContains(t, err, testJobID)
}

func TestRestoreRatio(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		status      *infomodels.ResponseRestoreState
		layoutNodes int
		want        float64
	}{
		{
			name: "complete job is done whatever its counters say",
			status: &infomodels.ResponseRestoreState{
				Kind:  infomodels.RestoreKindCold,
				State: infomodels.RestoreStateComplete,
			},
			want: 1,
		},
		{
			name: "fuzzy restore of a single backup counts covered partitions",
			status: &infomodels.ResponseRestoreState{
				Kind:          infomodels.RestoreKindFuzzy,
				State:         infomodels.RestoreStateActive,
				BackupEntries: 1,
				Covered:       partitionCount / 4,
			},
			want: 0.25,
		},
		{
			// The first backup is applied in full, the second one is half covered.
			name: "fuzzy restore counts the backups applied before the current one",
			status: &infomodels.ResponseRestoreState{
				Kind:          infomodels.RestoreKindFuzzy,
				State:         infomodels.RestoreStateActive,
				BackupEntry:   1,
				BackupEntries: 2,
				Covered:       partitionCount / 2,
			},
			want: 0.75,
		},
		{
			// A cluster wide coverage counter can overshoot the partition count.
			name: "fuzzy coverage is capped at the partition count",
			status: &infomodels.ResponseRestoreState{
				Kind:          infomodels.RestoreKindFuzzy,
				State:         infomodels.RestoreStateActive,
				BackupEntries: 1,
				Covered:       2 * partitionCount,
			},
			want: 1,
		},
		{
			name: "fuzzy restore with no entries reported",
			status: &infomodels.ResponseRestoreState{
				Kind:    infomodels.RestoreKindFuzzy,
				State:   infomodels.RestoreStateActive,
				Covered: partitionCount / 2,
			},
			want: 0.5,
		},
		{
			name: "prepared cold restore has not started",
			status: &infomodels.ResponseRestoreState{
				Kind:          infomodels.RestoreKindCold,
				State:         infomodels.RestoreStateActive,
				NsState:       infomodels.NamespaceStateReady,
				BackupEntries: 1,
				NDoneNodes:    1,
			},
			layoutNodes: testLayoutNodes,
			want:        0,
		},
		{
			name: "cold restore counts the nodes that finished",
			status: &infomodels.ResponseRestoreState{
				Kind:          infomodels.RestoreKindCold,
				State:         infomodels.RestoreStateActive,
				NsState:       infomodels.NamespaceStateRestoring,
				BackupEntries: 1,
				NDoneNodes:    1,
			},
			layoutNodes: 4,
			want:        0.25,
		},
		{
			// The newest backup is applied first, so the current entry counts down.
			name: "cold restore counts the backups applied before the current one",
			status: &infomodels.ResponseRestoreState{
				Kind:          infomodels.RestoreKindCold,
				State:         infomodels.RestoreStateActive,
				NsState:       infomodels.NamespaceStateRestoring,
				BackupEntry:   0,
				BackupEntries: 2,
				NDoneNodes:    2,
			},
			layoutNodes: 4,
			want:        0.75,
		},
		{
			name: "cold restore without layout nodes",
			status: &infomodels.ResponseRestoreState{
				Kind:          infomodels.RestoreKindCold,
				State:         infomodels.RestoreStateActive,
				NsState:       infomodels.NamespaceStateRestoring,
				BackupEntries: 1,
				NDoneNodes:    1,
			},
			want: 0,
		},
		{
			name: "done nodes are capped at the layout",
			status: &infomodels.ResponseRestoreState{
				Kind:          infomodels.RestoreKindCold,
				State:         infomodels.RestoreStateActive,
				NsState:       infomodels.NamespaceStateRestoring,
				BackupEntries: 1,
				NDoneNodes:    testLayoutNodes + 1,
			},
			layoutNodes: testLayoutNodes,
			want:        1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.InDelta(t, tt.want, restoreRatio(tt.status, tt.layoutNodes), 1e-9)
		})
	}
}

func TestRestoreSnapshot(t *testing.T) {
	t.Parallel()

	const (
		applied = 1000
		// attrJobID and attrState are the keys every restore line opens with.
		attrJobID         = "job-id"
		attrState         = "state"
		attrNsState       = "ns-state"
		attrBackupID      = "backup-id"
		attrBackupEntry   = "backup-entry"
		attrBackupEntries = "backup-entries"
	)

	stateStage := string(infomodels.RestoreStateActive)
	coldStage := stateStage + "/" + string(infomodels.NamespaceStateRestoring)

	tests := []struct {
		name           string
		status         *infomodels.ResponseRestoreState
		wantStage      string
		wantHasRecords bool
		wantRecords    uint64
		wantAttrs      []string
	}{
		{
			name: "fuzzy restore reports the applied records",
			status: &infomodels.ResponseRestoreState{
				JobID:         testJobID,
				Kind:          infomodels.RestoreKindFuzzy,
				State:         infomodels.RestoreStateActive,
				BackupEntries: 1,
				Applied:       applied,
			},
			wantStage:      stateStage,
			wantHasRecords: true,
			wantRecords:    applied,
			wantAttrs:      []string{attrJobID, attrState},
		},
		{
			// The namespace state tells the stages of a cold restore apart.
			name: "cold restore reports the namespace state and no speed",
			status: &infomodels.ResponseRestoreState{
				JobID:         testJobID,
				Kind:          infomodels.RestoreKindCold,
				State:         infomodels.RestoreStateActive,
				NsState:       infomodels.NamespaceStateRestoring,
				BackupEntries: 1,
			},
			wantStage: coldStage,
			wantAttrs: []string{attrJobID, attrState, attrNsState},
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
			wantStage:      stateStage,
			wantHasRecords: true,
			wantAttrs:      []string{attrJobID, attrState, attrBackupID, attrBackupEntry, attrBackupEntries},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			snap := restoreSnapshot(tt.status, testLayoutNodes)

			assert.Equal(t, tt.wantStage, snap.stage)
			assert.Equal(t, tt.wantHasRecords, snap.hasRecords)
			assert.Equal(t, tt.wantRecords, snap.records)

			keys := make([]string, 0, len(snap.attrs))
			for _, attr := range snap.attrs {
				a, ok := attr.(slog.Attr)
				require.True(t, ok)

				keys = append(keys, a.Key)
			}

			assert.Equal(t, tt.wantAttrs, keys)
		})
	}
}

// TestProgressPrinterPrintRestore pins down the restore progress line: a cold restore
// counts no records, so it reports no speed.
func TestProgressPrinterPrintRestore(t *testing.T) {
	t.Parallel()

	now := time.Now()

	tests := []struct {
		name        string
		status      *infomodels.ResponseRestoreState
		wantLogs    []string
		notWantLogs []string
	}{
		{
			name: "fuzzy restore reports the speed",
			status: &infomodels.ResponseRestoreState{
				JobID:         testJobID,
				Kind:          infomodels.RestoreKindFuzzy,
				State:         infomodels.RestoreStateActive,
				BackupEntries: 1,
				Covered:       partitionCount / 2,
				StartTime:     now.Add(-time.Hour),
			},
			wantLogs: []string{msgRestoreProgress, "job-id=" + testJobID, "pct=50", "rec/s=0", "remaining=1h0m0s"},
		},
		{
			name: "cold restore reports no speed",
			status: &infomodels.ResponseRestoreState{
				JobID:         testJobID,
				Kind:          infomodels.RestoreKindCold,
				State:         infomodels.RestoreStateActive,
				NsState:       infomodels.NamespaceStateRestoring,
				BackupEntries: 1,
				StartTime:     now.Add(-time.Hour),
			},
			wantLogs:    []string{msgRestoreProgress, "ns-state=RESTORING", "pct=0"},
			notWantLogs: []string{"rec/s="},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var buf bytes.Buffer

			logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
			printer := newProgressPrinter(logger, msgRestoreProgress)

			require.True(t, printer.Print(restoreSnapshot(tt.status, testLayoutNodes), now))

			for _, want := range tt.wantLogs {
				assert.Contains(t, buf.String(), want)
			}

			for _, notWant := range tt.notWantLogs {
				assert.NotContains(t, buf.String(), notWant)
			}
		})
	}
}
