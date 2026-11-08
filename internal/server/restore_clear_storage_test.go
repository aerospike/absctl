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
	"slices"
	"testing"

	"github.com/aerospike/backup-go/pkg/asinfo"
	infomodels "github.com/aerospike/backup-go/pkg/asinfo/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	logStorageCleared = "storage was successfully cleared"
	logClearing       = "clearing storage in progress"
	// logClearRequested is the line that announces a clear request that was sent.
	logClearRequested = `msg="clearing storage"`
	logZeroedBytes    = "zeroed-bytes=1024"
	logZeroedBytesKey = "zeroed-bytes"

	testZeroedBytes = 1024

	// logFollowRestore is part of the hint on how to follow the restore, which a command
	// that does not stay with the job gives.
	logFollowRestore = "--watch ...'"
)

// testClearStatus reports a cold restore job that is being prepared, at the given clear
// testHeldReason is the reason the server gives for a node it holds at PREPARING.
const testHeldReason = "truncate-state-live"

// and namespace states.
func testClearStatus(
	clearState infomodels.ClearState, nsState infomodels.NamespaceState,
) *infomodels.ResponseRestoreState {
	status := testColdStatus(infomodels.RestoreStateActive, nsState)
	status.Clear = clearState

	return status
}

func TestServiceClearRestoreStorage(t *testing.T) {
	t.Parallel()

	clearing := restoreAnswer{status: testClearStatus(infomodels.ClearStateClearing, infomodels.NamespaceStatePreparing)}
	required := restoreAnswer{status: testClearStatus(infomodels.ClearStateRequired, infomodels.NamespaceStatePreparing)}

	// coldPreparing is the status the check made before the request reads by default.
	coldPreparing := restoreAnswer{status: testColdStatus(infomodels.RestoreStateActive, infomodels.NamespaceStatePreparing)}
	fuzzyActive := restoreAnswer{status: testFuzzyStatus(infomodels.RestoreStateActive)}

	ready := testClearStatus(infomodels.ClearStateNone, infomodels.NamespaceStateReady)
	ready.ClearZeroedBytes = testZeroedBytes
	readyAnswer := restoreAnswer{status: ready}

	tests := []struct {
		name      string
		async     bool
		clearErrs []error
		// preAnswer is the status the check made before the request reads, coldPreparing
		// when nil. wantCalls counts the lookups that follow it.
		preAnswer      *restoreAnswer
		answers        []restoreAnswer
		wantErr        error
		wantCalls      int
		wantClearCalls int
		wantLogs       []string
		notWantLogs    []string
	}{
		{
			name:           "waits for the namespace to become ready",
			answers:        []restoreAnswer{clearing, clearing, readyAnswer},
			wantCalls:      3,
			wantClearCalls: 1,
			// The wait names the state under the same label the other commands print.
			wantLogs: []string{
				logClearing, "state=" + stateClearing, logStorageCleared, logZeroedBytes,
			},
			// The start that waits for the clear already follows the restore.
			notWantLogs: []string{logFollowRestore},
		},
		{
			// A node that backed out of the clear asks for it again.
			name:           "clears again a node that asks for it",
			answers:        []restoreAnswer{clearing, required, clearing, readyAnswer},
			wantCalls:      4,
			wantClearCalls: 2,
			wantLogs:       []string{logStorageCleared},
		},
		{
			// A cleared node only reaches READY once the preparation finishes.
			name: "waits for the preparation that follows the clear",
			answers: []restoreAnswer{
				{status: testClearStatus(infomodels.ClearStateNone, infomodels.NamespaceStatePreparing)},
				readyAnswer,
			},
			wantCalls:      2,
			wantClearCalls: 1,
			wantLogs:       []string{logStorageCleared},
		},
		{
			// The cluster starts the restore as soon as every node is READY, so a poll
			// can miss READY and see the restore running already.
			name: "stops once the restore has started",
			answers: []restoreAnswer{
				clearing,
				{status: testClearStatus(infomodels.ClearStateNone, infomodels.NamespaceStateRestoring)},
			},
			wantCalls:      2,
			wantClearCalls: 1,
			wantLogs:       []string{logStorageCleared},
			notWantLogs:    []string{logFollowRestore},
		},
		{
			// A restore that completed between two polls got past its preparation too.
			name: "stops once the restore has completed",
			answers: []restoreAnswer{
				clearing,
				{status: testCompleteRestore(testColdStatus(infomodels.RestoreStateComplete, infomodels.NamespaceStateNone))},
			},
			wantCalls:      2,
			wantClearCalls: 1,
			wantLogs:       []string{logStorageCleared},
			notWantLogs:    []string{logFollowRestore},
		},
		{
			// No node had storage to clear, so there are no clear figures to report.
			name: "omits the clear figures when nothing was cleared",
			answers: []restoreAnswer{
				{status: testClearStatus(infomodels.ClearStateNone, infomodels.NamespaceStateReady)},
			},
			wantCalls:      1,
			wantClearCalls: 1,
			wantLogs:       []string{logStorageCleared},
			notWantLogs:    []string{logZeroedBytesKey},
		},
		{
			name:           "returns as soon as the request is accepted when async",
			async:          true,
			answers:        []restoreAnswer{clearing},
			wantCalls:      0,
			wantClearCalls: 1,
			notWantLogs:    []string{logStorageCleared},
		},
		{
			name:           "propagates a rejected request without polling",
			clearErrs:      []error{errBoom},
			answers:        []restoreAnswer{readyAnswer},
			wantErr:        errBoom,
			wantCalls:      0,
			wantClearCalls: 1,
			notWantLogs:    []string{logStorageCleared},
		},
		{
			name:           "propagates a rejected repeated request",
			clearErrs:      []error{nil, errBoom},
			answers:        []restoreAnswer{required},
			wantErr:        errBoom,
			wantCalls:      1,
			wantClearCalls: 2,
			notWantLogs:    []string{logStorageCleared},
		},
		{
			name: "fails when the job is aborted",
			answers: []restoreAnswer{
				clearing,
				{status: testColdStatus(infomodels.RestoreStateAborted, infomodels.NamespaceStatePreparing)},
			},
			wantErr:        errRestoreNotPreparing,
			wantCalls:      2,
			wantClearCalls: 1,
		},
		{
			// A fuzzy restore writes into the live namespace, so there is nothing to clear,
			// and the request is not sent.
			name:        "fails for a fuzzy restore",
			preAnswer:   &fuzzyActive,
			wantErr:     errClearNotCold,
			notWantLogs: []string{logClearRequested, logStorageCleared},
		},
		{
			name:        "fails for a fuzzy restore when async",
			async:       true,
			preAnswer:   &fuzzyActive,
			wantErr:     errClearNotCold,
			notWantLogs: []string{logClearRequested},
		},
		{
			// The wait refuses a job that turns out fuzzy after the request as well.
			name:           "fails for a restore reported as fuzzy while waiting",
			answers:        []restoreAnswer{fuzzyActive},
			wantErr:        errClearNotCold,
			wantCalls:      1,
			wantClearCalls: 1,
			notWantLogs:    []string{logStorageCleared},
		},
		{
			// A job the status does not report yet is left to the request.
			name:           "sends the request for a job the status does not report",
			preAnswer:      &restoreAnswer{err: asinfo.ErrNotFound},
			answers:        []restoreAnswer{readyAnswer},
			wantCalls:      1,
			wantClearCalls: 1,
			wantLogs:       []string{logStorageCleared},
		},
		{
			// A node held at PREPARING waits for the operator, who is told why.
			name: "reports why a node is held",
			answers: []restoreAnswer{
				{status: func() *infomodels.ResponseRestoreState {
					s := testClearStatus(infomodels.ClearStateNone, infomodels.NamespaceStatePreparing)
					s.HeldReason = testHeldReason

					return s
				}()},
				readyAnswer,
			},
			wantCalls:      2,
			wantClearCalls: 1,
			wantLogs:       []string{"held-reason=" + testHeldReason, logStorageCleared},
		},
		{
			name: "fails when the preparation fails",
			answers: []restoreAnswer{
				{status: testClearStatus(infomodels.ClearStateNone, infomodels.NamespaceStateFailed)},
			},
			wantErr:        errPrepareFailed,
			wantCalls:      1,
			wantClearCalls: 1,
		},
		{
			// The nodes accepted the clear, so the job is given as many lookups to turn
			// up as a just-started one before the wait gives up on it.
			name:           "fails when the job is never reported",
			wantErr:        errRestoreNotFound,
			wantCalls:      startedJobConfirmations,
			wantClearCalls: 1,
			notWantLogs:    []string{logStorageCleared},
		},
		{
			// A single miss is a gap in the merged status, not a job that is gone.
			name:           "waits through a job missing from the status for a moment",
			answers:        []restoreAnswer{clearing, {err: asinfo.ErrNotFound}, readyAnswer},
			wantCalls:      3,
			wantClearCalls: 1,
			wantLogs:       []string{logStorageCleared},
		},
		{
			name: "fails when the job stops reporting",
			answers: append(
				[]restoreAnswer{clearing},
				repeatRestoreNotFound(vanishedJobConfirmations)...,
			),
			wantErr:        errRestoreStatusLost,
			wantCalls:      1 + vanishedJobConfirmations,
			wantClearCalls: 1,
			notWantLogs:    []string{logStorageCleared},
		},
		{
			name:           "propagates an unexpected status error",
			answers:        []restoreAnswer{{err: errBoom}},
			wantErr:        errBoom,
			wantCalls:      1,
			wantClearCalls: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			svc, buf := newTestService(t)
			svc.restoreCfg.ClearStorage.Async = tt.async

			pre := coldPreparing
			if tt.preAnswer != nil {
				pre = *tt.preAnswer
			}

			client := &fakeRestoreInfoClient{
				answers:   append([]restoreAnswer{pre}, tt.answers...),
				clearErrs: tt.clearErrs,
			}

			err := svc.clearRestoreStorage(t.Context(), client)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}

			assert.Equal(t, 1+tt.wantCalls, client.calls)
			assert.Equal(t, tt.wantClearCalls, client.clearCalls)

			// Every clear request names the configured job.
			assert.ElementsMatch(t, slices.Repeat([]string{testNamespace + "/" + testJobID}, tt.wantClearCalls),
				client.clearTargets)

			for _, want := range tt.wantLogs {
				assert.Contains(t, buf.String(), want)
			}

			for _, notWant := range tt.notWantLogs {
				assert.NotContains(t, buf.String(), notWant)
			}
		})
	}
}

// TestNamespacePrepared covers every namespace state that says the preparation is over,
// apart from the shortcut of a complete job, and the states that say it is not.
func TestNamespacePrepared(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		giveClear infomodels.ClearState
		giveNs    infomodels.NamespaceState
		want      bool
	}{
		{name: "ready", giveClear: infomodels.ClearStateNone, giveNs: infomodels.NamespaceStateReady, want: true},
		{name: "restoring", giveClear: infomodels.ClearStateNone, giveNs: infomodels.NamespaceStateRestoring, want: true},
		{name: "opening", giveClear: infomodels.ClearStateNone, giveNs: infomodels.NamespaceStateOpening, want: true},
		// NONE is also what a node reports before its gate goes up, so only COMPLETE
		// confirms a namespace that was already released.
		{name: "gate not up or released", giveClear: infomodels.ClearStateNone, giveNs: infomodels.NamespaceStateNone},
		{name: "preparing", giveClear: infomodels.ClearStateNone, giveNs: infomodels.NamespaceStatePreparing},
		{name: "failed", giveClear: infomodels.ClearStateNone, giveNs: infomodels.NamespaceStateFailed},
		{name: "clear required", giveClear: infomodels.ClearStateRequired, giveNs: infomodels.NamespaceStateReady},
		{name: "clearing", giveClear: infomodels.ClearStateClearing, giveNs: infomodels.NamespaceStateReady},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, namespacePrepared(testClearStatus(tt.giveClear, tt.giveNs)))
		})
	}
}
