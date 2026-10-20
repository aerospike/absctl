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
	"testing"

	"github.com/aerospike/backup-go/pkg/asinfo"
	infomodels "github.com/aerospike/backup-go/pkg/asinfo/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	logStorageCleared = "storage was successfully cleared"
	logClearing       = "clearing storage in progress"
	logZeroedBytes    = "zeroed-bytes=1024"
	logZeroedBytesKey = "zeroed-bytes"

	testZeroedBytes = 1024
)

// testClearStatus reports a cold restore job that is being prepared, at the given clear
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

	ready := testClearStatus(infomodels.ClearStateNone, infomodels.NamespaceStateReady)
	ready.ClearZeroedBytes = testZeroedBytes
	readyAnswer := restoreAnswer{status: ready}

	tests := []struct {
		name           string
		async          bool
		clearErrs      []error
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
			wantLogs:       []string{logClearing, logStorageCleared, logZeroedBytes},
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
			client := &fakeRestoreInfoClient{answers: tt.answers, clearErrs: tt.clearErrs}

			err := svc.clearRestoreStorage(t.Context(), client)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}

			assert.Equal(t, tt.wantCalls, client.calls)
			assert.Equal(t, tt.wantClearCalls, client.clearCalls)
			assert.Equal(t, testNamespace, client.clearNamespace)
			assert.Equal(t, testJobID, client.clearJobID)

			for _, want := range tt.wantLogs {
				assert.Contains(t, buf.String(), want)
			}

			for _, notWant := range tt.notWantLogs {
				assert.NotContains(t, buf.String(), notWant)
			}
		})
	}
}
