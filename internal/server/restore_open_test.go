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
	"log/slog"
	"testing"

	"github.com/aerospike/backup-go/pkg/asinfo"
	infomodels "github.com/aerospike/backup-go/pkg/asinfo/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const logNamespaceOpened = "namespace was successfully opened"

func TestServiceOpenRestore(t *testing.T) {
	t.Parallel()

	opening := restoreAnswer{status: testColdStatus(infomodels.RestoreStateAborted, infomodels.NamespaceStateOpening)}
	opened := restoreAnswer{status: testColdStatus(infomodels.RestoreStateAborted, infomodels.NamespaceStateNone)}

	tests := []struct {
		name        string
		async       bool
		openErr     error
		answers     []restoreAnswer
		wantErr     error
		wantCalls   int
		wantLogs    []string
		notWantLogs []string
	}{
		{
			name:      "waits for the namespace to open",
			answers:   []restoreAnswer{opening, opening, opened},
			wantCalls: 3,
			wantLogs:  []string{logNamespaceOpened},
		},
		{
			// A job missing from the status cannot confirm the release, so the wait goes
			// on rather than announcing an open namespace.
			name:      "waits through a job missing from the status",
			answers:   []restoreAnswer{{err: asinfo.ErrNotFound}, opened},
			wantCalls: 2,
			wantLogs:  []string{logNamespaceOpened},
		},
		{
			// The cluster accepted the release of the job, so the job is given as many
			// lookups to turn up as a just-started one before the wait gives up on it.
			name:        "fails when the job is never reported",
			wantErr:     errRestoreNotFound,
			wantCalls:   startedJobConfirmations,
			notWantLogs: []string{logNamespaceOpened},
		},
		{
			// The cluster keeps the state of the released job, so a job that stops
			// reporting cannot confirm the release.
			name: "fails when the job stops reporting",
			answers: append(
				[]restoreAnswer{opening},
				repeatRestoreNotFound(vanishedJobConfirmations)...,
			),
			wantErr:     errRestoreStatusLost,
			wantCalls:   1 + vanishedJobConfirmations,
			notWantLogs: []string{logNamespaceOpened},
		},
		{
			// A failed job keeps its state through the release, which only moves the
			// namespace state on, so the open waits for the namespace.
			name: "waits for the namespace of a failed job to open",
			answers: []restoreAnswer{
				{status: testFailedRestore(testColdStatus(infomodels.RestoreStateFailed, infomodels.NamespaceStateFailed))},
				{status: testFailedRestore(testColdStatus(infomodels.RestoreStateFailed, infomodels.NamespaceStateOpening))},
				{status: testFailedRestore(testColdStatus(infomodels.RestoreStateFailed, infomodels.NamespaceStateNone))},
			},
			wantCalls: 3,
			wantLogs: []string{
				logNamespaceOpened,
				// The wait shows where the namespace stands while it does not open.
				"state=" + stateClosed,
				"state=" + stateOpening,
			},
		},
		{
			// A node that was away keeps a FAILED row of the job, which says nothing
			// about a namespace that is open.
			name: "accepts an open namespace of a job one node reports as failed",
			answers: []restoreAnswer{
				{status: testColdStatus(infomodels.RestoreStateFailed, infomodels.NamespaceStateNone)},
			},
			wantCalls: 1,
			wantLogs:  []string{logNamespaceOpened},
		},
		{
			name: "accepts a namespace no node hosts",
			answers: []restoreAnswer{
				{status: testColdStatus(infomodels.RestoreStateAborted, infomodels.NamespaceStateStub)},
			},
			wantCalls: 1,
			wantLogs:  []string{logNamespaceOpened},
		},
		{
			name:        "returns as soon as the request is accepted when async",
			async:       true,
			answers:     []restoreAnswer{opening},
			wantCalls:   0,
			notWantLogs: []string{logNamespaceOpened},
		},
		{
			name:        "propagates a rejected request without polling",
			openErr:     errBoom,
			answers:     []restoreAnswer{opened},
			wantErr:     errBoom,
			wantCalls:   0,
			notWantLogs: []string{logNamespaceOpened},
		},
		{
			name:        "propagates an unexpected status error",
			answers:     []restoreAnswer{{err: errBoom}},
			wantErr:     errBoom,
			wantCalls:   1,
			notWantLogs: []string{logNamespaceOpened},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			svc, buf := newTestService(t)
			svc.restoreCfg.Open.Async = tt.async
			client := &fakeRestoreInfoClient{answers: tt.answers, openErr: tt.openErr}

			err := svc.openRestore(t.Context(), client)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}

			assert.Equal(t, 1, client.openCalls)
			assert.Equal(t, testNamespace, client.openNamespace)
			assert.Equal(t, testJobID, client.openJobID)
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

// TestServiceOpenRestoreDebugLines pins down that at the debug level the open names the
// state of the job under the same label in every line, the lines logged while the job is
// missing from the status for a moment included, and adds the full state.
func TestServiceOpenRestoreDebugLines(t *testing.T) {
	t.Parallel()

	const (
		logLastStateClosed = "last-state=" + stateClosed
		logDescription     = "description="
		logServerNsState   = "ns-state=FAILED"
	)

	svc, buf := newTestService(t)
	svc.logger = slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	closed := restoreAnswer{status: testFailedRestore(
		testColdStatus(infomodels.RestoreStateFailed, infomodels.NamespaceStateFailed))}
	client := &fakeRestoreInfoClient{answers: []restoreAnswer{
		closed,
		{err: asinfo.ErrNotFound},
		{status: testFailedRestore(testColdStatus(infomodels.RestoreStateFailed, infomodels.NamespaceStateNone))},
	}}

	require.NoError(t, svc.openRestore(t.Context(), client))

	for _, want := range []string{"state=" + stateClosed, logLastStateClosed, logDescription, logServerNsState} {
		assert.Contains(t, buf.String(), want)
	}
}
