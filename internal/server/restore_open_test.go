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

	// closed is the job the check before the release finds: a failed cold restore that
	// keeps the namespace closed.
	closed := restoreAnswer{status: testFailedRestore(
		testColdStatus(infomodels.RestoreStateFailed, infomodels.NamespaceStateFailed))}
	opening := restoreAnswer{status: testColdStatus(infomodels.RestoreStateAborted, infomodels.NamespaceStateOpening)}
	opened := restoreAnswer{status: testColdStatus(infomodels.RestoreStateAborted, infomodels.NamespaceStateNone)}

	tests := []struct {
		name          string
		async         bool
		openErr       error
		answers       []restoreAnswer
		wantErr       error
		wantOpenCalls int
		wantCalls     int
		wantLogs      []string
		notWantLogs   []string
	}{
		{
			name:          "waits for the namespace to open",
			answers:       []restoreAnswer{closed, opening, opening, opened},
			wantOpenCalls: 1,
			wantCalls:     4,
			wantLogs:      []string{logNamespaceOpened},
		},
		{
			// The job id is not sent, so a job the cluster does not report for the
			// namespace is refused before the namespace is released.
			name:        "refuses a job the cluster does not report before the release",
			wantErr:     errRestoreNotFound,
			wantCalls:   1,
			notWantLogs: []string{msgOpeningNamespace, logNamespaceOpened},
		},
		{
			name:        "propagates a status error before the release",
			answers:     []restoreAnswer{{err: errBoom}},
			wantErr:     errBoom,
			wantCalls:   1,
			notWantLogs: []string{msgOpeningNamespace},
		},
		{
			// A job missing from the status cannot confirm the release, so the wait goes
			// on rather than announcing an open namespace.
			name:          "waits through a job missing from the status",
			answers:       []restoreAnswer{closed, {err: asinfo.ErrNotFound}, opened},
			wantOpenCalls: 1,
			wantCalls:     3,
			wantLogs:      []string{logNamespaceOpened},
		},
		{
			// The check before the release has seen the job, so a job the wait never
			// finds has left the status, and is waited for as a reported job is.
			name:          "reports a job lost when the wait never finds it",
			answers:       []restoreAnswer{closed},
			wantErr:       errRestoreStatusLost,
			wantOpenCalls: 1,
			wantCalls:     1 + vanishedJobConfirmations,
			notWantLogs:   []string{logNamespaceOpened},
		},
		{
			// The cluster keeps the state of the released job, so a job that stops
			// reporting cannot confirm the release.
			name: "fails when the job stops reporting",
			answers: append(
				[]restoreAnswer{closed, opening},
				repeatRestoreNotFound(vanishedJobConfirmations)...,
			),
			wantErr:       errRestoreStatusLost,
			wantOpenCalls: 1,
			wantCalls:     2 + vanishedJobConfirmations,
			notWantLogs:   []string{logNamespaceOpened},
		},
		{
			// A failed job keeps its state through the release, which only moves the
			// namespace state on, so the open waits for the namespace.
			name: "waits for the namespace of a failed job to open",
			answers: []restoreAnswer{
				closed,
				closed,
				{status: testFailedRestore(testColdStatus(infomodels.RestoreStateFailed, infomodels.NamespaceStateOpening))},
				{status: testFailedRestore(testColdStatus(infomodels.RestoreStateFailed, infomodels.NamespaceStateNone))},
			},
			wantOpenCalls: 1,
			wantCalls:     4,
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
				closed,
				{status: testColdStatus(infomodels.RestoreStateFailed, infomodels.NamespaceStateNone)},
			},
			wantOpenCalls: 1,
			wantCalls:     2,
			wantLogs:      []string{logNamespaceOpened},
		},
		{
			name: "accepts a namespace no node hosts",
			answers: []restoreAnswer{
				closed,
				{status: testColdStatus(infomodels.RestoreStateAborted, infomodels.NamespaceStateStub)},
			},
			wantOpenCalls: 1,
			wantCalls:     2,
			wantLogs:      []string{logNamespaceOpened},
		},
		{
			// The job is still checked before the release, but the release is not
			// followed.
			name:          "returns as soon as the request is accepted when async",
			async:         true,
			answers:       []restoreAnswer{closed, opening},
			wantOpenCalls: 1,
			wantCalls:     1,
			notWantLogs:   []string{logNamespaceOpened},
		},
		{
			name:          "propagates a rejected request without following it",
			openErr:       errBoom,
			answers:       []restoreAnswer{closed, opened},
			wantErr:       errBoom,
			wantOpenCalls: 1,
			wantCalls:     1,
			notWantLogs:   []string{logNamespaceOpened},
		},
		{
			name:          "propagates an unexpected status error after the release",
			answers:       []restoreAnswer{closed, {err: errBoom}},
			wantErr:       errBoom,
			wantOpenCalls: 1,
			wantCalls:     2,
			notWantLogs:   []string{logNamespaceOpened},
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

			assert.Equal(t, tt.wantOpenCalls, client.openCalls)
			assert.Equal(t, tt.wantCalls, client.calls)

			if tt.wantOpenCalls > 0 {
				assert.Equal(t, testNamespace, client.openNamespace)
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

// TestServiceOpenRestoreWithoutJobID covers an open given no job id. The status of a
// restore is read by its job id, so the release is not followed, which the command says
// unless it was asked to return right away.
func TestServiceOpenRestoreWithoutJobID(t *testing.T) {
	t.Parallel()

	const (
		logOpenNotFollowed = "pass --job-id"
		logNamespaceAttr   = "namespace=" + testNamespace
	)

	tests := []struct {
		name        string
		giveAsync   bool
		wantLogs    []string
		notWantLogs []string
	}{
		{
			name:        "says the release is not followed",
			wantLogs:    []string{logOpenNotFollowed, logNamespaceAttr},
			notWantLogs: []string{logNamespaceOpened},
		},
		{
			name:        "returns quietly when async",
			giveAsync:   true,
			wantLogs:    []string{msgOpeningNamespace, logNamespaceAttr},
			notWantLogs: []string{logOpenNotFollowed, logNamespaceOpened},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			svc, buf := newTestService(t)
			svc.restoreCfg.Open.JobID = ""
			svc.restoreCfg.Open.Async = tt.giveAsync
			client := &fakeRestoreInfoClient{}

			require.NoError(t, svc.openRestore(t.Context(), client))

			assert.Equal(t, 1, client.openCalls)
			assert.Equal(t, testNamespace, client.openNamespace)
			// The status is never read.
			assert.Zero(t, client.calls)

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
		// The first answer is read by the check before the release.
		closed,
		closed,
		{err: asinfo.ErrNotFound},
		{status: testFailedRestore(testColdStatus(infomodels.RestoreStateFailed, infomodels.NamespaceStateNone))},
	}}

	require.NoError(t, svc.openRestore(t.Context(), client))

	for _, want := range []string{"state=" + stateClosed, logLastStateClosed, logDescription, logServerNsState} {
		assert.Contains(t, buf.String(), want)
	}
}
