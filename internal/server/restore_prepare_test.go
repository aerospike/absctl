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

	"github.com/aerospike/absctl/internal/config"
	"github.com/aerospike/absctl/internal/models"
	"github.com/aerospike/backup-go/pkg/asinfo"
	infomodels "github.com/aerospike/backup-go/pkg/asinfo/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	logPreparationStarted = "restore preparation started"
	logPreparing          = "preparing namespace"
	// logClearStorageHint is a part of msgClearStorageRequired that the text handler
	// does not escape.
	logClearStorageHint = "must be cleared"
)

func TestServicePrepareRestore(t *testing.T) {
	t.Parallel()

	preparing := restoreAnswer{status: testClearStatus(infomodels.ClearStateNone, infomodels.NamespaceStatePreparing)}
	clearing := restoreAnswer{status: testClearStatus(infomodels.ClearStateClearing, infomodels.NamespaceStatePreparing)}
	required := restoreAnswer{status: testClearStatus(infomodels.ClearStateRequired, infomodels.NamespaceStatePreparing)}
	ready := restoreAnswer{status: testClearStatus(infomodels.ClearStateNone, infomodels.NamespaceStateReady)}

	tests := []struct {
		name             string
		async            bool
		unstable         bool
		prepareErr       error
		answers          []restoreAnswer
		wantErr          error
		wantCalls        int
		wantPrepareCalls int
		wantLogs         []string
		notWantLogs      []string
	}{
		{
			name:             "waits for the namespace to become ready",
			answers:          []restoreAnswer{preparing, preparing, ready},
			wantCalls:        3,
			wantPrepareCalls: 1,
			wantLogs:         []string{logPreparationStarted, logPreparing, msgNamespaceReady},
			notWantLogs:      []string{logClearStorageHint},
		},
		{
			name:             "accepts a namespace that is ready right away",
			answers:          []restoreAnswer{ready},
			wantCalls:        1,
			wantPrepareCalls: 1,
			wantLogs:         []string{msgNamespaceReady},
			notWantLogs:      []string{logPreparing, logClearStorageHint},
		},
		{
			// A node whose storage has to be cleared never gets ready on its own, so
			// the wait ends and says how to get past it.
			name:             "tells to clear the storage when a node asks for it",
			answers:          []restoreAnswer{preparing, required},
			wantCalls:        2,
			wantPrepareCalls: 1,
			wantLogs:         []string{logClearStorageHint},
			notWantLogs:      []string{msgNamespaceReady},
		},
		{
			// A clear that is already running leads to READY on its own.
			name:             "waits for a clear that is already running",
			answers:          []restoreAnswer{clearing, ready},
			wantCalls:        2,
			wantPrepareCalls: 1,
			wantLogs:         []string{logPreparing, msgNamespaceReady},
			notWantLogs:      []string{logClearStorageHint},
		},
		{
			// A single miss is a gap in the merged status, not a job that is gone.
			name:             "waits through a job missing from the status for a moment",
			answers:          []restoreAnswer{preparing, {err: asinfo.ErrNotFound}, ready},
			wantCalls:        3,
			wantPrepareCalls: 1,
			wantLogs:         []string{msgNamespaceReady},
		},
		{
			// The cluster accepted the preparation, so the job is given as many lookups
			// to turn up as a just-started one.
			name: "waits for the job to turn up in the status",
			answers: append(
				repeatRestoreNotFound(startedJobConfirmations-1),
				ready,
			),
			wantCalls:        startedJobConfirmations,
			wantPrepareCalls: 1,
			wantLogs:         []string{msgNamespaceReady},
		},
		{
			name:             "fails when the job is never reported",
			wantErr:          errRestoreNotFound,
			wantCalls:        startedJobConfirmations,
			wantPrepareCalls: 1,
			notWantLogs:      []string{msgNamespaceReady},
		},
		{
			name: "fails when the job stops reporting",
			answers: append(
				[]restoreAnswer{preparing},
				repeatRestoreNotFound(vanishedJobConfirmations)...,
			),
			wantErr:          errRestoreStatusLost,
			wantCalls:        1 + vanishedJobConfirmations,
			wantPrepareCalls: 1,
			notWantLogs:      []string{msgNamespaceReady},
		},
		{
			name: "fails when the preparation fails",
			answers: []restoreAnswer{
				{status: testClearStatus(infomodels.ClearStateNone, infomodels.NamespaceStateFailed)},
			},
			wantErr:          errPrepareFailed,
			wantCalls:        1,
			wantPrepareCalls: 1,
			notWantLogs:      []string{msgNamespaceReady},
		},
		{
			name: "fails when the job is aborted",
			answers: []restoreAnswer{
				preparing,
				{status: testColdStatus(infomodels.RestoreStateAborted, infomodels.NamespaceStatePreparing)},
			},
			wantErr:          errRestoreNotPreparing,
			wantCalls:        2,
			wantPrepareCalls: 1,
			notWantLogs:      []string{msgNamespaceReady},
		},
		{
			name:             "propagates an unexpected status error",
			answers:          []restoreAnswer{{err: errBoom}},
			wantErr:          errBoom,
			wantCalls:        1,
			wantPrepareCalls: 1,
			notWantLogs:      []string{msgNamespaceReady},
		},
		{
			name:             "returns as soon as the request is accepted when async",
			async:            true,
			answers:          []restoreAnswer{preparing},
			wantCalls:        0,
			wantPrepareCalls: 1,
			wantLogs:         []string{logPreparationStarted},
			notWantLogs:      []string{msgNamespaceReady},
		},
		{
			name:             "propagates a rejected request without polling",
			prepareErr:       errBoom,
			answers:          []restoreAnswer{ready},
			wantErr:          errBoom,
			wantCalls:        0,
			wantPrepareCalls: 1,
			notWantLogs:      []string{logPreparationStarted, msgNamespaceReady},
		},
		{
			// A namespace with migrations in flight cannot be prepared, so nothing is
			// sent to the cluster.
			name:             "does not prepare an unstable namespace",
			unstable:         true,
			answers:          []restoreAnswer{ready},
			wantErr:          errClusterNotStable,
			wantCalls:        0,
			wantPrepareCalls: 0,
			notWantLogs:      []string{logPreparationStarted},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			svc, buf := newTestService(t)
			svc.restoreCfg.Prepare.Async = tt.async
			client := &fakeRestoreInfoClient{
				answers:    tt.answers,
				prepareErr: tt.prepareErr,
				unstable:   tt.unstable,
			}

			err := svc.prepareRestore(t.Context(), client)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}

			assert.Equal(t, tt.wantCalls, client.calls)
			assert.Equal(t, tt.wantPrepareCalls, client.prepareCalls)

			if tt.wantPrepareCalls > 0 {
				require.NotNil(t, client.prepareRequest)
				assert.Equal(t, testNamespace, client.prepareRequest.Namespace)
				assert.Equal(t, testJobID, client.prepareRequest.JobID)
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

func TestServicePrepareRestoreRequest(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name               string
		giveHydrateReplica bool
	}{
		{name: "all replicas", giveHydrateReplica: true},
		{name: "master only", giveHydrateReplica: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			svc := &Service{restoreCfg: &config.ServerRestoreServiceConfig{
				Prepare: &models.ServerRestorePrepare{
					Namespace:      testRequestNamespace,
					JobID:          testRequestJobID,
					HydrateReplica: tt.giveHydrateReplica,
				},
			}}

			assert.Equal(t, &infomodels.RequestPrepareRestore{
				Namespace:      testRequestNamespace,
				JobID:          testRequestJobID,
				HydrateReplica: new(tt.giveHydrateReplica),
			}, svc.prepareRestoreRequest())
		})
	}
}
