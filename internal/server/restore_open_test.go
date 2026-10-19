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
			name: "accepts a namespace no node hosts",
			answers: []restoreAnswer{
				{status: testColdStatus(infomodels.RestoreStateAborted, infomodels.NamespaceStateNotHosted)},
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

// TestServiceOpenRestoreTimesOut pins down the bound on the wait: a namespace that never
// opens has to end the command, and the error has to name the state it was left in.
func TestServiceOpenRestoreTimesOut(t *testing.T) {
	t.Parallel()

	svc, buf := newTestService(t)
	client := &fakeRestoreInfoClient{
		answers: []restoreAnswer{
			{status: testColdStatus(infomodels.RestoreStateAborted, infomodels.NamespaceStateOpening)},
		},
		repeatLast: true,
	}

	err := svc.openRestore(t.Context(), client)

	require.ErrorIs(t, err, errOpenStatusTimeout)
	require.ErrorContains(t, err, string(infomodels.NamespaceStateOpening))
	require.Greater(t, client.calls, 1, "the open must keep polling until the wait runs out")
	assert.NotContains(t, buf.String(), logNamespaceOpened)
}
