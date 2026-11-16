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

	infomodels "github.com/aerospike/backup-go/pkg/asinfo/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBackupStateLabel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		give infomodels.BackupState
		want string
	}{
		{give: infomodels.BackupStateInit, want: "initializing"},
		{give: infomodels.BackupStateBaseScanActive, want: "base-scan"},
		{give: infomodels.BackupStateBaseScanDone, want: "base-scan-done"},
		{give: infomodels.BackupStateIncrScanActive, want: "incremental-scan"},
		{give: infomodels.BackupStateStoppingChangeStream, want: "stopping-change-stream"},
		{give: infomodels.BackupStateFinalDraining, want: "draining"},
		{give: infomodels.BackupStateCommitting, want: "committing"},
		{give: infomodels.BackupStateComplete, want: stateComplete},
		{give: infomodels.BackupStateFailed, want: stateFailed},
		{give: infomodels.BackupStateAborting, want: stateAborting},
		{give: infomodels.BackupStateAborted, want: stateAborted},
		// A state this version does not know is labeled after its server name.
		{give: "NEW_STATE", want: "new-state"},
	}

	for _, tt := range tests {
		t.Run(string(tt.give), func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, backupStateLabel(tt.give))
			// The description explains the state, where the label only names it.
			assert.NotEqual(t, tt.want, backupStateDescription(tt.give))
		})
	}
}

func TestRestoreStateLabel(t *testing.T) {
	t.Parallel()

	clearRequired := testClearStatus(infomodels.ClearStateRequired, infomodels.NamespaceStatePreparing)

	tests := []struct {
		name            string
		give            *infomodels.ResponseRestoreState
		want            string
		wantDescription string
	}{
		{
			name:            "cold restore before the gate is up",
			give:            testColdStatus(infomodels.RestoreStateActive, infomodels.NamespaceStateNone),
			want:            "starting",
			wantDescription: "starting the restore",
		},
		{
			name:            "cold restore preparing",
			give:            testColdStatus(infomodels.RestoreStateActive, infomodels.NamespaceStatePreparing),
			want:            "preparing",
			wantDescription: "checking its storage",
		},
		{
			name:            "cold restore waiting for a clear",
			give:            clearRequired,
			want:            "waiting-for-clear",
			wantDescription: "storage to be cleared",
		},
		{
			name:            "cold restore clearing its storage",
			give:            testClearStatus(infomodels.ClearStateClearing, infomodels.NamespaceStatePreparing),
			want:            stateClearing,
			wantDescription: "clearing the namespace storage",
		},
		{
			// The job runs on until the principal aborts it, so it is not failed yet.
			name:            "cold restore whose preparation failed",
			give:            testColdStatus(infomodels.RestoreStateActive, infomodels.NamespaceStateFailed),
			want:            statePreparationFailed,
			wantDescription: "the job is stopping",
		},
		{
			name:            "cold restore prepared",
			give:            testColdStatus(infomodels.RestoreStateActive, infomodels.NamespaceStateReady),
			want:            "ready",
			wantDescription: "prepared on every node",
		},
		{
			name:            "cold restore hydrating",
			give:            testColdStatus(infomodels.RestoreStateActive, infomodels.NamespaceStateRestoring),
			want:            stateRestoring,
			wantDescription: "hydrating the partitions",
		},
		{
			name:            "cold restore releasing the namespace",
			give:            testColdStatus(infomodels.RestoreStateActive, infomodels.NamespaceStateOpening),
			want:            "opening",
			wantDescription: "releasing the namespace",
		},
		{
			name:            "fuzzy restore",
			give:            testFuzzyStatus(infomodels.RestoreStateActive),
			want:            stateRestoring,
			wantDescription: "live namespace",
		},
		{
			name:            "complete",
			give:            testColdStatus(infomodels.RestoreStateComplete, infomodels.NamespaceStateNone),
			want:            stateComplete,
			wantDescription: "restore complete",
		},
		{
			name:            "failed",
			give:            testColdStatus(infomodels.RestoreStateFailed, infomodels.NamespaceStateFailed),
			want:            stateFailed,
			wantDescription: "restore failed",
		},
		{
			name:            "aborting",
			give:            testFuzzyStatus(infomodels.RestoreStateAborting),
			want:            stateAborting,
			wantDescription: "aborting the restore",
		},
		{
			name:            "aborted",
			give:            testFuzzyStatus(infomodels.RestoreStateAborted),
			want:            stateAborted,
			wantDescription: "restore aborted",
		},
		{
			// A state this version does not know is labeled after its server name.
			name:            "unknown job state",
			give:            testFuzzyStatus("NEW_STATE"),
			want:            "new-state",
			wantDescription: "restore state new-state",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, restoreStateLabel(tt.give))
			assert.Contains(t, restoreStateDescription(tt.give), tt.wantDescription)
		})
	}
}

func TestOpenStateLabel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		give            *infomodels.ResponseRestoreState
		want            string
		wantDescription string
	}{
		{
			// The job keeps its failed state through the release, so the namespace
			// state is what the label tells.
			name:            "namespace still closed",
			give:            testColdStatus(infomodels.RestoreStateFailed, infomodels.NamespaceStateFailed),
			want:            stateClosed,
			wantDescription: "closed to clients",
		},
		{
			name:            "namespace being released",
			give:            testColdStatus(infomodels.RestoreStateAborted, infomodels.NamespaceStateOpening),
			want:            stateOpening,
			wantDescription: "releasing the namespace",
		},
		{
			name:            "any other state",
			give:            testColdStatus(infomodels.RestoreStateActive, infomodels.NamespaceStateRestoring),
			want:            stateRestoring,
			wantDescription: "hydrating the partitions",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, openStateLabel(tt.give))
			assert.Contains(t, openStateDescription(tt.give), tt.wantDescription)
		})
	}
}

// TestRestoreStateDescriptions makes sure every label a restore can be logged under has a
// description of its own, rather than the fallback for a state this version does not know.
func TestRestoreStateDescriptions(t *testing.T) {
	t.Parallel()

	labels := []string{
		stateStarting, statePreparing, stateWaitingForClear, stateClearing, statePreparationFailed,
		stateReady, stateOpening, stateComplete, stateFailed, stateAborting, stateAborted, stateUnknown,
	}

	for _, label := range labels {
		t.Run(label, func(t *testing.T) {
			t.Parallel()

			description, ok := restoreStateDescriptions[label]
			require.True(t, ok)
			assert.NotEqual(t, label, description)
		})
	}
}
