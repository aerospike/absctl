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
	"context"
	"log/slog"
	"strings"

	infomodels "github.com/aerospike/backup-go/pkg/asinfo/models"
)

// The short labels a job state is logged under. They stay close to the server states,
// so that a label tells what the server is doing, and are spelled with dashes, so that
// the log prints them without quotes. The full description of a state goes to the debug
// level, see backupStateDescription and restoreStateDescription.
const (
	stateComplete = "complete"
	stateFailed   = "failed"
	stateAborting = "aborting"
	stateAborted  = "aborted"
	stateUnknown  = "unknown"

	stateStarting          = "starting"
	statePreparing         = "preparing"
	stateWaitingForClear   = "waiting-for-clear"
	stateClearing          = "clearing"
	statePreparationFailed = "preparation-failed"
	stateReady             = "ready"
	stateRestoring         = "restoring"
	stateOpening           = "opening"
	// stateClosed is the label of a namespace a failed or aborted cold restore keeps
	// closed to clients until it is released.
	stateClosed = "closed"
)

// backupStateLabels are the labels of the backup states.
var backupStateLabels = map[infomodels.BackupState]string{
	infomodels.BackupStateInit:                 "initializing",
	infomodels.BackupStateBaseScanActive:       "base-scan",
	infomodels.BackupStateBaseScanDone:         "base-scan-done",
	infomodels.BackupStateIncrScanActive:       "incremental-scan",
	infomodels.BackupStateStoppingChangeStream: "stopping-change-stream",
	infomodels.BackupStateFinalDraining:        "draining",
	infomodels.BackupStateCommitting:           "committing",
	infomodels.BackupStateComplete:             stateComplete,
	infomodels.BackupStateFailed:               stateFailed,
	infomodels.BackupStateAborting:             stateAborting,
	infomodels.BackupStateAborted:              stateAborted,
	infomodels.BackupStateUnknown:              stateUnknown,
}

// backupStateLabel returns the label of a backup state. A state this version does not
// know is labeled after its server name.
func backupStateLabel(state infomodels.BackupState) string {
	if label, ok := backupStateLabels[state]; ok {
		return label
	}

	return serverStateLabel(string(state))
}

// backupStateDescription returns the full description of a backup state.
func backupStateDescription(state infomodels.BackupState) string {
	return state.Describe()
}

// restoreStateLabels are the labels of the restore states that tell the phase of the job
// on their own. An active cold restore is labeled after its namespace state instead, see
// restoreStateLabel.
var restoreStateLabels = map[infomodels.RestoreState]string{
	infomodels.RestoreStateComplete: stateComplete,
	infomodels.RestoreStateFailed:   stateFailed,
	infomodels.RestoreStateAborting: stateAborting,
	infomodels.RestoreStateAborted:  stateAborted,
	infomodels.RestoreStateUnknown:  stateUnknown,
}

// restoreStateLabel returns the label of the state of a restore job. The server reports
// an ongoing job as ACTIVE from start to end, and only the namespace state of a cold
// restore tells its phases apart, so an active cold restore is labeled after it.
func restoreStateLabel(status *infomodels.ResponseRestoreState) string {
	if label, ok := restoreStateLabels[status.State]; ok {
		return label
	}

	if status.State != infomodels.RestoreStateActive {
		return serverStateLabel(string(status.State))
	}

	if status.Kind != infomodels.RestoreKindCold {
		return stateRestoring
	}

	switch status.NsState {
	case infomodels.NamespaceStatePreparing:
		return preparingLabel(status.Clear)
	case infomodels.NamespaceStateFailed:
		// A node failed its preparation, and the job stops once the principal aborts it.
		return statePreparationFailed
	case infomodels.NamespaceStateReady:
		return stateReady
	case infomodels.NamespaceStateRestoring:
		return stateRestoring
	case infomodels.NamespaceStateOpening:
		return stateOpening
	case infomodels.NamespaceStateNone, infomodels.NamespaceStateStub, "":
		// The gate is not up yet on any node.
		return stateStarting
	default:
		return serverStateLabel(string(status.NsState))
	}
}

// preparingLabel returns the label of a cold restore that prepares its namespace, which
// the clear state of its nodes tells apart.
func preparingLabel(clearState infomodels.ClearState) string {
	switch clearState {
	case infomodels.ClearStateRequired:
		return stateWaitingForClear
	case infomodels.ClearStateClearing:
		return stateClearing
	default:
		return statePreparing
	}
}

// restoreStateDescriptions are the full descriptions of the restore labels.
var restoreStateDescriptions = map[string]string{
	stateStarting:          "starting the restore",
	statePreparing:         "closing the namespace to clients and checking its storage on every node",
	stateWaitingForClear:   "waiting for the namespace storage to be cleared on some nodes",
	stateClearing:          "clearing the namespace storage on some nodes",
	statePreparationFailed: "preparing the namespace failed on some nodes, the job is stopping",
	stateReady:             "namespace prepared on every node, the restore starts once all of them are ready",
	stateOpening:           "partitions restored, releasing the namespace to clients",
	stateComplete:          "restore complete",
	stateFailed:            "restore failed",
	stateAborting:          "aborting the restore",
	stateAborted:           "restore aborted",
	stateUnknown:           "unknown restore state",
}

// restoreStateDescription returns the full description of the state of a restore job.
func restoreStateDescription(status *infomodels.ResponseRestoreState) string {
	label := restoreStateLabel(status)

	if label == stateRestoring {
		if status.Kind == infomodels.RestoreKindCold {
			return "hydrating the partitions of the closed namespace from the backup"
		}

		return "writing the backup records into the live namespace"
	}

	if description, ok := restoreStateDescriptions[label]; ok {
		return description
	}

	return "restore state " + label
}

// serverStateLabel returns the label of a server state this version has no label for:
// its name in lower case, with dashes.
func serverStateLabel(state string) string {
	return strings.ToLower(strings.ReplaceAll(state, "_", "-"))
}

// openStateLabel returns the label of a restore job whose namespace is being released.
// The job keeps its failed or aborted state through the release, so only the namespace
// state tells how far the release got.
func openStateLabel(status *infomodels.ResponseRestoreState) string {
	switch status.NsState {
	case infomodels.NamespaceStateFailed:
		return stateClosed
	case infomodels.NamespaceStateOpening:
		return stateOpening
	default:
		return restoreStateLabel(status)
	}
}

// openStateDescription returns the full description of the state openStateLabel labels.
func openStateDescription(status *infomodels.ResponseRestoreState) string {
	switch openStateLabel(status) {
	case stateClosed:
		return "namespace closed to clients by the failed or aborted restore"
	case stateOpening:
		// The job state of a namespace being released says nothing about the release.
		return restoreStateDescriptions[stateOpening]
	default:
		return restoreStateDescription(status)
	}
}

// restoreDebugAttrs returns the attributes that describe the state of a restore job in
// full, under its label: description, the server state and, for a cold restore, the
// namespace and clear states the label is made from.
func restoreDebugAttrs(status *infomodels.ResponseRestoreState, description string) []any {
	attrs := []any{
		slog.String("description", description),
		slog.String("server-state", string(status.State)),
	}

	// Only a cold restore gates the namespace and clears its storage.
	if status.Kind == infomodels.RestoreKindCold {
		attrs = append(attrs,
			slog.String("ns-state", string(status.NsState)),
			slog.String("clear", string(status.Clear)))
	}

	return attrs
}

// withDebugAttrs returns attrs followed by the attributes debugAttrs builds when logger
// prints debug records, and attrs alone otherwise, so that the full state ends the line
// at the debug level and is not built at the others.
func withDebugAttrs(ctx context.Context, logger *slog.Logger, attrs []any, debugAttrs func() []any) []any {
	if debugAttrs == nil || !logger.Enabled(ctx, slog.LevelDebug) {
		return attrs
	}

	return append(attrs, debugAttrs()...)
}
