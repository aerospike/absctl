// Copyright 2024 Aerospike, Inc.
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

package logging

import (
	"log/slog"

	"github.com/aerospike/absctl/api"
)

const headerBackupState = "Backup state"

// PrintBackupState prints the backup routine state for the given routine.
// When toLog is true the data is emitted as a structured log entry through
// the provided logger; otherwise it is rendered as a human-readable section
// to stderr.
func PrintBackupState(state *api.DtoRoutineState, name string, toLog bool, logger *slog.Logger) {
	if toLog {
		logger.Info("backup state",
			slog.String("routine", name),
			slog.Any("state", state),
		)

		return
	}

	printBackupState(state, name)
}

func printBackupState(state *api.DtoRoutineState, name string) {
	printSection(headerBackupState)
	printMetric("Routine", name)

	if state == nil {
		printMetric("State", "n/a")
		return
	}

	printMetric("Last Full", strVal(state.LastFull))
	printMetric("Last Incremental", strVal(state.LastIncremental))
	printMetric("Next Full", strVal(state.NextFull))
	printMetric("Next Incremental", strVal(state.NextIncremental))

	if state.Full != nil {
		printSubSection("Full backup running")
		printRunningJob(state.Full)
	}

	if state.Incremental != nil {
		printSubSection("Incremental backup running")
		printRunningJob(state.Incremental)
	}
}

// printRunningJob prints the running-job details under an existing section.
func printRunningJob(job *api.DtoRunningJob) {
	printMetric("Start Time", strVal(job.StartTime))
	printMetric("Finish Time", strVal(job.FinishTime))
	printMetric("Estimated End", strVal(job.EstimatedEndTime))
	printMetric("Duration (s)", intVal(job.Duration))
	printMetric("Done Records", intVal(job.DoneRecords))
	printMetric("Total Records", intVal(job.TotalRecords))
	printMetric("Percentage Done", intVal(job.PercentageDone))

	if job.Metrics != nil {
		printMetric("Records/s", intVal(job.Metrics.RecordsPerSecond))
		printMetric("KB/s", intVal(job.Metrics.KilobytesPerSecond))
		printMetric("Pipeline", intVal(job.Metrics.Pipeline))
	}
}
