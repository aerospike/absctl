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
	"strconv"

	"github.com/aerospike/absctl/api"
)

const headerRestoreJobs = "Restore jobs"

// PrintRestoreJobs prints a listing of restore jobs keyed by job ID. When
// toLog is true the data is emitted as a structured log entry; otherwise a
// table is rendered to stderr.
func PrintRestoreJobs(jobs map[string]api.DtoRestoreJobStatus, toLog bool, logger *slog.Logger) {
	if toLog {
		logger.Info("restore jobs", slog.Any("jobs", jobs))
		return
	}

	printRestoreJobs(jobs)
}

func printRestoreJobs(jobs map[string]api.DtoRestoreJobStatus) {
	printSection(headerRestoreJobs)

	if len(jobs) == 0 {
		printToStderr("No restore jobs found.")
		return
	}

	tw := newTabWriter()
	writeRow(tw, "JOB ID", "STATUS", "READ", "INSERTED", "EXISTED", "FRESHER", "SKIPPED", "IGNORED", "ERROR")

	for _, id := range sortedKeys(jobs) {
		j := jobs[id]
		writeRow(tw,
			id,
			strVal((*string)(j.Status)),
			strconv.FormatInt(int64Val(j.ReadRecords), 10),
			strconv.FormatInt(int64Val(j.InsertedRecords), 10),
			strconv.FormatInt(int64Val(j.ExistedRecords), 10),
			strconv.FormatInt(int64Val(j.FresherRecords), 10),
			strconv.FormatInt(int64Val(j.SkippedRecords), 10),
			strconv.FormatInt(int64Val(j.IgnoredRecords), 10),
			strVal(j.Error),
		)
	}

	_ = tw.Flush()
}
