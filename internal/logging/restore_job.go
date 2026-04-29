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

const headerRestoreStatus = "Restore status"

// PrintRestoreStatus prints a single restore job status. When toLog is true the
// data is emitted as a structured log entry; otherwise it is rendered as a
// human-readable section to stderr.
func PrintRestoreStatus(status *api.DtoRestoreJobStatus, jobID int64, toLog bool, logger *slog.Logger) {
	if toLog {
		logger.Info("restore status",
			slog.Int64("jobId", jobID),
			slog.Any("status", status),
		)

		return
	}

	printRestoreStatus(status, jobID)
}

func printRestoreStatus(status *api.DtoRestoreJobStatus, jobID int64) {
	printSection(headerRestoreStatus)
	printMetric("Job ID", strconv.FormatInt(jobID, 10))

	if status == nil {
		printMetric("Status", "n/a")
		return
	}

	printMetric("Status", strVal((*string)(status.Status)))
	printMetric("Read Records", int64Val(status.ReadRecords))
	printMetric("Inserted Records", int64Val(status.InsertedRecords))
	printMetric("Existed Records", int64Val(status.ExistedRecords))
	printMetric("Expired Records", int64Val(status.ExpiredRecords))
	printMetric("Fresher Records", int64Val(status.FresherRecords))
	printMetric("Skipped Records", int64Val(status.SkippedRecords))
	printMetric("Ignored Records", int64Val(status.IgnoredRecords))
	printMetric("Index Count", int64Val(status.IndexCount))
	printMetric("UDF Count", int64Val(status.UdfCount))
	printMetric("Total Bytes", int64Val(status.TotalBytes))
	printMetric("Errors In Doubt", int64Val(status.ErrorsInDoubt))
	printMetric("Error", strVal(status.Error))

	if status.CurrentJob != nil {
		printSubSection("Current job")
		printRunningJob(status.CurrentJob)
	}
}
