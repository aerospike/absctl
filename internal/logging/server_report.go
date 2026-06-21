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
	"strings"

	sModels "github.com/aerospike/absctl/internal/server/models"
)

const segmentHeader = "Failed to validate segment"

// PrintServerValidationReport generates and logs or prints the server validation report based on the given parameters.
func PrintServerValidationReport(r *sModels.Report, toLog bool, logger *slog.Logger) {
	if toLog {
		logServerValidationReport(r, logger)
		return
	}

	printServerValidationReport(r)
}

func printServerValidationReport(r *sModels.Report) {
	printToOutWriter("")
	printToOutWriter(headerValidationReport)
	printToOutWriter(strings.Repeat("-", len(headerValidationReport)))

	printMetric("Backup ID", r.BackupID)
	printMetric("Total Segments", r.TotalSegments)
	printMetric("Checked Segments", r.CheckedSegments)
	printMetric("Verified by Metadata", r.VerifiedByMetadata)
	printMetric("Verified by Download", r.VerifiedByDownload)
	printMetric("Damaged Segments", len(r.Issues))
	printToOutWriter("")

	for _, issue := range r.Issues {
		printToOutWriter(segmentHeader)
		printToOutWriter(strings.Repeat("-", len(segmentHeader)))
		printMetric("Namespace", issue.Namespace)
		printMetric("Segment", issue.SegmentName)
		printMetric("Error", issue.Err)
		printMetric("Expected", issue.Expected)
		printMetric("Got", issue.Got)
	}
}

func logServerValidationReport(r *sModels.Report, logger *slog.Logger) {
	if len(r.Issues) == 0 {
		logger.Info("backup validation passed",
			slog.String("backup-id", r.BackupID),
			slog.Int("total-segments", r.TotalSegments),
			slog.Int("checked-segments", r.CheckedSegments),
			slog.Int("verified-by-metadata", r.VerifiedByMetadata),
			slog.Int("verified-by-download", r.VerifiedByDownload),
		)

		return
	}

	logger.Error("backup validation failed",
		slog.String("backup-id", r.BackupID),
		slog.Int("total-segments", r.TotalSegments),
		slog.Int("checked-segments", r.CheckedSegments),
		slog.Int("verified-by-metadata", r.VerifiedByMetadata),
		slog.Int("verified-by-download", r.VerifiedByDownload),
		slog.Int("damaged-segments", len(r.Issues)),
	)

	for _, issue := range r.Issues {
		attrs := []any{
			slog.String("namespace", issue.Namespace),
			slog.String("segment", issue.SegmentName),
			slog.String("expected", issue.Expected),
		}
		if issue.Err != nil {
			attrs = append(attrs, slog.String("error", issue.Err.Error()))
		} else {
			attrs = append(attrs, slog.String("got", issue.Got))
		}

		logger.Error("damaged segment", attrs...)
	}
}
