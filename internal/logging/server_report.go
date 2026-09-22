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

package logging

import (
	"fmt"
	"log/slog"
	"strings"

	sModels "github.com/aerospike/backup-go/pkg/server/segvalidator/models"
)

const (
	headerDryRunReport  = "Dry Run Validation Report"
	segmentParseHeader  = "Failed to parse segment"
	manifestIssueHeader = "Manifest does not match storage"
	unrecordedHeader    = "Segments not named by any manifest"
)

// PrintServerValidationReport logs or prints the dry-run validation report
// based on the given parameters.
func PrintServerValidationReport(r *sModels.ValidationReport, toLog bool, logger *slog.Logger) {
	if toLog {
		logServerValidationReport(r, logger)
		return
	}

	printServerValidationReport(r)
}

func printServerValidationReport(r *sModels.ValidationReport) {
	printToOutWriter("")
	printToOutWriter(headerDryRunReport)
	printToOutWriter(strings.Repeat("-", len(headerDryRunReport)))

	printMetric("Backup ID", r.BackupID)
	printMetric("Total Segments", r.TotalSegments)
	printMetric("Checked Segments", r.CheckedSegments)
	printMetric("Valid Segments", r.ValidSegments)
	printMetric("Unreadable Segments", r.InvalidSegments)
	printMetric("Records Read", r.TotalRecords)
	printMetric("Bytes Parsed", r.TotalBytes)
	printMetric("Skipped Compressed Records", r.SkippedCompressed)
	printToOutWriter("")

	printManifestMetrics(&r.Manifests)
	printUnrecordedSegments(&r.Manifests)

	// A run over a badly damaged backup reports more failures than it
	// describes, so say which of the two the list below is.
	if r.Truncated() {
		printToOutWriter(fmt.Sprintf("Describing the first %d of %d unreadable segments.",
			len(r.Issues), r.InvalidSegments))
		printToOutWriter("")
	}

	for _, issue := range r.Issues {
		printToOutWriter(segmentParseHeader)
		printToOutWriter(strings.Repeat("-", len(segmentParseHeader)))
		printMetric("Namespace", issue.Namespace)
		printMetric("Segment", issue.SegmentPath)
		printMetric("Error", issue.Err)

		if issue.RecordIndex != sModels.UnknownRecordIndex {
			printMetric("Record Index", issue.RecordIndex)
			printMetric("Record Offset", issue.Offset)
		}
	}

	printManifestIssues(&r.Manifests)
}

// printManifestMetrics reports what comparing manifests against the storage
// found. Only a manifest can tell that a segment is missing rather than merely
// unreadable, so its numbers are worth their own block.
func printManifestMetrics(m *sModels.ManifestReport) {
	printMetric("Total Manifests", m.Total)
	printMetric("Checked Manifests", m.Checked)
	printMetric("Checked Segment Records", m.CheckedSegments)
	printMetric("Missing Segments", m.MissingSegments)
	printMetric("Unrecorded Segments", m.Unrecorded)
	printMetric("Manifest Problems", m.Problems)
	printToOutWriter("")
}

// printUnrecordedSegments names the segments no manifest accounts for. A backup
// can hold them and still restore, so they are listed apart from the problems.
func printUnrecordedSegments(m *sModels.ManifestReport) {
	if len(m.UnrecordedExamples) == 0 {
		return
	}

	printToOutWriter(unrecordedHeader)
	printToOutWriter(strings.Repeat("-", len(unrecordedHeader)))

	if int64(len(m.UnrecordedExamples)) < m.Unrecorded {
		printToOutWriter(fmt.Sprintf("Naming the first %d of %d unrecorded segments.",
			len(m.UnrecordedExamples), m.Unrecorded))
	}

	for _, segment := range m.UnrecordedExamples {
		printMetric("Segment", segment)
	}

	printToOutWriter("")
}

func printManifestIssues(m *sModels.ManifestReport) {
	if m.Truncated() {
		printToOutWriter(fmt.Sprintf("Describing the first %d of %d manifest problems.",
			len(m.Issues), m.Problems))
		printToOutWriter("")
	}

	for _, issue := range m.Issues {
		printToOutWriter(manifestIssueHeader)
		printToOutWriter(strings.Repeat("-", len(manifestIssueHeader)))
		printMetric("Namespace", issue.Namespace)
		printMetric("Manifest", issue.ManifestPath)

		if issue.SegmentPath != "" {
			printMetric("Segment", issue.SegmentPath)
		}

		printMetric("Error", issue.Err)
	}
}

func logServerValidationReport(r *sModels.ValidationReport, logger *slog.Logger) {
	// The same counters go out whether the run passed or failed, so a log
	// reader does not have to know which outcome carries which field.
	attrs := []any{
		slog.String("backup-id", r.BackupID),
		slog.Int64("total-segments", r.TotalSegments),
		slog.Int64("checked-segments", r.CheckedSegments),
		slog.Int64("valid-segments", r.ValidSegments),
		slog.Int64("unreadable-segments", r.InvalidSegments),
		slog.Int64("records-read", r.TotalRecords),
		slog.Int64("bytes-parsed", r.TotalBytes),
		slog.Int64("skipped-compressed", r.SkippedCompressed),
		slog.Int64("total-manifests", r.Manifests.Total),
		slog.Int64("checked-manifests", r.Manifests.Checked),
		slog.Int64("checked-segment-records", r.Manifests.CheckedSegments),
		slog.Int64("missing-segments", r.Manifests.MissingSegments),
		slog.Int64("unrecorded-segments", r.Manifests.Unrecorded),
		slog.Int64("manifest-problems", r.Manifests.Problems),
	}

	if r.Failed() {
		logger.Error("backup dry run failed", attrs...)
	} else {
		logger.Info("backup dry run passed", attrs...)
	}

	logUnrecordedSegments(&r.Manifests, logger)

	if r.Truncated() {
		logger.Warn("unreadable segment list truncated",
			slog.Int("described-segments", len(r.Issues)),
			slog.Int64("unreadable-segments", r.InvalidSegments),
		)
	}

	for _, issue := range r.Issues {
		issueAttrs := []any{
			slog.String("namespace", issue.Namespace),
			slog.String("segment", issue.SegmentPath),
		}

		if issue.Err != nil {
			issueAttrs = append(issueAttrs, slog.String("error", issue.Err.Error()))
		}

		if issue.RecordIndex != sModels.UnknownRecordIndex {
			issueAttrs = append(issueAttrs,
				slog.Int("record-index", issue.RecordIndex),
				slog.Int("record-offset", issue.Offset),
			)
		}

		logger.Error("unreadable segment", issueAttrs...)
	}

	logManifestIssues(&r.Manifests, logger)
}

// logUnrecordedSegments names the segments no manifest accounts for. They do
// not fail the run, so they are reported even when everything else was clean.
func logUnrecordedSegments(m *sModels.ManifestReport, logger *slog.Logger) {
	if len(m.UnrecordedExamples) == 0 {
		return
	}

	logger.Warn("segments not named by any manifest",
		slog.Int("named-segments", len(m.UnrecordedExamples)),
		slog.Int64("unrecorded-segments", m.Unrecorded),
		slog.Any("segments", m.UnrecordedExamples),
	)
}

func logManifestIssues(m *sModels.ManifestReport, logger *slog.Logger) {
	if m.Truncated() {
		logger.Warn("manifest problem list truncated",
			slog.Int("described-problems", len(m.Issues)),
			slog.Int64("manifest-problems", m.Problems),
		)
	}

	for _, issue := range m.Issues {
		issueAttrs := []any{
			slog.String("namespace", issue.Namespace),
			slog.String("manifest", issue.ManifestPath),
		}

		if issue.SegmentPath != "" {
			issueAttrs = append(issueAttrs, slog.String("segment", issue.SegmentPath))
		}

		if issue.Err != nil {
			issueAttrs = append(issueAttrs, slog.String("error", issue.Err.Error()))
		}

		logger.Error("manifest does not match storage", issueAttrs...)
	}
}
