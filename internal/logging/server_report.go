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
	refusalHeader       = "Restore would refuse the backup"

	resultPassed = "passed"
	resultFailed = "failed"

	restoreStrict = "strict"
	restoreFuzzy  = "fuzzy"
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

	printMetric("Result", validationResult(r))
	printMetric("Backup ID", r.BackupID)
	printMetric("Total Segments", r.TotalSegments)
	printMetric("Checked Segments", r.CheckedSegments)
	printMetric("Valid Segments", r.ValidSegments)
	printMetric("Unreadable Segments", r.InvalidSegments)
	printMetric("Records Read", r.TotalRecords)
	printMetric("Bytes Parsed", r.TotalBytes)
	printToOutWriter("")

	printCommitMetrics(&r.Commit)
	printManifestMetrics(r)
	printUnrecordedSegments(&r.Manifests)
	printCommitRefusal(&r.Commit)

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

// printCommitMetrics reports what the commit record of the backup says and
// which restore the backup was validated for.
func printCommitMetrics(c *sModels.CommitReport) {
	printMetric("Commit Record", c.Path)
	printMetric("Commit State", c.State)
	printMetric("Backup Status", c.Status)
	printMetric("Backup Policy", c.BackupPolicy)
	printMetric("Achieved Consistency", c.AchievedConsistency)
	printMetric("Fallback Reason", c.FallbackReason)
	printMetric("Backup Namespace", c.Namespace)
	printMetric("Backup Type", c.BackupType)
	printMetric("Validated For Restore", validatedRestore(c))
	printMetric("Change Stream Skipped", c.ChangeStreamSkipped)
	printToOutWriter("")
}

// printManifestMetrics reports what comparing manifests against the storage
// found. Only a manifest can tell that a segment is missing rather than merely
// unreadable, so its numbers are worth their own block.
func printManifestMetrics(r *sModels.ValidationReport) {
	m := &r.Manifests

	printMetric("Total Manifests", m.Total)
	printMetric("Checked Manifests", m.Checked)
	printMetric("Superseded Manifests", m.Superseded)
	printMetric("Checked Segment Records", m.CheckedSegments)
	printMetric("Missing Segments", m.MissingSegments)
	printMetric("Unrecorded Segments", m.Unrecorded)
	printMetric("Manifest Problems", m.Problems)
	printMetric("Empty Namespaces", strings.Join(r.EmptyNamespaces, ", "))
	printToOutWriter("")
}

// printCommitRefusal says why a restore would refuse to start from the backup,
// which fails it even when every segment reads back.
func printCommitRefusal(c *sModels.CommitReport) {
	if c.Refusal == nil {
		return
	}

	printToOutWriter(refusalHeader)
	printToOutWriter(strings.Repeat("-", len(refusalHeader)))
	printMetric("Commit Record", c.Path)
	printMetric("Error", c.Refusal)
	printToOutWriter("")
}

// validationResult is the verdict of the run in the words both renderers use.
func validationResult(r *sModels.ValidationReport) string {
	if r.Failed() {
		return resultFailed
	}

	return resultPassed
}

// validatedRestore names the restore the backup was validated for, or nothing
// when the commit record was not read and so decided nothing.
func validatedRestore(c *sModels.CommitReport) string {
	switch {
	case c.State == "":
		return ""
	case c.Strict:
		return restoreStrict
	default:
		return restoreFuzzy
	}
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
		slog.String("commit-record", r.Commit.Path),
		slog.String("commit-state", r.Commit.State),
		slog.String("backup-status", r.Commit.Status),
		slog.String("backup-policy", r.Commit.BackupPolicy),
		slog.String("achieved-consistency", r.Commit.AchievedConsistency),
		slog.String("fallback-reason", r.Commit.FallbackReason),
		slog.String("backup-namespace", r.Commit.Namespace),
		slog.String("backup-type", r.Commit.BackupType),
		slog.String("validated-for-restore", validatedRestore(&r.Commit)),
		slog.Bool("change-stream-skipped", r.Commit.ChangeStreamSkipped),
		slog.Int64("total-manifests", r.Manifests.Total),
		slog.Int64("checked-manifests", r.Manifests.Checked),
		slog.Int64("superseded-manifests", r.Manifests.Superseded),
		slog.Int64("checked-segment-records", r.Manifests.CheckedSegments),
		slog.Int64("missing-segments", r.Manifests.MissingSegments),
		slog.Int64("unrecorded-segments", r.Manifests.Unrecorded),
		slog.Int64("manifest-problems", r.Manifests.Problems),
		slog.Any("empty-namespaces", r.EmptyNamespaces),
	}

	// The validator already logs a refusal as it finds it, so the reason rides on the
	// summary line instead of a line of its own.
	if r.Commit.Refusal != nil {
		attrs = append(attrs, slog.String("restore-refusal", r.Commit.Refusal.Error()))
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
