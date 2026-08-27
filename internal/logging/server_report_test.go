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
	"bytes"
	"errors"
	"log/slog"
	"testing"

	sModels "github.com/aerospike/backup-go/pkg/server/segvalidator/models"
	"github.com/stretchr/testify/assert"
)

var errSegmentUnreadable = errors.New("bad record magic")

// newSampleValidationReport builds a report of a backup that validated clean.
func newSampleValidationReport() *sModels.ValidationReport {
	return &sModels.ValidationReport{
		BackupID:          "519118324",
		TotalSegments:     1000,
		CheckedSegments:   100,
		ValidSegments:     100,
		TotalRecords:      4200,
		TotalBytes:        5000000,
		SkippedCompressed: 7,
	}
}

func TestPrintServerValidationReport(t *testing.T) {
	r := newSampleValidationReport()

	output := captureOutput(t, func() {
		printServerValidationReport(r)
	})

	assert.Contains(t, output, headerDryRunReport)
	assert.Contains(t, output, "Backup ID")
	assert.Contains(t, output, "519118324")
	assert.Contains(t, output, "Total Segments")
	assert.Contains(t, output, "1000")
	assert.Contains(t, output, "Checked Segments")
	assert.Contains(t, output, "Valid Segments")
	assert.Contains(t, output, "Unreadable Segments")
	assert.Contains(t, output, "Records Read")
	assert.Contains(t, output, "4200")
	assert.Contains(t, output, "Bytes Parsed")
	assert.Contains(t, output, "5000000")
	assert.Contains(t, output, "Skipped Compressed Records")
	assert.NotContains(t, output, segmentParseHeader)
}

func TestPrintServerValidationReportWithIssues(t *testing.T) {
	r := newSampleValidationReport()
	r.ValidSegments = 98
	r.InvalidSegments = 2
	r.Issues = []sModels.ValidationIssue{
		{
			Err:         errSegmentUnreadable,
			Namespace:   "source-ns1",
			SegmentPath: "519118324/segment/ns/source-ns1/s0.seg",
			RecordIndex: 3,
			Offset:      192,
		},
		{
			Err:         errSegmentUnreadable,
			Namespace:   "source-ns1",
			SegmentPath: "519118324/segment/ns/source-ns1/s1.seg",
			RecordIndex: sModels.UnknownRecordIndex,
		},
	}

	output := captureOutput(t, func() {
		printServerValidationReport(r)
	})

	assert.Contains(t, output, segmentParseHeader)
	assert.Contains(t, output, "source-ns1")
	assert.Contains(t, output, "519118324/segment/ns/source-ns1/s0.seg")
	assert.Contains(t, output, errSegmentUnreadable.Error())

	// The record position is shown for the issue that has one.
	assert.Contains(t, output, "Record Index")
	assert.Contains(t, output, "192")

	// A whole report is printed, so nothing hints at a truncated list.
	assert.NotContains(t, output, "Describing the first")
}

func TestPrintServerValidationReportTruncated(t *testing.T) {
	r := newSampleValidationReport()
	r.ValidSegments = 0
	r.InvalidSegments = 5000
	r.Issues = []sModels.ValidationIssue{
		{
			Err:         errSegmentUnreadable,
			Namespace:   "source-ns1",
			SegmentPath: "519118324/segment/ns/source-ns1/s0.seg",
			RecordIndex: sModels.UnknownRecordIndex,
		},
	}

	output := captureOutput(t, func() {
		printServerValidationReport(r)
	})

	// The count of failures must not be mistaken for the length of the list.
	assert.Contains(t, output, "Describing the first 1 of 5000 unreadable segments.")
	assert.Contains(t, output, "5000")
}

func TestLogServerValidationReport(t *testing.T) {
	var buf bytes.Buffer

	logger := slog.New(slog.NewTextHandler(&buf, nil))
	logServerValidationReport(newSampleValidationReport(), logger)

	logOutput := buf.String()
	assert.Contains(t, logOutput, "backup dry run passed")
	assert.Contains(t, logOutput, "backup-id=519118324")
	assert.Contains(t, logOutput, "total-segments=1000")
	assert.Contains(t, logOutput, "checked-segments=100")
	assert.Contains(t, logOutput, "valid-segments=100")
	assert.Contains(t, logOutput, "records-read=4200")
	assert.Contains(t, logOutput, "bytes-parsed=5000000")
	assert.Contains(t, logOutput, "skipped-compressed=7")
	assert.NotContains(t, logOutput, "unreadable segment")
}

func TestLogServerValidationReportWithIssues(t *testing.T) {
	r := newSampleValidationReport()
	r.ValidSegments = 99
	r.InvalidSegments = 1
	r.Issues = []sModels.ValidationIssue{
		{
			Err:         errSegmentUnreadable,
			Namespace:   "source-ns1",
			SegmentPath: "519118324/segment/ns/source-ns1/s0.seg",
			RecordIndex: 3,
			Offset:      192,
		},
	}

	var buf bytes.Buffer

	logger := slog.New(slog.NewTextHandler(&buf, nil))
	logServerValidationReport(r, logger)

	logOutput := buf.String()
	assert.Contains(t, logOutput, "backup dry run failed")
	assert.Contains(t, logOutput, "unreadable-segments=1")
	assert.Contains(t, logOutput, "unreadable segment")
	assert.Contains(t, logOutput, "namespace=source-ns1")
	assert.Contains(t, logOutput, "record-index=3")
	assert.Contains(t, logOutput, "record-offset=192")
	assert.NotContains(t, logOutput, "truncated")
}

func TestLogServerValidationReportTruncated(t *testing.T) {
	r := newSampleValidationReport()
	r.ValidSegments = 0
	r.InvalidSegments = 5000
	r.Issues = []sModels.ValidationIssue{
		{
			Err:         errSegmentUnreadable,
			Namespace:   "source-ns1",
			SegmentPath: "519118324/segment/ns/source-ns1/s0.seg",
			RecordIndex: sModels.UnknownRecordIndex,
		},
	}

	var buf bytes.Buffer

	logger := slog.New(slog.NewTextHandler(&buf, nil))
	logServerValidationReport(r, logger)

	logOutput := buf.String()
	assert.Contains(t, logOutput, "unreadable segment list truncated")
	assert.Contains(t, logOutput, "described-segments=1")
	assert.Contains(t, logOutput, "unreadable-segments=5000")

	// An issue without a record position must not claim one.
	assert.NotContains(t, logOutput, "record-index=")
}

func TestPrintServerValidationReportRouting(t *testing.T) {
	r := newSampleValidationReport()

	var buf bytes.Buffer

	logger := slog.New(slog.NewTextHandler(&buf, nil))

	output := captureOutput(t, func() {
		PrintServerValidationReport(r, true, logger)
	})

	assert.Empty(t, output, "logging must not write to the printer output")
	assert.Contains(t, buf.String(), "backup dry run passed")

	buf.Reset()

	output = captureOutput(t, func() {
		PrintServerValidationReport(r, false, logger)
	})

	assert.Contains(t, output, headerDryRunReport)
	assert.Empty(t, buf.String(), "printing must not write to the log")
}

// withManifestProblem adds a manifest issue about a missing segment.
func withManifestProblem(r *sModels.ValidationReport) *sModels.ValidationReport {
	r.Manifests = sModels.ManifestReport{
		Total:           500,
		Checked:         4,
		CheckedSegments: 40,
		MissingSegments: 1,
		Problems:        1,
		Issues: []sModels.ManifestIssue{
			{
				Err:          errSegmentUnreadable,
				Namespace:    "source-ns1",
				ManifestPath: "519118324/ns/source-ns1/query-stream/p1/manifest_0.json",
				SegmentPath:  "519118324/ns/source-ns1/query-stream/p1/s9.seg",
			},
		},
	}

	return r
}

func TestPrintServerValidationReportManifests(t *testing.T) {
	r := withManifestProblem(newSampleValidationReport())

	output := captureOutput(t, func() {
		printServerValidationReport(r)
	})

	assert.Contains(t, output, "Total Manifests")
	assert.Contains(t, output, "Checked Manifests")
	assert.Contains(t, output, "Checked Segment Records")
	assert.Contains(t, output, "Missing Segments")
	assert.Contains(t, output, manifestIssueHeader)
	assert.Contains(t, output, "519118324/ns/source-ns1/query-stream/p1/manifest_0.json")
	assert.Contains(t, output, "519118324/ns/source-ns1/query-stream/p1/s9.seg")
}

func TestPrintServerValidationReportManifestsTruncated(t *testing.T) {
	r := withManifestProblem(newSampleValidationReport())
	r.Manifests.Problems = 900

	output := captureOutput(t, func() {
		printServerValidationReport(r)
	})

	assert.Contains(t, output, "Describing the first 1 of 900 manifest problems.")
}

func TestLogServerValidationReportManifests(t *testing.T) {
	r := withManifestProblem(newSampleValidationReport())

	var buf bytes.Buffer

	logger := slog.New(slog.NewTextHandler(&buf, nil))
	logServerValidationReport(r, logger)

	logOutput := buf.String()

	// A missing segment is a failed backup even though every segment that does
	// exist parsed cleanly.
	assert.Contains(t, logOutput, "backup dry run failed")
	assert.Contains(t, logOutput, "manifest-problems=1")
	assert.Contains(t, logOutput, "total-manifests=500")
	assert.Contains(t, logOutput, "checked-manifests=4")
	assert.Contains(t, logOutput, "checked-segment-records=40")
	assert.Contains(t, logOutput, "missing-segments=1")
	assert.Contains(t, logOutput, "manifest does not match storage")
}
