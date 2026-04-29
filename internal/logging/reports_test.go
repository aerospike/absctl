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
	"log/slog"
	"strings"
	"testing"
	"time"

	bModels "github.com/aerospike/backup-go/models"
	"github.com/stretchr/testify/assert"
)

func TestIndent(t *testing.T) {
	tests := []struct {
		name     string
		key      string
		expected string
	}{
		{
			name:     "Empty key",
			key:      "",
			expected: ":" + strings.Repeat(" ", 21),
		},
		{
			name:     "Short key",
			key:      "Key",
			expected: "Key:" + strings.Repeat(" ", 18),
		},
		{
			name:     "Long key",
			key:      "ThisIsAVeryLongKey",
			expected: "ThisIsAVeryLongKey:" + strings.Repeat(" ", 3),
		},
		{
			name:     "Exact 20 character key",
			key:      "12345678901234567890",
			expected: "12345678901234567890:" + strings.Repeat(" ", 1),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := indent(tt.key)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestPrintMetric(t *testing.T) {
	output := captureOutput(t, func() {
		printMetric("TestKey", "TestValue")
		printMetric("IntKey", 123)
		printMetric("FloatKey", 123.456)
	})

	assert.Contains(t, output, "TestKey:"+strings.Repeat(" ", 21-len("TestKey"))+"TestValue")
	assert.Contains(t, output, "IntKey:"+strings.Repeat(" ", 21-len("IntKey"))+"123")
	assert.Contains(t, output, "FloatKey:"+strings.Repeat(" ", 21-len("FloatKey"))+"123.456")
}

// newSampleBackupStats returns BackupStats populated with the same numbers
// every backup-report test relies on. Centralising it keeps the test file
// short and the assertions easy to follow.
func newSampleBackupStats() *bModels.BackupStats {
	stats := bModels.NewBackupStats()
	stats.StartTime = time.Now().Add(-1 * time.Hour)
	stats.ReadRecords.Add(1000)
	stats.AddSIndexes(5)
	stats.AddUDFs(3)
	stats.BytesWritten.Add(5000000)
	stats.TotalRecords.Add(1000)

	for range 10 {
		stats.IncFiles()
	}

	return stats
}

// newSampleRestoreStats mirrors newSampleBackupStats for restore reports.
func newSampleRestoreStats() *bModels.RestoreStats {
	stats := bModels.NewRestoreStats()
	stats.StartTime = time.Now().Add(-1 * time.Hour)
	stats.ReadRecords.Add(1000)
	stats.AddSIndexes(5)
	stats.AddUDFs(3)
	stats.RecordsExpired.Add(10)
	stats.RecordsSkipped.Add(20)
	stats.RecordsIgnored.Add(30)

	for range 40 {
		stats.IncrRecordsFresher()
	}

	for range 50 {
		stats.IncrRecordsExisted()
	}

	for range 60 {
		stats.IncrRecordsInserted()
	}

	for range 5 {
		stats.IncrErrorsInDoubt()
	}

	stats.TotalBytesRead.Add(5000000)

	return stats
}

func TestPrintBackupReport(t *testing.T) {
	stats := newSampleBackupStats()

	output := captureOutput(t, func() {
		printBackupReport(stats, false)
	})

	assert.Contains(t, output, headerBackupReport)
	assert.Contains(t, output, "Start Time")
	assert.Contains(t, output, stats.StartTime.Format(time.RFC1123))
	assert.Contains(t, output, "Duration")
	assert.Contains(t, output, "Records Read")
	assert.Contains(t, output, "1000")
	assert.Contains(t, output, "sIndex Read")
	assert.Contains(t, output, "5")
	assert.Contains(t, output, "UDFs Read")
	assert.Contains(t, output, "3")
	assert.Contains(t, output, "Bytes Written")
	assert.Contains(t, output, "5000000")
	assert.Contains(t, output, "Files Written")
	assert.Contains(t, output, "10")
}

func TestPrintBackupReportXdr(t *testing.T) {
	stats := newSampleBackupStats()

	output := captureOutput(t, func() {
		printBackupReport(stats, true)
	})

	assert.Contains(t, output, headerBackupReport)
	assert.Contains(t, output, "Records Received")
	assert.NotContains(t, output, "Records Read")
}

func TestLogBackupReport(t *testing.T) {
	stats := newSampleBackupStats()

	var buf bytes.Buffer

	logger := slog.New(slog.NewTextHandler(&buf, nil))
	logBackupReport(stats, false, logger)

	logOutput := buf.String()
	assert.Contains(t, logOutput, "backup report")
	assert.Contains(t, logOutput, "start_time=")
	assert.Contains(t, logOutput, "duration=")
	assert.Contains(t, logOutput, "records_read=1000")
	assert.Contains(t, logOutput, "s_index_read=5")
	assert.Contains(t, logOutput, "udf_read=3")
	assert.Contains(t, logOutput, "bytes_written=5000000")
	assert.Contains(t, logOutput, "files_written=10")
}

func TestLogBackupReportXdr(t *testing.T) {
	stats := newSampleBackupStats()

	var buf bytes.Buffer

	logger := slog.New(slog.NewTextHandler(&buf, nil))
	logBackupReport(stats, true, logger)

	logOutput := buf.String()
	assert.Contains(t, logOutput, "backup report")
	assert.Contains(t, logOutput, "records_received=1000")
	assert.NotContains(t, logOutput, "records_read=")
}

func TestReportBackup(t *testing.T) {
	stats := newSampleBackupStats()

	t.Run("Console output", func(t *testing.T) {
		output := captureOutput(t, func() {
			ReportBackup(stats, false, false, nil)
		})

		assert.Contains(t, output, headerBackupReport)
		assert.Contains(t, output, "Records Read")
	})

	t.Run("JSON output", func(t *testing.T) {
		var buf bytes.Buffer

		logger := slog.New(slog.NewTextHandler(&buf, nil))
		ReportBackup(stats, false, true, logger)

		logOutput := buf.String()
		assert.Contains(t, logOutput, "backup report")
		assert.Contains(t, logOutput, "records_read=1000")
	})
}

func TestPrintRestoreReport(t *testing.T) {
	stats := newSampleRestoreStats()

	output := captureOutput(t, func() {
		printRestoreReport(stats, false)
	})

	assert.Contains(t, output, headerRestoreReport)
	assert.Contains(t, output, "Start Time")
	assert.Contains(t, output, stats.StartTime.Format(time.RFC1123))
	assert.Contains(t, output, "Duration")
	assert.Contains(t, output, "Records Read")
	assert.Contains(t, output, "1000")
	assert.Contains(t, output, "sIndex Read")
	assert.Contains(t, output, "5")
	assert.Contains(t, output, "UDFs Read")
	assert.Contains(t, output, "3")
	assert.Contains(t, output, "Expired Records")
	assert.Contains(t, output, "10")
	assert.Contains(t, output, "Skipped Records")
	assert.Contains(t, output, "20")
	assert.Contains(t, output, "Ignored Records")
	assert.Contains(t, output, "30")
	assert.Contains(t, output, "Fresher Records")
	assert.Contains(t, output, "40")
	assert.Contains(t, output, "Existed Records")
	assert.Contains(t, output, "50")
	assert.Contains(t, output, "Inserted Records")
	assert.Contains(t, output, "60")
	assert.Contains(t, output, "In Doubt Errors")
	assert.Contains(t, output, "5")
	assert.Contains(t, output, "Total Bytes Read")
	assert.Contains(t, output, "5000000")
}

func TestLogRestoreReport(t *testing.T) {
	stats := newSampleRestoreStats()

	var buf bytes.Buffer

	logger := slog.New(slog.NewTextHandler(&buf, nil))
	logRestoreReport(stats, logger, false)

	logOutput := buf.String()
	assert.Contains(t, logOutput, "restore report")
	assert.Contains(t, logOutput, "start_time=")
	assert.Contains(t, logOutput, "duration=")
	assert.Contains(t, logOutput, "records_read=1000")
	assert.Contains(t, logOutput, "s_index_read=5")
	assert.Contains(t, logOutput, "udf_read=3")
	assert.Contains(t, logOutput, "expired_records=10")
	assert.Contains(t, logOutput, "skipped_records=20")
	assert.Contains(t, logOutput, "ignored_records=30")
	assert.Contains(t, logOutput, "fresher_records=40")
	assert.Contains(t, logOutput, "existed_records=50")
	assert.Contains(t, logOutput, "inserted_records=60")
	assert.Contains(t, logOutput, "in_doubt_errors=5")
	assert.Contains(t, logOutput, "total_bytes_read=5000000")
}

func TestReportRestore(t *testing.T) {
	stats := newSampleRestoreStats()

	t.Run("Console output", func(t *testing.T) {
		output := captureOutput(t, func() {
			ReportRestore(stats, false, false, nil)
		})

		assert.Contains(t, output, headerRestoreReport)
		assert.Contains(t, output, "Records Read")
	})

	t.Run("JSON output", func(t *testing.T) {
		var buf bytes.Buffer

		logger := slog.New(slog.NewTextHandler(&buf, nil))
		ReportRestore(stats, false, true, logger)

		logOutput := buf.String()
		assert.Contains(t, logOutput, "restore report")
		assert.Contains(t, logOutput, "records_read=1000")
	})
}

func TestPrintEstimateReport(t *testing.T) {
	output := captureOutput(t, func() {
		printEstimateReport(5000000)
	})

	assert.Contains(t, output, headerEstimateReport)
	assert.Contains(t, output, "File size (bytes)")
	assert.Contains(t, output, "5000000")
}

func TestLogEstimateReport(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	logEstimateReport(5000000, logger)

	logOutput := buf.String()
	assert.Contains(t, logOutput, "estimate report")
	assert.Contains(t, logOutput, "file_size_bytes=5000000")
}

func TestReportEstimate(t *testing.T) {
	t.Run("Console output", func(t *testing.T) {
		output := captureOutput(t, func() {
			ReportEstimate(5000000, false, nil)
		})

		assert.Contains(t, output, headerEstimateReport)
		assert.Contains(t, output, "File size (bytes)")
		assert.Contains(t, output, "5000000")
	})

	t.Run("JSON output", func(t *testing.T) {
		var buf bytes.Buffer

		logger := slog.New(slog.NewTextHandler(&buf, nil))
		ReportEstimate(5000000, true, logger)

		logOutput := buf.String()
		assert.Contains(t, logOutput, "estimate report")
		assert.Contains(t, logOutput, "file_size_bytes=5000000")
	})
}
