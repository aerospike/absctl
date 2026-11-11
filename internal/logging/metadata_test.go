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
	"bytes"
	"log/slog"
	"strings"
	"testing"

	"github.com/aerospike/backup-go/pkg/server/lister/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testListBackupID      = "519118324"
	testListNamespace     = "source-ns1"
	testListSets          = "set1,set2"
	testListModifiedAfter = "2026-10-01T00:00:00.000Z"
	testListFallback      = "node bb9a1b2c3d4e5f6 failed - timeout"
	// testListRawFallback is a fallback reason that would break the table as it is.
	testListRawFallback = "node bb9a1b2c3d4e5f6 failed |\tsee\r\nlog"
	// testListFlatFallback is testListRawFallback as the table shows it.
	testListFlatFallback = "node bb9a1b2c3d4e5f6 failed | see  log"

	// testCheckpointMs is testCheckpointTime in Citrusleaf epoch milliseconds.
	testCheckpointMs   int64 = 530627696789
	testCheckpointTime       = "2026-10-25T12:34:56.789Z"
)

func TestCheckpointTime(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		want string
		give int64
	}{
		{name: "no checkpoint", give: 0, want: ""},
		{name: "citrusleaf epoch start", give: 1, want: "2010-01-01T00:00:00.001Z"},
		{name: "strict checkpoint", give: testCheckpointMs, want: testCheckpointTime},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, checkpointTime(tt.give))
		})
	}
}

func TestPrintMetadataSummary(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		give      *models.Metadata
		wantCells []string
	}{
		{
			name: "strict incremental backup",
			give: &models.Metadata{
				BackupID:            testListBackupID,
				Namespace:           testListNamespace,
				Sets:                testListSets,
				Status:              models.BackupStatusComplete,
				CheckpointMs:        testCheckpointMs,
				BackupType:          models.BackupTypeIncremental,
				ModifiedAfter:       testListModifiedAfter,
				BackupPolicy:        models.BackupPolicyStrict,
				AchievedConsistency: models.AchievedConsistencyStrict,
			},
			wantCells: []string{
				testListBackupID, testListNamespace, testListSets,
				string(models.BackupStatusComplete), string(models.BackupTypeIncremental),
				string(models.BackupPolicyStrict), string(models.AchievedConsistencyStrict),
				testCheckpointTime, testListModifiedAfter, emptyCell, emptyCell,
			},
		},
		{
			name: "best-effort backup fell back to fuzzy",
			give: &models.Metadata{
				BackupID:            testListBackupID,
				Namespace:           testListNamespace,
				Status:              models.BackupStatusComplete,
				BackupType:          models.BackupTypeFull,
				BackupPolicy:        models.BackupPolicyBestEffort,
				AchievedConsistency: models.AchievedConsistencyFuzzy,
				FallbackReason:      testListFallback,
			},
			wantCells: []string{
				testListBackupID, testListNamespace, emptyCell,
				string(models.BackupStatusComplete), string(models.BackupTypeFull),
				string(models.BackupPolicyBestEffort), string(models.AchievedConsistencyFuzzy),
				emptyCell, emptyCell, emptyCell, testListFallback,
			},
		},
		{
			// The server writes the reason as free text, so it is kept to one cell.
			name: "fallback reason with separators",
			give: &models.Metadata{
				BackupID:       testListBackupID,
				Namespace:      testListNamespace,
				Status:         models.BackupStatusComplete,
				FallbackReason: testListRawFallback,
			},
			wantCells: []string{
				testListBackupID, testListNamespace, emptyCell,
				string(models.BackupStatusComplete), emptyCell, emptyCell, emptyCell,
				emptyCell, emptyCell, emptyCell, testListFlatFallback,
			},
		},
		{
			// Manifests from older builds carry none of the newer fields.
			name: "legacy manifest",
			give: &models.Metadata{
				BackupID:  testListBackupID,
				Namespace: testListNamespace,
				Status:    models.BackupStatusComplete,
			},
			wantCells: []string{
				testListBackupID, testListNamespace, emptyCell,
				string(models.BackupStatusComplete), emptyCell, emptyCell, emptyCell,
				emptyCell, emptyCell, emptyCell, emptyCell,
			},
		},
	}

	// Each value must sit under its own label, so the header is pinned down too.
	wantHeader := []string{
		"BACKUP ID", "NAMESPACE", "SETS", "STATUS", "TYPE", "POLICY", "CONSISTENCY",
		"CHECKPOINT", "MODIFIED AFTER", "MODIFIED BEFORE", "FALLBACK REASON",
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var buf bytes.Buffer

			require.NoError(t, printMetadataSummary(&buf, []*models.Metadata{tt.give}))

			lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
			require.Len(t, lines, 2)

			assert.Equal(t, wantHeader, tableCells(lines[0], lines[0]))
			assert.Equal(t, tt.wantCells, tableCells(lines[0], lines[1]))
		})
	}
}

func TestPrintMetadataToLog(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer

	logger := slog.New(slog.NewTextHandler(&buf, nil))

	printMetadataToLog(&models.Metadata{
		BackupID:            testListBackupID,
		Namespace:           testListNamespace,
		Status:              models.BackupStatusComplete,
		CheckpointMs:        testCheckpointMs,
		BackupType:          models.BackupTypeFull,
		BackupPolicy:        models.BackupPolicyBestEffort,
		AchievedConsistency: models.AchievedConsistencyStrict,
	}, logger)

	out := buf.String()
	for _, want := range []string{
		"backup-id=" + testListBackupID,
		"namespace=" + testListNamespace,
		`sets=""`,
		"status=complete",
		"backup-type=full",
		"backup-policy=best-effort",
		"achieved-consistency=strict",
		"checkpoint=" + testCheckpointTime,
		`modified-after=""`,
		`modified-before=""`,
		`fallback-reason=""`,
	} {
		assert.Contains(t, out, want)
	}
}

// tableCells splits a tabwriter.Debug row into its trimmed cells. The row is cut where
// the header has its separators, as tabwriter aligns every row to them, so a "|" inside
// a value does not split it.
func tableCells(header, line string) []string {
	cells := make([]string, 0, strings.Count(header, "|")+1)
	start := 0

	for i := range len(header) {
		if header[i] != '|' {
			continue
		}

		cells = append(cells, strings.TrimSpace(line[start:i]))
		start = i + 1
	}

	return append(cells, strings.TrimSpace(line[start:]))
}
