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
	"io"
	"log/slog"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/aerospike/backup-go/pkg/server/lister/models"
)

const (
	// citrusleafEpochMs is the start of the Citrusleaf epoch (2010-01-01T00:00:00Z) in
	// Unix milliseconds. The server writes checkpoints relative to it.
	citrusleafEpochMs int64 = 1262304000000

	// metadataTimeLayout matches the ISO8601 form the server writes for the modified
	// bounds, so every time in a listing reads the same.
	metadataTimeLayout = "2006-01-02T15:04:05.000Z"

	// emptyCell stands in for a field the manifest does not carry.
	emptyCell = "-"
)

// cellReplacer flattens the characters that would end a table cell or row early. Free
// text such as a fallback reason is written by the server and may hold any of them.
var cellReplacer = strings.NewReplacer("\t", " ", "\n", " ", "\r", " ")

// PrintMetadata displays backup metadata either as a formatted table
// (to stdout) or as structured log records.
func PrintMetadata(data []*models.Metadata, toLog bool, logger *slog.Logger) error {
	if toLog {
		for i := range data {
			printMetadataToLog(data[i], logger)
		}

		return nil
	}

	return printMetadataToTable(os.Stdout, data)
}

func printMetadataToLog(md *models.Metadata, logger *slog.Logger) {
	// Every entry carries the same keys, so a log reader does not have to know which
	// backup kind omits which field.
	attrs := []any{
		slog.String("backup-id", md.BackupID),
		slog.String("namespace", md.Namespace),
		slog.String("sets", md.Sets),
		slog.String("status", string(md.Status)),
		slog.String("backup-type", string(md.BackupType)),
		slog.String("backup-policy", string(md.BackupPolicy)),
		slog.String("achieved-consistency", string(md.AchievedConsistency)),
		slog.String("checkpoint", checkpointTime(md.CheckpointMs)),
		slog.String("modified-after", md.ModifiedAfter),
		slog.String("modified-before", md.ModifiedBefore),
		slog.String("fallback-reason", md.FallbackReason),
	}

	logger.Info("backup entry", attrs...)
}

func printMetadataToTable(out io.Writer, data []*models.Metadata) error {
	return printMetadataSummary(out, data)
}

func printMetadataSummary(out io.Writer, data []*models.Metadata) error {
	w := tabwriter.NewWriter(out, 0, 0, 3, ' ', tabwriter.Debug)

	// The fallback reason is free text of any length, so it goes last where it does
	// not push the other columns apart.
	fmt.Fprintln(w, "BACKUP ID\tNAMESPACE\tSETS\tSTATUS\tTYPE\tPOLICY\tCONSISTENCY\t"+
		"CHECKPOINT\tMODIFIED AFTER\tMODIFIED BEFORE\tFALLBACK REASON")

	for i := range data {
		md := data[i]

		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			md.BackupID,
			md.Namespace,
			cell(md.Sets),
			cell(string(md.Status)),
			cell(string(md.BackupType)),
			cell(string(md.BackupPolicy)),
			cell(string(md.AchievedConsistency)),
			cell(checkpointTime(md.CheckpointMs)),
			cell(md.ModifiedAfter),
			cell(md.ModifiedBefore),
			cell(md.FallbackReason),
		)
	}

	if err := w.Flush(); err != nil {
		return fmt.Errorf("failed to flush output: %w", err)
	}

	return nil
}

// checkpointTime renders the strict checkpoint, the cluster-wide restore cut, as UTC
// wall-clock time. A zero checkpoint means the backup has no cut (fuzzy, aborted or
// written by an older build) and renders as nothing.
func checkpointTime(ms int64) string {
	if ms == 0 {
		return ""
	}

	return time.UnixMilli(citrusleafEpochMs + ms).UTC().Format(metadataTimeLayout)
}

// cell keeps an absent field visible in the table, where an empty cell would read as
// a misaligned column, and keeps a present one on a single line of its own column.
func cell(v string) string {
	if v == "" {
		return emptyCell
	}

	return cellReplacer.Replace(v)
}
