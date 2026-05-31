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
	"fmt"
	"io"
	"log/slog"
	"os"
	"text/tabwriter"
	"time"

	"github.com/aerospike/absctl/internal/models"
)

// PrintMetadata displays the node data in a formatted table.
func PrintMetadata(w io.Writer, data models.Metadata, toLog bool, logger *slog.Logger) {
	// count totals.
	var (
		totalRecords int64
		totalBytes   int64
		minCreated   time.Time
		maxFinished  time.Time
	)

	// Initialize with the values of the first node
	if len(data.Nodes) != 0 {
		minCreated = data.Nodes[0].Created
		maxFinished = data.Nodes[0].Finished
	}

	for _, node := range data.Nodes {
		totalRecords += node.RecordCount
		totalBytes += node.ByteCount

		if node.Created.Before(minCreated) {
			minCreated = node.Created
		}

		if node.Finished.After(maxFinished) {
			maxFinished = node.Finished
		}
	}

	if toLog {
		logger.Info("backup entry",
			slog.String("backupId", data.BackupID),
			slog.String("namespace", data.Namespace),
			slog.Int64("totalRecords", totalRecords),
			slog.Int64("totalBytes", totalBytes),
			slog.Time("created", minCreated),
			slog.Time("finished", maxFinished),
		)

		return
	}

	fmt.Fprintf(w, "%s\t%s\t%d\t%d\t%s\t%s\n",
		data.BackupID,
		data.Namespace,
		totalRecords,
		totalBytes,
		minCreated.Format("2006-01-02 15:04:05"),
		maxFinished.Format("2006-01-02 15:04:05"),
	)
}

// GetMetadataWriter returns a writer for the metadata table.
func GetMetadataWriter(toLog bool) io.Writer {
	if toLog {
		return nil
	}

	// tabwriter (with the header) is only needed for the stdout table.
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', tabwriter.Debug)
	fmt.Fprintln(w, "BACKUP ID\tNAMESPACE\tRECORDS\tBYTES\tCREATED\tFINISHED")

	return w
}

// CloseMetadataWriter flushes the metadata table.
func CloseMetadataWriter(w io.Writer) error {
	if w != nil {
		if err := w.(*tabwriter.Writer).Flush(); err != nil {
			return fmt.Errorf("failed to flush output: %w", err)
		}
	}

	return nil
}
