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
	"log/slog"
	"os"
	"text/tabwriter"
	"time"

	"github.com/aerospike/backup-go/pkg/server/models"
)

type metaDataStats struct {
	totalRecords int64
	totalBytes   int64
	minCreated   time.Time
	maxFinished  time.Time
}

// PrintMetadata displays the node data in a formatted table.
func PrintMetadata(data []models.Metadata, toLog bool, logger *slog.Logger) error {
	var w *tabwriter.Writer
	if !toLog {
		w = tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', tabwriter.Debug)
	}

	for _, md := range data {
		stats := calcMetadataStats(md)
		if toLog {
			printMetadataToLog(md, stats, logger)
			continue
		}

		printMetadataToTable(md, stats, w)
	}

	if w != nil {
		if err := w.Flush(); err != nil {
			return fmt.Errorf("failed to flush output: %w", err)
		}
	}

	return nil
}

func printMetadataToLog(data models.Metadata, stats metaDataStats, logger *slog.Logger) {
	logger.Info("backup entry",
		slog.String("backup-id", data.BackupID),
		slog.String("namespace", data.Namespace),
		slog.Int64("total-records", stats.totalRecords),
		slog.Int64("total-bytes", stats.totalBytes),
		slog.Time("created", stats.minCreated),
		slog.Time("finished", stats.maxFinished),
	)
}

func printMetadataToTable(data models.Metadata, stats metaDataStats, w *tabwriter.Writer) {
	fmt.Fprintf(w, "%s\t%s\t%d\t%d\t%s\t%s\n",
		data.BackupID,
		data.Namespace,
		stats.totalRecords,
		stats.totalBytes,
		stats.minCreated.Format("2006-01-02 15:04:05"),
		stats.maxFinished.Format("2006-01-02 15:04:05"),
	)
}

func calcMetadataStats(data models.Metadata) metaDataStats {
	// count totals.
	var stats metaDataStats

	// Initialize with the values of the first node
	if len(data.Nodes) != 0 {
		stats.minCreated = data.Nodes[0].Created
		stats.maxFinished = data.Nodes[0].Finished
	}

	for _, node := range data.Nodes {
		stats.totalRecords += node.RecordCount
		stats.totalBytes += node.ByteCount

		if node.Created.Before(stats.minCreated) {
			stats.minCreated = node.Created
		}

		if node.Finished.After(stats.maxFinished) {
			stats.maxFinished = node.Finished
		}
	}

	return stats
}
