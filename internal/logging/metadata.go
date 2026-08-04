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
	"cmp"
	"fmt"
	"io"
	"log/slog"
	"os"
	"slices"
	"text/tabwriter"
	"time"

	"github.com/aerospike/backup-go/pkg/server/models"
)

const timeFormat = "2006-01-02 15:04:05"

type metaDataStats struct {
	totalRecords int64
	totalBytes   int64
	minCreated   time.Time
	maxFinished  time.Time
}

// PrintMetadata displays backup metadata either as a formatted table
// (to stdout) or as structured log records. When withNodes is true,
// per-node details are included as well.
func PrintMetadata(data []models.Metadata, withNodes, toLog bool, logger *slog.Logger) error {
	if toLog {
		for i := range data {
			printMetadataToLog(data[i], withNodes, logger)
		}

		return nil
	}

	return printMetadataToTable(os.Stdout, data, withNodes)
}

func printMetadataToLog(md models.Metadata, withNodes bool, logger *slog.Logger) {
	stats := calcMetadataStats(md)

	attrs := []any{
		slog.String("backup-id", md.BackupID),
		slog.String("namespace", md.Namespace),
		slog.Int64("total-records", stats.totalRecords),
		slog.Int64("total-bytes", stats.totalBytes),
		slog.Time("created", stats.minCreated),
		slog.Time("finished", stats.maxFinished),
	}

	if withNodes {
		// slog.Any renders md.Nodes as a JSON array (with json tags) for
		// the JSON handler and as a readable value for the text handler.
		attrs = append(attrs, slog.Any("nodes", md.Nodes))
	}

	logger.Info("backup entry", attrs...)
}

func printMetadataToTable(out io.Writer, data []models.Metadata, withNodes bool) error {
	if withNodes {
		return printMetadataDetailed(out, data)
	}

	return printMetadataSummary(out, data)
}

func printMetadataSummary(out io.Writer, data []models.Metadata) error {
	fmt.Println("start printMetadataSummary")

	w := tabwriter.NewWriter(out, 0, 0, 3, ' ', tabwriter.Debug)

	fmt.Fprintln(w, "BACKUP ID\tNAMESPACE\tRECORDS\tBYTES\tCREATED\tFINISHED")

	for i := range data {
		md := data[i]
		stats := calcMetadataStats(md)

		fmt.Fprintf(w, "%s\t%s\t%d\t%d\t%s\t%s\n",
			md.BackupID,
			md.Namespace,
			stats.totalRecords,
			stats.totalBytes,
			stats.minCreated.Format(timeFormat),
			stats.maxFinished.Format(timeFormat),
		)
	}

	fmt.Println("try to flush printMetadataSummary")

	if err := w.Flush(); err != nil {
		return fmt.Errorf("failed to flush output: %w", err)
	}

	fmt.Println("finished printMetadataSummary")

	return nil
}

func printMetadataDetailed(out io.Writer, data []models.Metadata) error {
	fmt.Println("start printMetadataDetailed")

	for i := range data {
		md := data[i]
		stats := calcMetadataStats(md)

		fmt.Fprintf(out,
			"Backup %s  (namespace: %s)  records: %d  bytes: %d  created: %s  finished: %s\n",
			md.BackupID,
			md.Namespace,
			stats.totalRecords,
			stats.totalBytes,
			stats.minCreated.Format(timeFormat),
			stats.maxFinished.Format(timeFormat),
		)

		if err := printNodes(out, md.Nodes); err != nil {
			return err
		}

		fmt.Fprintln(out) // blank line between backups.
	}

	fmt.Println("finished printMetadataDetailed")

	return nil
}

func printNodes(out io.Writer, nodes []models.Node) error {
	fmt.Println("start printNodes")

	if len(nodes) == 0 {
		fmt.Fprintln(out, "  no nodes")

		return nil
	}

	// Sort a copy so we don't mutate the caller's slice and the output
	// stays deterministic regardless of collection order.
	sorted := slices.Clone(nodes)
	slices.SortFunc(sorted, func(a, b models.Node) int {
		return cmp.Compare(a.NodeID, b.NodeID)
	})

	w := tabwriter.NewWriter(out, 0, 0, 3, ' ', tabwriter.Debug)

	fmt.Fprintln(w, "  NODE ID\tCREATED\tFINISHED\tRECORDS\tBYTES\tPARTITIONS\tSEGMENTS")

	for _, n := range sorted {
		fmt.Fprintf(w, "  %s\t%s\t%s\t%d\t%d\t%d\t%d\n",
			n.NodeID,
			n.Created.Format(timeFormat),
			n.Finished.Format(timeFormat),
			n.RecordCount,
			n.ByteCount,
			n.PartitionCount,
			n.SegmentCount,
		)
	}

	fmt.Println("try to flush printNodes")

	if err := w.Flush(); err != nil {
		return fmt.Errorf("failed to flush nodes output: %w", err)
	}

	fmt.Println("end printNodes")

	return nil
}

func calcMetadataStats(md models.Metadata) metaDataStats {
	var stats metaDataStats

	if len(md.Nodes) == 0 {
		return stats
	}

	// Initialize with the first node so min/max comparisons are correct.
	stats.minCreated = md.Nodes[0].Created
	stats.maxFinished = md.Nodes[0].Finished

	for _, node := range md.Nodes {
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
