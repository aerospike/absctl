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

	"github.com/aerospike/backup-go/pkg/server/models"
)

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
	attrs := []any{
		slog.String("backup-id", md.BackupID),
		slog.String("namespace", md.Namespace),
	}

	logger.Info("backup entry", attrs...)
}

func printMetadataToTable(out io.Writer, data []models.Metadata, withNodes bool) error {
	return printMetadataSummary(out, data)
}

func printMetadataSummary(out io.Writer, data []models.Metadata) error {
	w := tabwriter.NewWriter(out, 0, 0, 3, ' ', tabwriter.Debug)

	fmt.Fprintln(w, "BACKUP ID\tNAMESPACE")

	for i := range data {
		md := data[i]

		fmt.Fprintf(w, "%s\t%s\n",
			md.BackupID,
			md.Namespace,
		)
	}

	if err := w.Flush(); err != nil {
		return fmt.Errorf("failed to flush output: %w", err)
	}

	return nil
}
