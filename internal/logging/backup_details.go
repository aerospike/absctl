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
	"log/slog"
	"sort"
	"strconv"
	"text/tabwriter"

	"github.com/aerospike/absctl/api"
)

const (
	headerFullBackupsList        = "Full backups"
	headerIncrementalBackupsList = "Incremental backups"
)

// PrintBackupDetailsByRoutine prints a per-routine grouped listing of backup
// details. When toLog is true the entire map is logged via slog; otherwise a
// table grouped by routine is rendered to stderr.
func PrintBackupDetailsByRoutine(
	data map[string][]api.DtoBackupDetails, isIncremental, toLog bool, logger *slog.Logger,
) {
	if toLog {
		logger.Info(logHeader(isIncremental), slog.Any("backups", data))
		return
	}

	printBackupDetailsByRoutine(data, isIncremental)
}

// PrintBackupDetailsForRoutine prints a flat listing of backup details for a
// single routine. When toLog is true the slice is logged via slog; otherwise
// a table is rendered to stderr.
func PrintBackupDetailsForRoutine(
	data []api.DtoBackupDetails, name string, isIncremental, toLog bool, logger *slog.Logger,
) {
	if toLog {
		logger.Info(logHeader(isIncremental),
			slog.String("routine", name),
			slog.Any("backups", data),
		)

		return
	}

	printBackupDetailsForRoutine(data, name, isIncremental)
}

func printBackupDetailsByRoutine(data map[string][]api.DtoBackupDetails, isIncremental bool) {
	header := headerFullBackupsList
	if isIncremental {
		header = headerIncrementalBackupsList
	}

	printSection(header)

	if len(data) == 0 {
		printToOutWriter("No backups found.")
		return
	}

	tw := newTabWriter()
	writeBackupDetailsHeader(tw, true)

	for _, routine := range sortedKeys(data) {
		for i := range data[routine] {
			writeBackupDetailsRow(tw, routine, &data[routine][i])
		}
	}

	_ = tw.Flush()
}

func printBackupDetailsForRoutine(data []api.DtoBackupDetails, name string, isIncremental bool) {
	header := headerFullBackupsList
	if isIncremental {
		header = headerIncrementalBackupsList
	}

	printSection(header)
	printMetric("Routine", name)
	printToOutWriter("")

	if len(data) == 0 {
		printToOutWriter("No backups found.")
		return
	}

	tw := newTabWriter()
	writeBackupDetailsHeader(tw, false)

	for i := range data {
		writeBackupDetailsRow(tw, "", &data[i])
	}

	_ = tw.Flush()
}

func writeBackupDetailsHeader(tw *tabwriter.Writer, withRoutine bool) {
	if withRoutine {
		writeRow(tw, "ROUTINE", "CREATED", "NAMESPACE", "RECORDS", "BYTES", "FILES", "KEY")
		return
	}

	writeRow(tw, "CREATED", "NAMESPACE", "RECORDS", "BYTES", "FILES", "KEY")
}

func writeBackupDetailsRow(tw *tabwriter.Writer, routine string, b *api.DtoBackupDetails) {
	cells := []string{
		strVal(b.Created),
		strVal(b.Namespace),
		strconv.FormatInt(int64Val(b.RecordCount), 10),
		strconv.FormatInt(int64Val(b.ByteCount), 10),
		strconv.FormatInt(int64Val(b.FileCount), 10),
		strVal(b.Key),
	}

	if routine != "" {
		cells = append([]string{routine}, cells...)
	}

	writeRow(tw, cells...)
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	return keys
}

func logHeader(isIncremental bool) string {
	if isIncremental {
		return "incremental backups"
	}

	return "full backups"
}
