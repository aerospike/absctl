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
	"strconv"
	"strings"

	"github.com/aerospike/absctl/api"
)

const (
	headerRoutine  = "Backup routine"
	headerRoutines = "Backup routines"
)

// PrintRoutines prints all configured backup routines keyed by name. When
// toLog is true the entire map is logged via slog; otherwise a tabular
// summary is rendered to stderr.
func PrintRoutines(routines map[string]api.DtoBackupRoutine, toLog bool, logger *slog.Logger) {
	if toLog {
		logger.Info("routines", slog.Any("routines", routines))
		return
	}

	printSection(headerRoutines)
	printRoutinesTable(routines)
}

// PrintRoutine prints a single backup routine. When toLog is true the DTO is
// logged via slog; otherwise key fields are rendered as a section.
func PrintRoutine(name string, routine *api.DtoBackupRoutine, toLog bool, logger *slog.Logger) {
	if toLog {
		logger.Info("routine",
			slog.String("name", name),
			slog.Any("routine", routine),
		)

		return
	}

	printRoutine(name, routine)
}

func printRoutinesTable(routines map[string]api.DtoBackupRoutine) {
	if len(routines) == 0 {
		printToStderr("No routines configured.")
		return
	}

	tw := newTabWriter()
	writeRow(tw, "NAME", "CLUSTER", "STORAGE", "POLICY", "INTERVAL", "INCR INTERVAL", "NAMESPACES", "DISABLED")

	for _, name := range sortedKeys(routines) {
		r := routines[name]
		writeRow(tw,
			name,
			r.SourceCluster,
			r.Storage,
			strVal(r.BackupPolicy),
			r.IntervalCron,
			strVal(r.IncrIntervalCron),
			joinStrings(r.Namespaces),
			strconv.FormatBool(boolVal(r.Disabled)),
		)
	}

	_ = tw.Flush()
}

func printRoutine(name string, r *api.DtoBackupRoutine) {
	printSection(headerRoutine)
	printMetric("Name", name)

	if r == nil {
		return
	}

	printMetric("Source Cluster", r.SourceCluster)
	printMetric("Storage", r.Storage)
	printMetric("Backup Policy", strVal(r.BackupPolicy))
	printMetric("Interval Cron", r.IntervalCron)
	printMetric("Incr Interval Cron", strVal(r.IncrIntervalCron))
	printMetric("Namespaces", joinStrings(r.Namespaces))
	printMetric("Disabled", boolVal(r.Disabled))

	if r.SetList != nil {
		printMetric("Set List", joinStrings(*r.SetList))
	}

	if r.BinList != nil {
		printMetric("Bin List", joinStrings(*r.BinList))
	}

	if r.NodeList != nil {
		printMetric("Node List", joinStrings(*r.NodeList))
	}
}

func joinStrings(s []string) string {
	if len(s) == 0 {
		return "-"
	}

	return strings.Join(s, ",")
}
