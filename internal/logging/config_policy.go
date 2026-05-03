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

	"github.com/aerospike/absctl/api"
)

const (
	headerPolicy   = "Backup policy"
	headerPolicies = "Backup policies"
)

// PrintPolicies prints all configured backup policies keyed by name. When
// toLog is true the entire map is logged via slog; otherwise a tabular
// summary is rendered to stderr.
func PrintPolicies(policies map[string]api.DtoBackupPolicy, toLog bool, logger *slog.Logger) {
	if toLog {
		logger.Info("policies", slog.Any("policies", policies))
		return
	}

	printSection(headerPolicies)
	printPoliciesTable(policies)
}

// PrintPolicy prints a single backup policy. When toLog is true the DTO is
// logged via slog; otherwise key fields are rendered as a section.
func PrintPolicy(name string, policy *api.DtoBackupPolicy, toLog bool, logger *slog.Logger) {
	if toLog {
		logger.Info("policy",
			slog.String("name", name),
			slog.Any("policy", policy),
		)

		return
	}

	printPolicy(name, policy)
}

func printPoliciesTable(policies map[string]api.DtoBackupPolicy) {
	if len(policies) == 0 {
		printToOutWriter("No policies configured.")
		return
	}

	tw := newTabWriter()
	writeRow(tw, "NAME", "PARALLEL", "BANDWIDTH", "FILE LIMIT", "COMPRESSION", "ENCRYPTION", "SEALED")

	for _, name := range sortedKeys(policies) {
		p := policies[name]
		writeRow(tw,
			name,
			strconv.Itoa(intVal(p.Parallel)),
			strconv.Itoa(intVal(p.Bandwidth)),
			strconv.Itoa(intVal(p.FileLimit)),
			compressionMode(p.Compression),
			encryptionMode(p.Encryption),
			strconv.FormatBool(boolVal(p.Sealed)),
		)
	}

	_ = tw.Flush()
}

func printPolicy(name string, p *api.DtoBackupPolicy) {
	printSection(headerPolicy)
	printMetric("Name", name)

	if p == nil {
		return
	}

	printMetric("Parallel", intVal(p.Parallel))
	printMetric("Parallel Write", intVal(p.ParallelWrite))
	printMetric("Bandwidth", intVal(p.Bandwidth))
	printMetric("Records/Second", intVal(p.RecordsPerSecond))
	printMetric("File Limit", intVal(p.FileLimit))
	printMetric("Max Concurrent Nodes", intVal(p.MaxConcurrentNodes))
	printMetric("Compression", compressionMode(p.Compression))
	printMetric("Encryption", encryptionMode(p.Encryption))
	printMetric("Sealed", boolVal(p.Sealed))
	printMetric("No Indexes", boolVal(p.NoIndexes))
	printMetric("No Records", boolVal(p.NoRecords))
	printMetric("No UDFs", boolVal(p.NoUdfs))
	printMetric("Socket Timeout", intVal(p.SocketTimeout))
	printMetric("Total Timeout", intVal(p.TotalTimeout))
}

func compressionMode(c *api.DtoCompressionPolicy) string {
	if c == nil || c.Mode == nil {
		return "-"
	}

	return string(*c.Mode)
}

func encryptionMode(e *api.DtoEncryptionPolicy) string {
	if e == nil || e.Mode == nil {
		return "-"
	}

	return string(*e.Mode)
}
