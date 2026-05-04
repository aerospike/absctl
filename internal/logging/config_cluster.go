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
	"strconv"
	"strings"

	"github.com/aerospike/absctl/api"
)

const (
	headerCluster  = "Aerospike cluster"
	headerClusters = "Aerospike clusters"
)

// PrintClusters prints all configured clusters keyed by name. When toLog is
// true the entire map is logged via slog; otherwise a tabular summary is
// rendered to stderr.
func PrintClusters(clusters map[string]api.DtoAerospikeCluster, toLog bool, logger *slog.Logger) {
	if toLog {
		logger.Info("clusters", slog.Any("clusters", clusters))
		return
	}

	printSection(headerClusters)
	printClustersTable(clusters)
}

// PrintCluster prints a single cluster definition. When toLog is true the DTO
// is logged via slog; otherwise key fields are rendered as a section.
func PrintCluster(name string, cluster *api.DtoAerospikeCluster, toLog bool, logger *slog.Logger) {
	if toLog {
		logger.Info("cluster",
			slog.String("name", name),
			slog.Any("cluster", cluster),
		)

		return
	}

	printCluster(name, cluster)
}

func printClustersTable(clusters map[string]api.DtoAerospikeCluster) {
	if len(clusters) == 0 {
		printToOutWriter("No clusters configured.")
		return
	}

	tw := newTabWriter()
	writeRow(tw, "NAME", "LABEL", "SEED NODES", "MAX PARALLEL SCANS", "USE SERVICES ALTERNATE")

	for _, name := range sortedKeys(clusters) {
		c := clusters[name]
		writeRow(tw,
			name,
			strVal(c.Label),
			formatSeedNodes(c.SeedNodes),
			strconv.Itoa(intVal(c.MaxParallelScans)),
			strconv.FormatBool(boolVal(c.UseServicesAlternate)),
		)
	}

	_ = tw.Flush()
}

func printCluster(name string, c *api.DtoAerospikeCluster) {
	printSection(headerCluster)
	printMetric("Name", name)

	if c == nil {
		return
	}

	printMetric("Label", strVal(c.Label))
	printMetric("Seed Nodes", formatSeedNodes(c.SeedNodes))
	printMetric("Conn Timeout", intVal(c.ConnTimeout))
	printMetric("Max Parallel Scans", intVal(c.MaxParallelScans))
	printMetric("Use Services Alt", boolVal(c.UseServicesAlternate))

	if c.Credentials != nil && c.Credentials.User != nil {
		printMetric("User", strVal(c.Credentials.User))
	}

	if c.Tls != nil {
		printMetric("TLS Name", strVal(c.Tls.Name))
	}
}

func formatSeedNodes(nodes []api.DtoSeedNode) string {
	if len(nodes) == 0 {
		return "-"
	}

	parts := make([]string, 0, len(nodes))
	for i := range nodes {
		parts = append(parts, fmt.Sprintf("%s:%d", nodes[i].HostName, nodes[i].Port))
	}

	return strings.Join(parts, ",")
}
