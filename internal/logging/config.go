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

	"github.com/aerospike/absctl/api"
)

const headerConfig = "Service configuration"

// PrintConfig prints the full service configuration. When toLog is true the
// entire DTO is logged as a structured entry; otherwise each section
// (clusters, policies, routines, storage) is rendered as its own table.
func PrintConfig(cfg *api.DtoConfig, toLog bool, logger *slog.Logger) {
	if toLog {
		logger.Info("config", slog.Any("config", cfg))
		return
	}

	printConfig(cfg)
}

func printConfig(cfg *api.DtoConfig) {
	printSection(headerConfig)

	if cfg == nil {
		printToOutWriter("No configuration available.")
		return
	}

	printSubSection("Clusters")

	if cfg.AerospikeClusters != nil {
		printClustersTable(*cfg.AerospikeClusters)
	}

	printSubSection("Policies")

	if cfg.BackupPolicies != nil {
		printPoliciesTable(*cfg.BackupPolicies)
	}

	printSubSection("Routines")

	if cfg.BackupRoutines != nil {
		printRoutinesTable(*cfg.BackupRoutines)
	}

	printSubSection("Storage")

	if cfg.Storage != nil {
		printStorageTable(*cfg.Storage)
	}
}
