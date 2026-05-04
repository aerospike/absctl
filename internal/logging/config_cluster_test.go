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
	"testing"

	"github.com/aerospike/absctl/api"
	"github.com/stretchr/testify/assert"
)

func sampleCluster() api.DtoAerospikeCluster {
	label := "primary"

	return api.DtoAerospikeCluster{
		Label: &label,
		SeedNodes: []api.DtoSeedNode{
			{HostName: "10.0.0.1", Port: 3000},
		},
	}
}

func TestPrintClusters_Console(t *testing.T) {
	clusters := map[string]api.DtoAerospikeCluster{"primary": sampleCluster()}

	output := captureOutput(t, func() {
		PrintClusters(clusters, false, nil)
	})

	assert.Contains(t, output, headerClusters)
	assert.Contains(t, output, "NAME")
	assert.Contains(t, output, "primary")
	assert.Contains(t, output, "10.0.0.1:3000")
}

func TestPrintClusters_Console_Empty(t *testing.T) {
	output := captureOutput(t, func() {
		PrintClusters(nil, false, nil)
	})

	assert.Contains(t, output, headerClusters)
	assert.Contains(t, output, "No clusters configured")
}

func TestPrintCluster_Console(t *testing.T) {
	c := sampleCluster()

	output := captureOutput(t, func() {
		PrintCluster("primary", &c, false, nil)
	})

	assert.Contains(t, output, headerCluster)
	assert.Contains(t, output, "primary")
	assert.Contains(t, output, "10.0.0.1:3000")
}

func TestPrintClusters_JSON(t *testing.T) {
	clusters := map[string]api.DtoAerospikeCluster{"primary": sampleCluster()}

	out := captureLogJSON(t, func(logger *slog.Logger) {
		PrintClusters(clusters, true, logger)
	})

	assert.Contains(t, out, `"msg":"clusters"`)
	assert.Contains(t, out, `"label":"primary"`)
}

func TestPrintCluster_JSON(t *testing.T) {
	c := sampleCluster()

	out := captureLogJSON(t, func(logger *slog.Logger) {
		PrintCluster("primary", &c, true, logger)
	})

	assert.Contains(t, out, `"name":"primary"`)
	assert.Contains(t, out, `"host-name":"10.0.0.1"`)
}
