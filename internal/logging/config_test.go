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

func sampleConfig() *api.DtoConfig {
	clusters := map[string]api.DtoAerospikeCluster{"primary": sampleCluster()}
	policies := map[string]api.DtoBackupPolicy{"daily": samplePolicy()}
	routines := map[string]api.DtoBackupRoutine{"hourly": sampleRoutine()}
	storage := map[string]api.DtoStorage{"local": sampleLocalStorage()}

	return &api.DtoConfig{
		AerospikeClusters: &clusters,
		BackupPolicies:    &policies,
		BackupRoutines:    &routines,
		Storage:           &storage,
	}
}

func TestPrintConfig_Console(t *testing.T) {
	output := captureOutput(t, func() {
		PrintConfig(sampleConfig(), false, nil)
	})

	assert.Contains(t, output, headerConfig)
	assert.Contains(t, output, "primary")
	assert.Contains(t, output, "daily")
	assert.Contains(t, output, "hourly")
	assert.Contains(t, output, "/var/backup")
}

func TestPrintConfig_Console_Nil(t *testing.T) {
	output := captureOutput(t, func() {
		PrintConfig(nil, false, nil)
	})

	assert.Contains(t, output, "No configuration available")
}

func TestPrintConfig_JSON(t *testing.T) {
	out := captureLogJSON(t, func(logger *slog.Logger) {
		PrintConfig(sampleConfig(), true, logger)
	})

	assert.Contains(t, out, `"msg":"config"`)
	assert.Contains(t, out, `"primary"`)
}
