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

func sampleRoutine() api.DtoBackupRoutine {
	policy := "daily"

	return api.DtoBackupRoutine{
		SourceCluster: "primary",
		Storage:       "local",
		BackupPolicy:  &policy,
		IntervalCron:  "0 0 * * *",
		Namespaces:    []string{"test", "users"},
	}
}

func TestPrintRoutines_Console(t *testing.T) {
	routines := map[string]api.DtoBackupRoutine{"hourly": sampleRoutine()}

	output := captureOutput(t, func() {
		PrintRoutines(routines, false, nil)
	})

	assert.Contains(t, output, headerRoutines)
	assert.Contains(t, output, "NAME")
	assert.Contains(t, output, "CLUSTER")
	assert.Contains(t, output, "hourly")
	assert.Contains(t, output, "primary")
	assert.Contains(t, output, "test,users")
}

func TestPrintRoutines_Console_Empty(t *testing.T) {
	output := captureOutput(t, func() {
		PrintRoutines(nil, false, nil)
	})

	assert.Contains(t, output, "No routines configured")
}

func TestPrintRoutine_Console(t *testing.T) {
	r := sampleRoutine()

	output := captureOutput(t, func() {
		PrintRoutine("hourly", &r, false, nil)
	})

	assert.Contains(t, output, headerRoutine)
	assert.Contains(t, output, "hourly")
	assert.Contains(t, output, "primary")
	assert.Contains(t, output, "0 0 * * *")
}

func TestPrintRoutines_JSON(t *testing.T) {
	routines := map[string]api.DtoBackupRoutine{"hourly": sampleRoutine()}

	out := captureLogJSON(t, func(logger *slog.Logger) {
		PrintRoutines(routines, true, logger)
	})

	assert.Contains(t, out, `"msg":"routines"`)
	assert.Contains(t, out, `"source-cluster":"primary"`)
}
