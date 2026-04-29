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

func sampleRoutineState() *api.DtoRoutineState {
	last := "2024-01-01T00:00:00Z"
	next := "2024-01-02T00:00:00Z"
	start := "2024-01-01T01:00:00Z"
	pct := 42
	done := 1234

	return &api.DtoRoutineState{
		LastFull:        &last,
		LastIncremental: &last,
		NextFull:        &next,
		NextIncremental: &next,
		Full: &api.DtoRunningJob{
			StartTime:      &start,
			DoneRecords:    &done,
			PercentageDone: &pct,
		},
	}
}

func TestPrintBackupState_Console(t *testing.T) {
	state := sampleRoutineState()

	output := captureOutput(t, func() {
		PrintBackupState(state, "daily", false, nil)
	})

	assert.Contains(t, output, headerBackupState)
	assert.Contains(t, output, "daily")
	assert.Contains(t, output, "2024-01-01T00:00:00Z")
	assert.Contains(t, output, "Full backup running")
	assert.Contains(t, output, "1234")
	assert.Contains(t, output, "42")
}

func TestPrintBackupState_Console_Nil(t *testing.T) {
	output := captureOutput(t, func() {
		PrintBackupState(nil, "daily", false, nil)
	})

	assert.Contains(t, output, headerBackupState)
	assert.Contains(t, output, "daily")
	assert.Contains(t, output, "n/a")
}

func TestPrintBackupState_JSON(t *testing.T) {
	state := sampleRoutineState()

	out := captureLogJSON(t, func(logger *slog.Logger) {
		PrintBackupState(state, "daily", true, logger)
	})

	assert.Contains(t, out, `"routine":"daily"`)
	assert.Contains(t, out, `"done-records":1234`)
}
