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

func sampleRestoreStatus() *api.DtoRestoreJobStatus {
	read := int64(1000)
	inserted := int64(950)
	status := api.DtoJobStatus("Running")

	return &api.DtoRestoreJobStatus{
		Status:          &status,
		ReadRecords:     &read,
		InsertedRecords: &inserted,
	}
}

func TestPrintRestoreStatus_Console(t *testing.T) {
	output := captureOutput(t, func() {
		PrintRestoreStatus(sampleRestoreStatus(), 42, false, nil)
	})

	assert.Contains(t, output, headerRestoreStatus)
	assert.Contains(t, output, "Job ID")
	assert.Contains(t, output, "42")
	assert.Contains(t, output, "Running")
	assert.Contains(t, output, "1000")
	assert.Contains(t, output, "950")
}

func TestPrintRestoreStatus_Console_Nil(t *testing.T) {
	output := captureOutput(t, func() {
		PrintRestoreStatus(nil, 42, false, nil)
	})

	assert.Contains(t, output, headerRestoreStatus)
	assert.Contains(t, output, "n/a")
}

func TestPrintRestoreStatus_JSON(t *testing.T) {
	out := captureLogJSON(t, func(logger *slog.Logger) {
		PrintRestoreStatus(sampleRestoreStatus(), 42, true, logger)
	})

	assert.Contains(t, out, `"job-id":42`)
	assert.Contains(t, out, `"read-records":1000`)
}
