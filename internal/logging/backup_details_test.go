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

func sampleBackupDetails() api.DtoBackupDetails {
	created := "2024-01-01T00:00:00Z"
	ns := "test"
	key := "data/backup-1"
	rec, byt, fil := int64(1000), int64(50000), int64(2)

	return api.DtoBackupDetails{
		Created:     &created,
		Namespace:   &ns,
		Key:         &key,
		RecordCount: &rec,
		ByteCount:   &byt,
		FileCount:   &fil,
	}
}

func TestPrintBackupDetailsByRoutine_Console(t *testing.T) {
	data := map[string][]api.DtoBackupDetails{
		"daily": {sampleBackupDetails()},
	}

	output := captureOutput(t, func() {
		PrintBackupDetailsByRoutine(data, false, false, nil)
	})

	assert.Contains(t, output, headerFullBackupsList)
	assert.Contains(t, output, "ROUTINE")
	assert.Contains(t, output, "CREATED")
	assert.Contains(t, output, "daily")
	assert.Contains(t, output, "test")
	assert.Contains(t, output, "1000")
	assert.Contains(t, output, "data/backup-1")
}

func TestPrintBackupDetailsByRoutine_Console_Empty(t *testing.T) {
	output := captureOutput(t, func() {
		PrintBackupDetailsByRoutine(nil, true, false, nil)
	})

	assert.Contains(t, output, headerIncrementalBackupsList)
	assert.Contains(t, output, "No backups found")
}

func TestPrintBackupDetailsForRoutine_Console(t *testing.T) {
	data := []api.DtoBackupDetails{sampleBackupDetails()}

	output := captureOutput(t, func() {
		PrintBackupDetailsForRoutine(data, "daily", false, false, nil)
	})

	assert.Contains(t, output, headerFullBackupsList)
	assert.Contains(t, output, "Routine")
	assert.Contains(t, output, "daily")
	assert.NotContains(t, output, "ROUTINE", "single-routine view should not show the ROUTINE column")
	assert.Contains(t, output, "CREATED")
	assert.Contains(t, output, "test")
}

func TestPrintBackupDetailsByRoutine_JSON(t *testing.T) {
	data := map[string][]api.DtoBackupDetails{
		"daily": {sampleBackupDetails()},
	}

	out := captureLogJSON(t, func(logger *slog.Logger) {
		PrintBackupDetailsByRoutine(data, false, true, logger)
	})

	assert.Contains(t, out, `"msg":"full backups"`)
	assert.Contains(t, out, `"key":"data/backup-1"`)
}

func TestPrintBackupDetailsForRoutine_JSON(t *testing.T) {
	data := []api.DtoBackupDetails{sampleBackupDetails()}

	out := captureLogJSON(t, func(logger *slog.Logger) {
		PrintBackupDetailsForRoutine(data, "daily", true, true, logger)
	})

	assert.Contains(t, out, `"msg":"incremental backups"`)
	assert.Contains(t, out, `"routine":"daily"`)
}
