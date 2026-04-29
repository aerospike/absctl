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

func TestPrintRestoreJobs_Console(t *testing.T) {
	jobs := map[string]api.DtoRestoreJobStatus{
		"42": *sampleRestoreStatus(),
	}

	output := captureOutput(t, func() {
		PrintRestoreJobs(jobs, false, nil)
	})

	assert.Contains(t, output, headerRestoreJobs)
	assert.Contains(t, output, "JOB ID")
	assert.Contains(t, output, "STATUS")
	assert.Contains(t, output, "42")
	assert.Contains(t, output, "Running")
	assert.Contains(t, output, "1000")
}

func TestPrintRestoreJobs_Console_Empty(t *testing.T) {
	output := captureOutput(t, func() {
		PrintRestoreJobs(map[string]api.DtoRestoreJobStatus{}, false, nil)
	})

	assert.Contains(t, output, headerRestoreJobs)
	assert.Contains(t, output, "No restore jobs found")
}

func TestPrintRestoreJobs_JSON(t *testing.T) {
	jobs := map[string]api.DtoRestoreJobStatus{
		"42": *sampleRestoreStatus(),
	}

	out := captureLogJSON(t, func(logger *slog.Logger) {
		PrintRestoreJobs(jobs, true, logger)
	})

	assert.Contains(t, out, `"msg":"restore jobs"`)
	assert.Contains(t, out, `"42"`)
}
