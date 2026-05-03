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

func samplePolicy() api.DtoBackupPolicy {
	parallel := 8
	bw := 100
	mode := api.DtoCompressionPolicyMode("ZSTD")

	return api.DtoBackupPolicy{
		Parallel:  &parallel,
		Bandwidth: &bw,
		Compression: &api.DtoCompressionPolicy{
			Mode: &mode,
		},
	}
}

func TestPrintPolicies_Console(t *testing.T) {
	policies := map[string]api.DtoBackupPolicy{"daily": samplePolicy()}

	output := captureOutput(t, func() {
		PrintPolicies(policies, false, nil)
	})

	assert.Contains(t, output, headerPolicies)
	assert.Contains(t, output, "NAME")
	assert.Contains(t, output, "PARALLEL")
	assert.Contains(t, output, "daily")
	assert.Contains(t, output, "ZSTD")
}

func TestPrintPolicies_Console_Empty(t *testing.T) {
	output := captureOutput(t, func() {
		PrintPolicies(nil, false, nil)
	})

	assert.Contains(t, output, "No policies configured")
}

func TestPrintPolicy_Console(t *testing.T) {
	p := samplePolicy()

	output := captureOutput(t, func() {
		PrintPolicy("daily", &p, false, nil)
	})

	assert.Contains(t, output, headerPolicy)
	assert.Contains(t, output, "daily")
	assert.Contains(t, output, "Parallel")
	assert.Contains(t, output, "Compression")
}

func TestPrintPolicies_JSON(t *testing.T) {
	policies := map[string]api.DtoBackupPolicy{"daily": samplePolicy()}

	out := captureLogJSON(t, func(logger *slog.Logger) {
		PrintPolicies(policies, true, logger)
	})

	assert.Contains(t, out, `"msg":"policies"`)
	assert.Contains(t, out, `"parallel":8`)
}
