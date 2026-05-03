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
	"bytes"
	"log/slog"
	"testing"
)

// captureOutput runs fn while swapping the package-level output writer with
// an in-memory buffer and returns whatever fn wrote.
//
// Tests that exercise pretty-printers should not call SetOutWriter directly;
// using this helper guarantees the previous writer is restored even when the
// test fails.
func captureOutput(t *testing.T, fn func()) string {
	t.Helper()

	buf := &bytes.Buffer{}
	prev := SetOutWriter(buf)

	t.Cleanup(func() {
		SetOutWriter(prev)
	})

	fn()

	return buf.String()
}

// captureLogJSON runs fn with a JSON slog logger writing into a buffer and
// returns the resulting JSON line(s). Use it to verify log emissions that
// PrintXxx functions produce when toLog is true.
func captureLogJSON(_ *testing.T, fn func(logger *slog.Logger)) string {
	buf := &bytes.Buffer{}
	logger := slog.New(slog.NewJSONHandler(buf, nil))

	fn(logger)

	return buf.String()
}
