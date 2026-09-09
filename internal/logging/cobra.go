// Copyright 2026 Aerospike, Inc.
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
	"strings"
)

// CobraLogger is a wrapper around slog to use it as the default writer for cobra output.
type CobraLogger struct {
	logger *slog.Logger
}

// NewCobraLogger returns a wrapper for logging cobra output.
func NewCobraLogger(logger *slog.Logger) *CobraLogger {
	return &CobraLogger{
		logger: logger,
	}
}

// Write prints any message with slog.logger on WARN level.
func (c *CobraLogger) Write(p []byte) (n int, err error) {
	// Cobra can add \n or spaces to the end of the line, so remove it for pretty printing.
	msg := strings.TrimSpace(string(p))
	c.logger.Warn(msg)

	return len(p), nil
}
