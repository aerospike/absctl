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
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
)

const (
	defaultLogLevel = "info"
)

var validLevelStrings = map[string]struct{}{
	"debug": {},
	"info":  {},
	"warn":  {},
	"error": {},
}

type Config struct {
	// Level is parsed when Verbose==true (keeps your original behavior).
	// Valid: debug, info, warn, error.
	Level   string
	Verbose bool

	// JSON switches handler format for both stderr and file handlers.
	JSON bool

	// File used to write logs to the file.
	File string
}

// NewConfig creates a new Config with the given parameters.
func NewConfig(verbose, json bool, level, file string) *Config {
	return &Config{
		Level:   level,
		Verbose: verbose,
		JSON:    json,
		File:    file,
	}
}

func (c *Config) Validate() error {
	lvl := strings.ToLower(strings.TrimSpace(c.Level))

	if lvl == "" {
		c.Level = defaultLogLevel
	}

	if _, ok := validLevelStrings[lvl]; !ok {
		return fmt.Errorf("invalid log level %q (valid: debug, info, warn, error)", c.Level)
	}
	c.Level = lvl

	return nil
}

// NewLogger creates a new logger with the given configuration.
// Returns logger, close function, and error.
func NewLogger(cfg *Config) (*slog.Logger, func() error, error) {
	if err := cfg.Validate(); err != nil {
		return nil, nil, err
	}

	opts, err := newHandlerOptions(cfg)
	if err != nil {
		return nil, nil, err
	}

	writer, closeFun, err := newLogWriter(cfg)
	if err != nil {
		return nil, nil, err
	}

	handler := newHandler(cfg.JSON, writer, opts)

	return slog.New(handler),
		closeFun,
		nil
}

// newLogWriter creates a new log writer based on the given configuration.
func newLogWriter(cfg *Config) (io.Writer, func() error, error) {
	if cfg.File != "" {
		file, err := newFile(cfg.File)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to open new log file: %w", err)
		}

		return file, newCloseFun(file), nil
	}

	return os.Stderr, newCloseFun(nil), nil
}

// newCloseFun creates a new close function that closes the given file.
func newCloseFun(file *os.File) func() error {
	return func() error {
		if file == nil {
			return nil
		}

		if err := file.Sync(); err != nil {
			return err
		}

		if err := file.Close(); err != nil {
			return err
		}

		file = nil

		return nil
	}
}

// newHandlerOptions creates a new handler options based on the given configuration.
func newHandlerOptions(cfg *Config) (*slog.HandlerOptions, error) {
	loggerOpt := &slog.HandlerOptions{}

	if cfg.Verbose {
		var logLvl slog.Level

		err := logLvl.UnmarshalText([]byte(cfg.Level))
		if err != nil {
			return nil, fmt.Errorf("invalid log level: %w", err)
		}

		loggerOpt.Level = logLvl
	}

	return loggerOpt, nil
}

// newHandler creates a new handler based on the given configuration.
func newHandler(isJSON bool, w io.Writer, opts *slog.HandlerOptions) slog.Handler {
	if isJSON {
		return slog.NewJSONHandler(w, opts)
	}

	return slog.NewTextHandler(w, opts)
}

// newFile creates a new file based on the given path.
func newFile(path string) (*os.File, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("failed to create log file: %w", err)
	}

	flags := os.O_CREATE | os.O_WRONLY | os.O_APPEND

	return os.OpenFile(path, flags, 0o666)
}

// NewDefaultLogger returns default logger.
func NewDefaultLogger() *slog.Logger {
	cfg := &Config{
		Level:   defaultLogLevel,
		JSON:    false,
		Verbose: false,
	}
	// We can't log an error without a logger, so ignore it.
	l, _, _ := NewLogger(cfg)

	return l
}
