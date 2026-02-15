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

package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/aerospike/absctl/internal/config/dto"
	"github.com/stretchr/testify/require"
)

const (
	validBackupYAML = `
app:
  log-level: info
cluster:
  seeds:
    - host: 127.0.0.1
      tls-name: ""
      port: 3000
backup:
  namespace: test
  directory: test
compression:
  compress: zstd
encryption:
  encrypt: none
`

	validRestoreYAML = `
app:
  log-level: info
cluster:
  seeds:
    - host: 127.0.0.1
      tls-name: ""
      port: 3000
restore:
  namespace: test
  directory: test
compression:
  compress: zstd
encryption:
  encrypt: none
`

	invalidYAML = `
invalid: yaml: content:
  - this is not valid
    - yaml format
`
)

func TestDecodeBackupServiceConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		filename  string
		content   string
		setupFile bool
		wantErr   string
	}{
		{
			name:      "valid backup config",
			filename:  "valid_backup.yaml",
			content:   validBackupYAML,
			setupFile: true,
			wantErr:   "",
		},
		{
			name:      "empty filename",
			filename:  "",
			content:   "",
			setupFile: false,
			wantErr:   "config path is empty",
		},
		{
			name:      "non-existent file",
			filename:  "non_existent.yaml",
			content:   "",
			setupFile: false,
			wantErr:   "failed to open config file non_existent.yaml:",
		},
		{
			name:      "invalid yaml content",
			filename:  "invalid.yaml",
			content:   invalidYAML,
			setupFile: true,
			wantErr:   "failed to decode config file",
		},
		{
			name:      "empty file",
			filename:  "empty.yaml",
			content:   "",
			setupFile: true,
			wantErr:   "EOF",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var tempFile string
			if tt.setupFile {
				tempFile = createTempFile(t, tt.filename, tt.content)
				defer func() {
					_ = os.Remove(tempFile)
				}()
			}

			filename := tt.filename
			if tt.setupFile {
				filename = tempFile
			}

			config, err := DecodeBackupServiceConfig(filename)

			if tt.wantErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.wantErr)
				require.Nil(t, config)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, config)
		})
	}
}

func TestDecodeRestoreServiceConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		filename  string
		content   string
		setupFile bool
		wantErr   string
	}{
		{
			name:      "valid restore config",
			filename:  "valid_restore.yaml",
			content:   validRestoreYAML,
			setupFile: true,
			wantErr:   "",
		},
		{
			name:      "empty filename",
			filename:  "",
			content:   "",
			setupFile: false,
			wantErr:   "config path is empty",
		},
		{
			name:      "non-existent file",
			filename:  "non_existent.yaml",
			content:   "",
			setupFile: false,
			wantErr:   "failed to open config file non_existent.yaml:",
		},
		{
			name:      "invalid yaml content",
			filename:  "invalid.yaml",
			content:   invalidYAML,
			setupFile: true,
			wantErr:   "failed to decode config file",
		},
		{
			name:      "empty file",
			filename:  "empty.yaml",
			content:   "",
			setupFile: true,
			wantErr:   "EOF",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var tempFile string
			if tt.setupFile {
				tempFile = createTempFile(t, tt.filename, tt.content)
				defer func() {
					_ = os.Remove(tempFile)
				}()
			}

			filename := tt.filename
			if tt.setupFile {
				filename = tempFile
			}

			config, err := DecodeRestoreServiceConfig(filename)

			if tt.wantErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.wantErr)
				require.Nil(t, config)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, config)
		})
	}
}

func TestDecodeFromFile(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		filename  string
		content   string
		setupFile bool
		wantErr   string
	}{
		{
			name:      "valid yaml decode to dto.Backup",
			filename:  "valid.yaml",
			content:   validBackupYAML,
			setupFile: true,
			wantErr:   "",
		},
		{
			name:      "empty filename",
			filename:  "",
			content:   "",
			setupFile: false,
			wantErr:   "config path is empty",
		},
		{
			name:      "non-existent file",
			filename:  "missing.yaml",
			content:   "",
			setupFile: false,
			wantErr:   "failed to open config file missing.yaml:",
		},
		{
			name:      "invalid yaml syntax",
			filename:  "invalid.yaml",
			content:   invalidYAML,
			setupFile: true,
			wantErr:   "failed to decode config file",
		},
		{
			name:      "unknown fields in yaml",
			filename:  "unknown_fields.yaml",
			content:   "unknown_field: value\napp:\n  log-level: info",
			setupFile: true,
			wantErr:   "failed to decode config file",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var tempFile string
			if tt.setupFile {
				tempFile = createTempFile(t, tt.filename, tt.content)
				defer func() {
					_ = os.Remove(tempFile)
				}()
			}

			filename := tt.filename
			if tt.setupFile {
				filename = tempFile
			}

			var params dto.Backup

			err := decodeFromFile(filename, &params)

			if tt.wantErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.wantErr)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestDumpFile(t *testing.T) {
	logLevel := "info"

	tests := []struct {
		name     string
		filename string
		params   any
		wantErr  string
	}{
		{
			name:     "valid struct dump",
			filename: "valid_dump.yaml",
			params: dto.Backup{
				App: dto.App{
					LogLevel: &logLevel,
				},
			},
			wantErr: "",
		},
		{
			name:     "nil params",
			filename: "nil_dump.yaml",
			params:   nil,
			wantErr:  "",
		},
		{
			name:     "complex nested struct",
			filename: "map_dump.yaml",
			params: map[string]any{
				"key1": "value1",
				"key2": map[string]string{
					"nested": "value",
				},
			},
			wantErr: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if _, err := os.Stat(tt.filename); err == nil {
					_ = os.Remove(tt.filename)
				}
			}()

			err := DumpFile(tt.filename, tt.params)

			if tt.wantErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.wantErr)

				return
			}

			require.NoError(t, err)

			// Verify file was created and contains data
			info, err := os.Stat(tt.filename)
			require.NoError(t, err)
			require.Positive(t, info.Size())
		})
	}
}

// Helper function to create temporary files for testing
func createTempFile(t *testing.T, name, content string) string {
	t.Helper()

	tempDir := t.TempDir()
	tempFile := filepath.Join(tempDir, name)

	err := os.WriteFile(tempFile, []byte(content), 0o600)
	require.NoError(t, err)

	return tempFile
}
