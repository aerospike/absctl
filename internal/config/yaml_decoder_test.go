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

package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/aerospike/absctl/internal/config/dto"
	"github.com/aerospike/absctl/internal/models"
	"github.com/stretchr/testify/assert"
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

	// validServerBackupYAML describes the whole snapshot-backup command tree:
	// each subcommand reads only its own section.
	validServerBackupYAML = `
app:
  log-level: info
cluster:
  seeds:
    - host: 127.0.0.1
      port: 3000
backup:
  namespace: test
  object-storage-type: aws-s3
  set-list:
    - set1
    - set2
  no-indexes: true
list:
  path: some/prefix
validate:
  backup-id: bkp-1
  sample-size: 500
progress:
  backup-id: bkp-1
  watch: true
  watch-poll: 2000
aws:
  s3:
    bucket-name: my-bucket
    region: eu-central-1
`

	// validServerRestoreYAML describes the whole snapshot-restore command tree.
	validServerRestoreYAML = `
app:
  log-level: info
cluster:
  seeds:
    - host: 127.0.0.1
      port: 3000
restore:
  namespace: test
  object-storage-type: aws-s3
  backup-id: bkp-1
  path: some/prefix
  fuzzy-restore: true
prepare:
  namespace: test
  backup-id: bkp-1
progress:
  namespace: test
aws:
  s3:
    bucket-name: my-bucket
    region: eu-central-1
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
			}

			filename := tt.filename
			if tt.setupFile {
				filename = tempFile
			}

			config, err := DecodeBackupServiceConfig(t.Context(), filename)

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
			}

			filename := tt.filename
			if tt.setupFile {
				filename = tempFile
			}

			config, err := DecodeRestoreServiceConfig(t.Context(), filename)

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
					LogLevel: new(logLevel),
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

func TestDecodeServerBackupServiceConfig(t *testing.T) {
	t.Parallel()

	filename := createTempFile(t, "valid_server_backup.yaml", validServerBackupYAML)

	tests := []struct {
		name    string
		command ServerBackupCommand
		// assert checks that only the section belonging to the subcommand is
		// populated. The others must stay nil so their validation is skipped.
		assert func(t *testing.T, cfg *ServerBackupServiceConfig)
	}{
		{
			name:    "start reads the backup section",
			command: ServerBackupCommandStart,
			assert: func(t *testing.T, cfg *ServerBackupServiceConfig) {
				t.Helper()

				require.NotNil(t, cfg.Start)
				assert.Nil(t, cfg.List)
				assert.Nil(t, cfg.Validation)
				assert.Nil(t, cfg.Progress)

				assert.Equal(t, "test", cfg.Start.Namespace)
				assert.Equal(t, "aws-s3", cfg.Start.StorageType)
				assert.Equal(t, "set1,set2", cfg.Start.SetList)
				assert.True(t, cfg.Start.NoIndexes)
			},
		},
		{
			name:    "list reads the list section",
			command: ServerBackupCommandList,
			assert: func(t *testing.T, cfg *ServerBackupServiceConfig) {
				t.Helper()

				require.NotNil(t, cfg.List)
				assert.Nil(t, cfg.Start)
				assert.Nil(t, cfg.Validation)
				assert.Nil(t, cfg.Progress)

				assert.Equal(t, "some/prefix", cfg.List.Path)
			},
		},
		{
			name:    "validate reads the validate section",
			command: ServerBackupCommandValidate,
			assert: func(t *testing.T, cfg *ServerBackupServiceConfig) {
				t.Helper()

				require.NotNil(t, cfg.Validation)
				assert.Nil(t, cfg.Start)
				assert.Nil(t, cfg.List)
				assert.Nil(t, cfg.Progress)

				assert.Equal(t, "bkp-1", cfg.Validation.JobID)
				assert.Equal(t, 500, cfg.Validation.SampleSize)
			},
		},
		{
			name:    "progress reads the progress section",
			command: ServerBackupCommandProgress,
			assert: func(t *testing.T, cfg *ServerBackupServiceConfig) {
				t.Helper()

				require.NotNil(t, cfg.Progress)
				assert.Nil(t, cfg.Start)
				assert.Nil(t, cfg.List)
				assert.Nil(t, cfg.Validation)

				assert.Equal(t, "bkp-1", cfg.Progress.JobID)
				assert.True(t, cfg.Progress.Watch)
				assert.Equal(t, int64(2000), cfg.Progress.WatchPoll)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg, err := DecodeServerBackupServiceConfig(t.Context(), filename, tt.command)
			require.NoError(t, err)
			require.NotNil(t, cfg)

			// The shared sections are always present.
			require.NotNil(t, cfg.App)
			require.NotNil(t, cfg.ClientConfig)
			require.NotNil(t, cfg.AwsS3)
			assert.Equal(t, "my-bucket", cfg.AwsS3.BucketName)
			assert.Equal(t, "eu-central-1", cfg.AwsS3.Region)

			tt.assert(t, cfg)

			// Only the populated section is validated, so a config carrying a
			// single subcommand's settings passes for that subcommand.
			require.NoError(t, cfg.Validate(false))
		})
	}
}

func TestDecodeServerRestoreServiceConfig(t *testing.T) {
	t.Parallel()

	filename := createTempFile(t, "valid_server_restore.yaml", validServerRestoreYAML)

	tests := []struct {
		name    string
		command ServerRestoreCommand
		assert  func(t *testing.T, cfg *ServerRestoreServiceConfig)
	}{
		{
			name:    "start reads the restore section",
			command: ServerRestoreCommandStart,
			assert: func(t *testing.T, cfg *ServerRestoreServiceConfig) {
				t.Helper()

				require.NotNil(t, cfg.Start)
				assert.Nil(t, cfg.Prepare)
				assert.Nil(t, cfg.Progress)

				assert.Equal(t, "test", cfg.Start.Namespace)
				assert.Equal(t, "aws-s3", cfg.Start.StorageType)
				assert.Equal(t, "bkp-1", cfg.Start.JobID)
				assert.Equal(t, "some/prefix", cfg.Start.Path)
				assert.True(t, cfg.Start.FuzzyRestore)
			},
		},
		{
			name:    "prepare reads the prepare section",
			command: ServerRestoreCommandPrepare,
			assert: func(t *testing.T, cfg *ServerRestoreServiceConfig) {
				t.Helper()

				require.NotNil(t, cfg.Prepare)
				assert.Nil(t, cfg.Start)
				assert.Nil(t, cfg.Progress)

				assert.Equal(t, "test", cfg.Prepare.Namespace)
				assert.Equal(t, "bkp-1", cfg.Prepare.JobID)
			},
		},
		{
			name:    "progress reads the progress section",
			command: ServerRestoreCommandProgress,
			assert: func(t *testing.T, cfg *ServerRestoreServiceConfig) {
				t.Helper()

				require.NotNil(t, cfg.Progress)
				assert.Nil(t, cfg.Start)
				assert.Nil(t, cfg.Prepare)

				assert.Equal(t, "test", cfg.Progress.Namespace)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg, err := DecodeServerRestoreServiceConfig(t.Context(), filename, tt.command)
			require.NoError(t, err)
			require.NotNil(t, cfg)

			require.NotNil(t, cfg.App)
			require.NotNil(t, cfg.ClientConfig)
			require.NotNil(t, cfg.AwsS3)
			assert.Equal(t, "my-bucket", cfg.AwsS3.BucketName)

			tt.assert(t, cfg)

			require.NoError(t, cfg.Validate(false))
		})
	}
}

func TestDecodeServerServiceConfigErrors(t *testing.T) {
	t.Parallel()

	t.Run("unknown field is rejected", func(t *testing.T) {
		t.Parallel()

		filename := createTempFile(t, "unknown_field.yaml", "backup:\n  namespace: ns1\n  bogus: 1\n")

		cfg, err := DecodeServerBackupServiceConfig(t.Context(), filename, ServerBackupCommandStart)
		require.Error(t, err)
		require.Nil(t, cfg)
		assert.Contains(t, err.Error(), "field bogus not found")
	})

	t.Run("missing file", func(t *testing.T) {
		t.Parallel()

		cfg, err := DecodeServerBackupServiceConfig(t.Context(), "non_existent.yaml", ServerBackupCommandStart)
		require.Error(t, err)
		require.Nil(t, cfg)
		assert.Contains(t, err.Error(), "failed to open config file non_existent.yaml:")
	})

	t.Run("empty path", func(t *testing.T) {
		t.Parallel()

		cfg, err := DecodeServerRestoreServiceConfig(t.Context(), "", ServerRestoreCommandStart)
		require.Error(t, err)
		require.Nil(t, cfg)
		assert.Contains(t, err.Error(), "config path is empty")
	})

	t.Run("unknown subcommand", func(t *testing.T) {
		t.Parallel()

		filename := createTempFile(t, "unknown_command.yaml", validServerBackupYAML)

		cfg, err := DecodeServerBackupServiceConfig(t.Context(), filename, ServerBackupCommand(99))
		require.Error(t, err)
		require.Nil(t, cfg)
		assert.Contains(t, err.Error(), "unknown snapshot-backup command")
	})
}

func TestDecodeAppConfig(t *testing.T) {
	t.Parallel()

	t.Run("reads the app section and ignores the rest", func(t *testing.T) {
		t.Parallel()

		// The full schema is decoded later by the subcommand, so the sections
		// this call does not care about must not make it fail.
		filename := createTempFile(t, "app.yaml", validServerBackupYAML)

		app, err := DecodeAppConfig(filename)
		require.NoError(t, err)
		require.NotNil(t, app)
		assert.Equal(t, "info", app.LogLevel)
	})

	t.Run("falls back to defaults when the section is absent", func(t *testing.T) {
		t.Parallel()

		filename := createTempFile(t, "no_app.yaml", "backup:\n  namespace: ns1\n")

		app, err := DecodeAppConfig(filename)
		require.NoError(t, err)
		require.NotNil(t, app)
		assert.Equal(t, models.DefaultAppLogLevel, app.LogLevel)
	})

	t.Run("empty path", func(t *testing.T) {
		t.Parallel()

		app, err := DecodeAppConfig("")
		require.Error(t, err)
		require.Nil(t, app)
		assert.Contains(t, err.Error(), "config path is empty")
	})
}
