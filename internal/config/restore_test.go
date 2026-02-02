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
	"log/slog"
	"os"
	"runtime"
	"testing"

	"github.com/aerospike/absctl/internal/models"
	"github.com/aerospike/tools-common-go/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewRestoreServiceConfig_WithoutConfigFile(t *testing.T) {
	t.Parallel()

	app := &models.App{ConfigFilePath: ""}
	clientConfig := &client.AerospikeConfig{}
	clientPolicy := &models.ClientPolicy{}
	restore := &models.Restore{}
	compression := &models.Compression{}
	encryption := &models.Encryption{}
	secretAgent := &models.SecretAgent{}
	awsS3 := &models.AwsS3{}
	gcpStorage := &models.GcpStorage{}
	azureBlob := &models.AzureBlob{}

	config, err := NewRestoreServiceConfig(
		app,
		clientConfig,
		clientPolicy,
		restore,
		compression,
		encryption,
		secretAgent,
		awsS3,
		gcpStorage,
		azureBlob,
	)

	require.NoError(t, err)
	assert.NotNil(t, config)
	assert.Equal(t, app, config.App)
	assert.Equal(t, clientConfig, config.ClientConfig)
	assert.Equal(t, clientPolicy, config.ClientPolicy)
	assert.Equal(t, restore, config.Restore)
	assert.Equal(t, compression, config.Compression)
	assert.Equal(t, encryption, config.Encryption)
	assert.Equal(t, secretAgent, config.SecretAgent)
	assert.Equal(t, awsS3, config.AwsS3)
	assert.Equal(t, gcpStorage, config.GcpStorage)
	assert.Equal(t, azureBlob, config.AzureBlob)
}

func TestRestoreServiceConfig_IsStdin(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		config   *RestoreServiceConfig
		expected bool
	}{
		{
			name: "restore is nil",
			config: &RestoreServiceConfig{
				Restore: nil,
			},
			expected: false,
		},
		{
			name: "input file is StdPlaceholder",
			config: &RestoreServiceConfig{
				Restore: &models.Restore{
					InputFile: StdPlaceholder,
				},
			},
			expected: true,
		},
		{
			name: "input file is regular file path",
			config: &RestoreServiceConfig{
				Restore: &models.Restore{
					InputFile: "/path/to/file",
				},
			},
			expected: false,
		},
		{
			name: "input file is empty string",
			config: &RestoreServiceConfig{
				Restore: &models.Restore{
					InputFile: "",
				},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := tt.config.IsStdin()
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestNewRestoreConfig_DefaultValues(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	serviceConfig := &RestoreServiceConfig{
		Restore:     &models.Restore{},
		Compression: &models.Compression{},
		Encryption:  &models.Encryption{},
		SecretAgent: &models.SecretAgent{},
	}

	config := NewRestoreConfig(serviceConfig, logger)

	require.NotNil(t, config)
	assert.Equal(t, runtime.NumCPU(), config.Parallel)
	assert.True(t, config.MetricsEnabled)
	assert.Equal(t, int64(0), config.Bandwidth)
	assert.False(t, config.NoRecords)
	assert.False(t, config.NoIndexes)
	assert.False(t, config.NoUDFs)
	assert.False(t, config.IgnoreRecordError)
	assert.False(t, config.DisableBatchWrites)
	assert.False(t, config.ValidateOnly)
}

func TestNewRestoreConfig_CustomParallel(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	customParallel := 8

	serviceConfig := &RestoreServiceConfig{
		Restore: &models.Restore{
			Common: models.Common{
				Parallel: customParallel,
			},
		},
		Compression: &models.Compression{},
		Encryption:  &models.Encryption{},
		SecretAgent: &models.SecretAgent{},
	}

	config := NewRestoreConfig(serviceConfig, logger)

	require.NotNil(t, config)
	assert.Equal(t, customParallel, config.Parallel)
}

func TestNewRestoreConfig_BandwidthConversion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		bandwidthMiB      int64
		expectedBandwidth int64
	}{
		{
			name:              "zero bandwidth",
			bandwidthMiB:      0,
			expectedBandwidth: 0,
		},
		{
			name:              "1 MiB bandwidth",
			bandwidthMiB:      1,
			expectedBandwidth: 1024 * 1024,
		},
		{
			name:              "10 MiB bandwidth",
			bandwidthMiB:      10,
			expectedBandwidth: 10 * 1024 * 1024,
		},
		{
			name:              "100 MiB bandwidth",
			bandwidthMiB:      100,
			expectedBandwidth: 100 * 1024 * 1024,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
			serviceConfig := &RestoreServiceConfig{
				Restore: &models.Restore{
					Common: models.Common{
						Bandwidth: tt.bandwidthMiB,
					},
				},
				Compression: &models.Compression{},
				Encryption:  &models.Encryption{},
				SecretAgent: &models.SecretAgent{},
			}

			config := NewRestoreConfig(serviceConfig, logger)

			require.NotNil(t, config)
			assert.Equal(t, tt.expectedBandwidth, config.Bandwidth)
		})
	}
}

func TestNewRestoreConfig_AllFlags(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	serviceConfig := &RestoreServiceConfig{
		Restore: &models.Restore{
			Common: models.Common{
				Namespace:        "test-namespace",
				SetList:          "set1,set2,set3",
				BinList:          "bin1,bin2,bin3",
				NoRecords:        true,
				NoIndexes:        true,
				NoUDFs:           true,
				RecordsPerSecond: 1000,
				Parallel:         4,
				Bandwidth:        50,
			},
			ExtraTTL:           3600,
			IgnoreRecordError:  true,
			DisableBatchWrites: true,
			BatchSize:          128,
			MaxAsyncBatches:    32,
			RetryBaseInterval:  100,
			RetryMultiplier:    2.0,
			RetryMaxAttempts:   5,
			ValidateOnly:       false,
		},
		Compression: &models.Compression{
			Mode:  "zstd",
			Level: 3,
		},
		Encryption: &models.Encryption{
			Mode: "aes256",
		},
		SecretAgent: &models.SecretAgent{
			Address: "localhost",
		},
	}

	config := NewRestoreConfig(serviceConfig, logger)

	require.NotNil(t, config)
	assert.NotNil(t, config.Namespace)
	assert.Len(t, config.SetList, 3)
	assert.Contains(t, config.SetList, "set1")
	assert.Contains(t, config.SetList, "set2")
	assert.Contains(t, config.SetList, "set3")
	assert.Len(t, config.BinList, 3)
	assert.Contains(t, config.BinList, "bin1")
	assert.Contains(t, config.BinList, "bin2")
	assert.Contains(t, config.BinList, "bin3")
	assert.True(t, config.NoRecords)
	assert.True(t, config.NoIndexes)
	assert.True(t, config.NoUDFs)
	assert.Equal(t, 1000, config.RecordsPerSecond)
	assert.Equal(t, 4, config.Parallel)
	assert.Equal(t, int64(50*1024*1024), config.Bandwidth)
	assert.Equal(t, int64(3600), config.ExtraTTL)
	assert.True(t, config.IgnoreRecordError)
	assert.True(t, config.DisableBatchWrites)
	assert.Equal(t, 128, config.BatchSize)
	assert.Equal(t, 32, config.MaxAsyncBatches)
	assert.True(t, config.MetricsEnabled)
	assert.NotNil(t, config.CompressionPolicy)
	assert.NotNil(t, config.EncryptionPolicy)
	assert.NotNil(t, config.SecretAgentConfig)
	assert.NotNil(t, config.RetryPolicy)
	assert.False(t, config.ValidateOnly)
}

func TestNewRestoreConfig_ValidateOnly(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	serviceConfig := &RestoreServiceConfig{
		Restore: &models.Restore{
			ValidateOnly: true,
		},
		Compression: &models.Compression{},
		Encryption:  &models.Encryption{},
		SecretAgent: &models.SecretAgent{},
	}

	config := NewRestoreConfig(serviceConfig, logger)

	require.NotNil(t, config)
	assert.True(t, config.ValidateOnly)
}

func TestGetEncryptionLog(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		encryption *models.Encryption
		expected   string
	}{
		{
			name:       "nil encryption",
			encryption: nil,
			expected:   noneVal,
		},
		{
			name: "empty mode",
			encryption: &models.Encryption{
				Mode: "",
			},
			expected: noneVal,
		},
		{
			name: "none mode lowercase",
			encryption: &models.Encryption{
				Mode: "none",
			},
			expected: noneVal,
		},
		{
			name: "none mode uppercase",
			encryption: &models.Encryption{
				Mode: "NONE",
			},
			expected: noneVal,
		},
		{
			name: "aes128 mode",
			encryption: &models.Encryption{
				Mode: "aes128",
			},
			expected: "aes128",
		},
		{
			name: "aes256 mode",
			encryption: &models.Encryption{
				Mode: "aes256",
			},
			expected: "aes256",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := getEncryptionLog(tt.encryption)
			assert.Equal(t, "encryption", result.Key)
			assert.Equal(t, tt.expected, result.Value.String())
		})
	}
}

func TestGetCompressionLog(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		compression *models.Compression
		expectNone  bool
		expectGroup bool
	}{
		{
			name:        "nil compression",
			compression: nil,
			expectNone:  true,
			expectGroup: false,
		},
		{
			name: "empty mode",
			compression: &models.Compression{
				Mode: "",
			},
			expectNone:  true,
			expectGroup: false,
		},
		{
			name: "none mode lowercase",
			compression: &models.Compression{
				Mode: "none",
			},
			expectNone:  true,
			expectGroup: false,
		},
		{
			name: "none mode uppercase",
			compression: &models.Compression{
				Mode: "NONE",
			},
			expectNone:  true,
			expectGroup: false,
		},
		{
			name: "zstd compression",
			compression: &models.Compression{
				Mode:  "zstd",
				Level: 3,
			},
			expectNone:  false,
			expectGroup: true,
		},
		{
			name: "gzip compression",
			compression: &models.Compression{
				Mode:  "gzip",
				Level: 6,
			},
			expectNone:  false,
			expectGroup: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := getCompressionLog(tt.compression)
			assert.Equal(t, "compression", result.Key)

			if tt.expectNone {
				assert.Equal(t, noneVal, result.Value.String())
			}

			if tt.expectGroup {
				assert.NotNil(t, result.Value)
			}
		})
	}
}

func TestNewRestoreConfig_NilValues(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	serviceConfig := &RestoreServiceConfig{
		Restore:     &models.Restore{},
		Compression: nil,
		Encryption:  nil,
		SecretAgent: nil,
	}

	// Should not panic.
	config := NewRestoreConfig(serviceConfig, logger)
	require.NotNil(t, config)
}

func TestNewRestoreConfig_EmptyStringLists(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	serviceConfig := &RestoreServiceConfig{
		Restore: &models.Restore{
			Common: models.Common{
				SetList: "",
				BinList: "",
			},
		},
		Compression: &models.Compression{},
		Encryption:  &models.Encryption{},
		SecretAgent: &models.SecretAgent{},
	}

	config := NewRestoreConfig(serviceConfig, logger)

	require.NotNil(t, config)
	assert.Nil(t, config.SetList)
	assert.Nil(t, config.BinList)
}

func TestNewRestoreConfig_RetryPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		baseInterval   int64
		multiplier     float64
		maxRetries     uint
		expectedNonNil bool
	}{
		{
			name:           "default retry policy",
			baseInterval:   0,
			multiplier:     0,
			maxRetries:     0,
			expectedNonNil: true,
		},
		{
			name:           "custom retry policy",
			baseInterval:   100,
			multiplier:     2.0,
			maxRetries:     5,
			expectedNonNil: true,
		},
		{
			name:           "high retry values",
			baseInterval:   1000,
			multiplier:     3.0,
			maxRetries:     10,
			expectedNonNil: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
			serviceConfig := &RestoreServiceConfig{
				Restore: &models.Restore{
					RetryBaseInterval: tt.baseInterval,
					RetryMultiplier:   tt.multiplier,
					RetryMaxAttempts:  tt.maxRetries,
				},
				Compression: &models.Compression{},
				Encryption:  &models.Encryption{},
				SecretAgent: &models.SecretAgent{},
			}

			config := NewRestoreConfig(serviceConfig, logger)

			require.NotNil(t, config)

			if tt.expectedNonNil {
				assert.NotNil(t, config.RetryPolicy)
			}
		})
	}
}
