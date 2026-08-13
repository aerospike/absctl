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
	"testing"

	"github.com/aerospike/absctl/internal/models"
	"github.com/aerospike/tools-common-go/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testCompression() *models.Compression {
	return &models.Compression{
		Mode:  "ZSTD",
		Level: 3,
	}
}

func testEncryption() *models.Encryption {
	return &models.Encryption{
		Mode:    "AES256",
		KeyFile: "/path/to/keyfile",
	}
}

func testSecretAgent() *models.SecretAgent {
	return &models.SecretAgent{
		Address:            "localhost",
		ConnectionType:     "tcp",
		Port:               8080,
		TimeoutMillisecond: 1000,
		CaFile:             "/path/to/ca.pem",
		CertFile:           "/path/to/cert.pem",
		KeyFile:            "/path/to/key.pem",
		TLSName:            "example.com",
		IsBase64:           true,
	}
}

func TestNewBackupServiceConfig_WithoutConfigFile(t *testing.T) {
	t.Parallel()

	app := &models.App{ConfigFilePath: ""}
	clientConfig := &client.AerospikeConfig{}
	clientPolicy := &models.ClientPolicy{}
	backupModel := &models.Backup{}
	compression := &models.Compression{}
	encryption := &models.Encryption{}
	secretAgent := &models.SecretAgent{}
	awsS3 := &models.AwsS3{}
	gcpStorage := &models.GcpStorage{}
	azureBlob := &models.AzureBlob{}
	local := &models.Local{}

	config := NewBackupServiceConfig(
		app,
		clientConfig,
		clientPolicy,
		backupModel,
		compression,
		encryption,
		secretAgent,
		awsS3,
		gcpStorage,
		azureBlob,
		local,
	)

	err := config.Validate()
	require.ErrorContains(t, err, "must specify either estimate, output-file or directory")
}

func TestBackupServiceConfig_IsContinue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		config   *BackupServiceConfig
		expected bool
	}{
		{
			name: "backup is nil",
			config: &BackupServiceConfig{
				Backup: nil,
			},
			expected: false,
		},
		{
			name: "continue is empty",
			config: &BackupServiceConfig{
				Backup: &models.Backup{
					Continue: "",
				},
			},
			expected: false,
		},
		{
			name: "continue is not empty",
			config: &BackupServiceConfig{
				Backup: &models.Backup{
					Continue: "state.asb",
				},
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := tt.config.IsContinue()
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestBackupServiceConfig_ShouldInitWriter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		config   *BackupServiceConfig
		expected bool
	}{
		{
			name: "backup is nil",
			config: &BackupServiceConfig{
				Backup: nil,
			},
			expected: true,
		},
		{
			name: "estimate is false",
			config: &BackupServiceConfig{
				Backup: &models.Backup{
					Estimate: false,
				},
			},
			expected: true,
		},
		{
			name: "estimate is true",
			config: &BackupServiceConfig{
				Backup: &models.Backup{
					Estimate: true,
				},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := tt.config.ShouldInitWriter()
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestBackupServiceConfig_IsStdout(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		config   *BackupServiceConfig
		expected bool
	}{
		{
			name: "backup is nil",
			config: &BackupServiceConfig{
				Backup: nil,
			},
			expected: false,
		},
		{
			name: "output file is StdPlaceholder",
			config: &BackupServiceConfig{
				Backup: &models.Backup{
					OutputFile: StdPlaceholder,
				},
			},
			expected: true,
		},
		{
			name: "output file is regular path",
			config: &BackupServiceConfig{
				Backup: &models.Backup{
					OutputFile: "/path/to/file",
				},
			},
			expected: false,
		},
		{
			name: "output file is empty string",
			config: &BackupServiceConfig{
				Backup: &models.Backup{
					OutputFile: "",
				},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := tt.config.IsStdout()
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestNewBackupConfig_RegularBackup(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	serviceConfig := &BackupServiceConfig{
		Backup: &models.Backup{
			Common: models.Common{
				Namespace: "test-namespace",
				Parallel:  4,
			},
		},
		ServiceConfigCommon: ServiceConfigCommon{
			Compression: &models.Compression{},
			Encryption:  &models.Encryption{},
			SecretAgent: &models.SecretAgent{},
		},
	}

	backupConfig, err := NewBackupConfig(serviceConfig, logger)

	require.NoError(t, err)
	assert.NotNil(t, backupConfig)
	assert.Equal(t, "test-namespace", backupConfig.Namespace)
	assert.Equal(t, 4, backupConfig.ParallelRead)
	assert.Equal(t, 4, backupConfig.ParallelWrite)
	assert.True(t, backupConfig.MetricsEnabled)
}

func TestNewBackupConfig_BandwidthConversion(t *testing.T) {
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
			name:              "50 MiB bandwidth",
			bandwidthMiB:      50,
			expectedBandwidth: 50 * 1024 * 1024,
		},
		{
			name:              "1000 MiB bandwidth",
			bandwidthMiB:      1000,
			expectedBandwidth: 1000 * 1024 * 1024,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
			serviceConfig := &BackupServiceConfig{
				Backup: &models.Backup{
					Common: models.Common{
						Bandwidth: tt.bandwidthMiB,
					},
				},
				ServiceConfigCommon: ServiceConfigCommon{
					Compression: &models.Compression{},
					Encryption:  &models.Encryption{},
					SecretAgent: &models.SecretAgent{},
				},
			}

			backupConfig, err := NewBackupConfig(serviceConfig, logger)

			require.NoError(t, err)
			assert.NotNil(t, backupConfig)
			assert.Equal(t, tt.expectedBandwidth, backupConfig.Bandwidth)
		})
	}
}

func TestNewBackupConfig_StdoutConfiguration(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	serviceConfig := &BackupServiceConfig{
		Backup: &models.Backup{
			OutputFile: StdPlaceholder,
			FileLimit:  1000,
			Common: models.Common{
				Parallel: 8,
			},
		},
		ServiceConfigCommon: ServiceConfigCommon{
			Compression: &models.Compression{},
			Encryption:  &models.Encryption{},
			SecretAgent: &models.SecretAgent{},
		},
	}

	backupConfig, err := NewBackupConfig(serviceConfig, logger)

	require.NoError(t, err)
	assert.NotNil(t, backupConfig)
	assert.Equal(t, uint64(0), backupConfig.FileLimit)
	assert.Equal(t, 1, backupConfig.ParallelWrite)
	assert.Equal(t, 8, backupConfig.ParallelRead)
}

func TestNewBackupConfig_AllFlags(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	serviceConfig := &BackupServiceConfig{
		Backup: &models.Backup{
			Common: models.Common{
				Namespace:        "test-namespace",
				SetList:          "set1,set2,set3",
				BinList:          "bin1,bin2,bin3",
				NoRecords:        true,
				NoIndexes:        true,
				NoUDFs:           true,
				RecordsPerSecond: 5000,
				Parallel:         8,
				Bandwidth:        100,
				Directory:        "/tmp",
			},
			FileLimit:        100,
			Compact:          true,
			NoTTLOnly:        true,
			OutputFilePrefix: "backup-",
			StateFileDst:     "state.asb",
			ScanPageSize:     10000,
		},
		ServiceConfigCommon: ServiceConfigCommon{
			Compression: &models.Compression{
				Mode:  "zstd",
				Level: 3,
			},
			Encryption: &models.Encryption{
				Mode: "aes256",
			},
			SecretAgent: &models.SecretAgent{},
		},
	}

	backupConfig, err := NewBackupConfig(serviceConfig, logger)

	require.NoError(t, err)
	assert.NotNil(t, backupConfig)
	assert.Equal(t, "test-namespace", backupConfig.Namespace)
	assert.Len(t, backupConfig.SetList, 3)
	assert.Contains(t, backupConfig.SetList, "set1")
	assert.Contains(t, backupConfig.SetList, "set2")
	assert.Contains(t, backupConfig.SetList, "set3")
	assert.Len(t, backupConfig.BinList, 3)
	assert.Contains(t, backupConfig.BinList, "bin1")
	assert.True(t, backupConfig.NoRecords)
	assert.True(t, backupConfig.NoIndexes)
	assert.True(t, backupConfig.NoUDFs)
	assert.Equal(t, 5000, backupConfig.RecordsPerSecond)
	assert.Equal(t, uint64(100*1024*1024), backupConfig.FileLimit)
	assert.Equal(t, 8, backupConfig.ParallelRead)
	assert.Equal(t, 8, backupConfig.ParallelWrite)
	assert.Equal(t, int64(100*1024*1024), backupConfig.Bandwidth)
	assert.True(t, backupConfig.Compact)
	assert.True(t, backupConfig.NoTTLOnly)
	assert.Equal(t, "backup-", backupConfig.OutputFilePrefix)
	assert.Equal(t, "/tmp/state.asb", backupConfig.StateFile)
	assert.Equal(t, int64(10000), backupConfig.PageSize)
	assert.True(t, backupConfig.MetricsEnabled)
}

func TestNewBackupConfig_ContinueBackup(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	serviceConfig := &BackupServiceConfig{
		Backup: &models.Backup{
			Common: models.Common{
				Directory: "/backup/dir",
			},
			Continue:     "continue.state",
			ScanPageSize: 5000,
		},
		ServiceConfigCommon: ServiceConfigCommon{
			Compression: &models.Compression{},
			Encryption:  &models.Encryption{},
			SecretAgent: &models.SecretAgent{},
		},
	}

	backupConfig, err := NewBackupConfig(serviceConfig, logger)

	require.NoError(t, err)
	assert.NotNil(t, backupConfig)
	assert.Equal(t, "/backup/dir/continue.state", backupConfig.StateFile)
	assert.True(t, backupConfig.Continue)
	assert.Equal(t, int64(5000), backupConfig.PageSize)
}

func TestNewBackupConfig_ParallelNodes(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	serviceConfig := &BackupServiceConfig{
		Backup: &models.Backup{
			NodeList: "node1,node2,node3",
		},
		ServiceConfigCommon: ServiceConfigCommon{
			Compression: &models.Compression{},
			Encryption:  &models.Encryption{},
			SecretAgent: &models.SecretAgent{},
		},
	}

	backupConfig, err := NewBackupConfig(serviceConfig, logger)

	require.NoError(t, err)
	assert.NotNil(t, backupConfig)
	assert.Len(t, backupConfig.NodeList, 3)
	assert.Contains(t, backupConfig.NodeList, "node1")
	assert.Contains(t, backupConfig.NodeList, "node2")
	assert.Contains(t, backupConfig.NodeList, "node3")
}

func TestNewBackupConfig_RackList(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	serviceConfig := &BackupServiceConfig{
		Backup: &models.Backup{
			RackList: "1,2,3",
		},
		ServiceConfigCommon: ServiceConfigCommon{
			Compression: &models.Compression{},
			Encryption:  &models.Encryption{},
			SecretAgent: &models.SecretAgent{},
		},
	}

	backupConfig, err := NewBackupConfig(serviceConfig, logger)

	require.NoError(t, err)
	assert.NotNil(t, backupConfig)
	assert.NotNil(t, backupConfig.RackList)
	assert.Len(t, backupConfig.RackList, 3)
}

func TestNewBackupConfig_InvalidRackList(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	serviceConfig := &BackupServiceConfig{
		Backup: &models.Backup{
			RackList: "invalid,rack,list",
		},
		ServiceConfigCommon: ServiceConfigCommon{
			Compression: &models.Compression{},
			Encryption:  &models.Encryption{},
			SecretAgent: &models.SecretAgent{},
		},
	}

	_, err := NewBackupConfig(serviceConfig, logger)

	assert.Error(t, err)
}

func TestNewBackupConfig_ModifiedBefore(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	serviceConfig := &BackupServiceConfig{
		Backup: &models.Backup{
			ModifiedBefore: "2024-01-01",
		},
		ServiceConfigCommon: ServiceConfigCommon{
			Compression: &models.Compression{},
			Encryption:  &models.Encryption{},
			SecretAgent: &models.SecretAgent{},
		},
	}

	backupConfig, err := NewBackupConfig(serviceConfig, logger)

	require.NoError(t, err)
	assert.NotNil(t, backupConfig)
	assert.NotNil(t, backupConfig.ModBefore)
}

func TestNewBackupConfig_ModifiedAfter(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	serviceConfig := &BackupServiceConfig{
		Backup: &models.Backup{
			ModifiedAfter: "2024-01-01",
		},
		ServiceConfigCommon: ServiceConfigCommon{
			Compression: &models.Compression{},
			Encryption:  &models.Encryption{},
			SecretAgent: &models.SecretAgent{},
		},
	}

	backupConfig, err := NewBackupConfig(serviceConfig, logger)

	require.NoError(t, err)
	assert.NotNil(t, backupConfig)
	assert.NotNil(t, backupConfig.ModAfter)
}

func TestNewBackupConfig_InvalidModifiedBefore(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	serviceConfig := &BackupServiceConfig{
		Backup: &models.Backup{
			ModifiedBefore: "invalid-date",
		},
		ServiceConfigCommon: ServiceConfigCommon{
			Compression: &models.Compression{},
			Encryption:  &models.Encryption{},
			SecretAgent: &models.SecretAgent{},
		},
	}

	_, err := NewBackupConfig(serviceConfig, logger)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse modified before date")
}

func TestNewBackupConfig_InvalidModifiedAfter(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	serviceConfig := &BackupServiceConfig{
		Backup: &models.Backup{
			ModifiedAfter: "invalid-date",
		},
		ServiceConfigCommon: ServiceConfigCommon{
			Compression: &models.Compression{},
			Encryption:  &models.Encryption{},
			SecretAgent: &models.SecretAgent{},
		},
	}

	_, err := NewBackupConfig(serviceConfig, logger)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse modified after date")
}

func TestNewBackupConfig_EmptyStringLists(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	serviceConfig := &BackupServiceConfig{
		Backup: &models.Backup{
			Common: models.Common{
				SetList: "",
				BinList: "",
			},
			NodeList: "",
		},
		ServiceConfigCommon: ServiceConfigCommon{
			Compression: &models.Compression{},
			Encryption:  &models.Encryption{},
			SecretAgent: &models.SecretAgent{},
		},
	}

	backupConfig, err := NewBackupConfig(serviceConfig, logger)

	require.NoError(t, err)
	assert.NotNil(t, backupConfig)
	assert.Nil(t, backupConfig.SetList)
	assert.Nil(t, backupConfig.BinList)
}

func TestNewBackupConfig_NilValues(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	serviceConfig := &BackupServiceConfig{
		Backup: &models.Backup{},
		ServiceConfigCommon: ServiceConfigCommon{
			Compression: nil,
			Encryption:  nil,
			SecretAgent: nil,
		},
	}

	// Should not panic.
	backupConfig, err := NewBackupConfig(serviceConfig, logger)

	require.NoError(t, err)
	assert.NotNil(t, backupConfig)
}

func TestConstants(t *testing.T) {
	t.Parallel()

	assert.Equal(t, 1000000, MaxRack)
	assert.Equal(t, "-", StdPlaceholder)
}

func TestMapBackupConfig_Success(t *testing.T) {
	t.Parallel()

	params := &BackupServiceConfig{
		Backup: &models.Backup{
			FileLimit:        5000,
			AfterDigest:      "AvDsV2KuSZHZugDBftnLxGpR+88=",
			ModifiedBefore:   "2023-09-01_12:00:00",
			ModifiedAfter:    "2023-09-02_12:00:00",
			FilterExpression: "k1EDpHRlc3Q=",
			Compact:          true,
			NodeList:         "node1,node2",
			NoTTLOnly:        true,
			Common: models.Common{
				Namespace:        "test-namespace",
				SetList:          "set1,set2",
				BinList:          "bin1,bin2",
				NoRecords:        true,
				NoIndexes:        false,
				RecordsPerSecond: 1000,
				Bandwidth:        10,
				Parallel:         5,
			},
		},
		ServiceConfigCommon: ServiceConfigCommon{
			App:         &models.App{},
			Compression: testCompression(),
			Encryption:  testEncryption(),
			SecretAgent: testSecretAgent(),
		},
	}

	config, err := newBackupConfig(params)
	require.NoError(t, err)

	assert.Equal(t, "test-namespace", config.Namespace)
	assert.ElementsMatch(t, []string{"set1", "set2"}, config.SetList)
	assert.ElementsMatch(t, []string{"bin1", "bin2"}, config.BinList)
	assert.True(t, config.NoRecords)
	assert.False(t, config.NoIndexes)
	assert.Equal(t, 1000, config.RecordsPerSecond)
	assert.Equal(t, uint64(5000*1024*1024), config.FileLimit)
	assert.True(t, config.NoTTLOnly)

	modBefore, err := models.ParseLocalTimeToUTC("2023-09-01_12:00:00")
	require.NoError(t, err)
	modAfter, err := models.ParseLocalTimeToUTC("2023-09-02_12:00:00")
	require.NoError(t, err)
	assert.Equal(t, modBefore, *config.ModBefore)
	assert.Equal(t, modAfter, *config.ModAfter)

	assert.NotNil(t, config.CompressionPolicy)
	assert.Equal(t, "ZSTD", config.CompressionPolicy.Mode)
	assert.Equal(t, 3, config.CompressionPolicy.Level)

	assert.NotNil(t, config.EncryptionPolicy)
	assert.Equal(t, "AES256", config.EncryptionPolicy.Mode)
	assert.Equal(t, "/path/to/keyfile", *config.EncryptionPolicy.KeyFile)

	assert.NotNil(t, config.SecretAgentConfig)
	assert.Equal(t, "localhost", *config.SecretAgentConfig.Address)
	assert.Equal(t, "tcp", *config.SecretAgentConfig.ConnectionType)
	assert.Equal(t, 8080, *config.SecretAgentConfig.Port)

	assert.Equal(t, 5, config.ParallelWrite, "The ParallelWrite should be set correctly")
	assert.Equal(t, 5, config.ParallelRead, "The ParallelRead should be set correctly")
	assert.Equal(t, int64(10*1024*1024), config.Bandwidth, "The Bandwidth should be set to 10 MiB in bytes")
	assert.True(t, config.Compact, "The Compact flag should be set correctly")
	assert.ElementsMatch(t, []string{"node1", "node2"}, config.NodeList, "The NodeList should be set correctly")
}

func TestMapBackupConfig_InvalidModifiedBefore(t *testing.T) {
	t.Parallel()

	params := &BackupServiceConfig{
		Backup: &models.Backup{
			ModifiedBefore: "invalid-date",
			Common: models.Common{
				Namespace: "test-namespace",
			},
		},
		ServiceConfigCommon: ServiceConfigCommon{
			App:         &models.App{},
			Compression: testCompression(),
			Encryption:  testEncryption(),
			SecretAgent: testSecretAgent(),
		},
	}

	config, err := newBackupConfig(params)
	require.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "failed to parse modified before date")
}

func TestMapBackupConfig_InvalidModifiedAfter(t *testing.T) {
	t.Parallel()

	params := &BackupServiceConfig{
		Backup: &models.Backup{
			ModifiedAfter: "invalid-date",
			Common: models.Common{
				Namespace: "test-namespace",
			},
		},
		ServiceConfigCommon: ServiceConfigCommon{
			App:         &models.App{},
			Compression: testCompression(),
			Encryption:  testEncryption(),
			SecretAgent: testSecretAgent(),
		},
	}

	config, err := newBackupConfig(params)
	require.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "failed to parse modified after date")
}

func TestMapBackupConfig_InvalidExpression(t *testing.T) {
	t.Parallel()

	params := &BackupServiceConfig{
		Backup: &models.Backup{
			FilterExpression: "invalid-exp",
			Common: models.Common{
				Namespace: "test-namespace",
			},
		},
		ServiceConfigCommon: ServiceConfigCommon{
			App:         &models.App{},
			Compression: testCompression(),
			Encryption:  testEncryption(),
			SecretAgent: testSecretAgent(),
		},
	}

	config, err := newBackupConfig(params)
	require.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "failed to parse filter expression")
}
