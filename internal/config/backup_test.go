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
	"time"

	"github.com/aerospike/absctl/internal/models"
	"github.com/aerospike/backup-go"
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
	backupXDRModel := &models.BackupXDR{}
	compression := &models.Compression{}
	encryption := &models.Encryption{}
	secretAgent := &models.SecretAgent{}
	awsS3 := &models.AwsS3{}
	gcpStorage := &models.GcpStorage{}
	azureBlob := &models.AzureBlob{}
	local := &models.Local{}

	config, err := NewBackupServiceConfig(
		app,
		clientConfig,
		clientPolicy,
		backupModel,
		backupXDRModel,
		compression,
		encryption,
		secretAgent,
		awsS3,
		gcpStorage,
		azureBlob,
		local,
	)

	require.NoError(t, err)
	assert.NotNil(t, config)
	assert.Equal(t, app, config.App)
	assert.Equal(t, clientConfig, config.ClientConfig)
	assert.Equal(t, clientPolicy, config.ClientPolicy)
	assert.Equal(t, backupModel, config.Backup)
	assert.Equal(t, backupXDRModel, config.BackupXDR)
	assert.Equal(t, compression, config.Compression)
	assert.Equal(t, encryption, config.Encryption)
	assert.Equal(t, secretAgent, config.SecretAgent)
	assert.Equal(t, awsS3, config.AwsS3)
	assert.Equal(t, gcpStorage, config.GcpStorage)
	assert.Equal(t, azureBlob, config.AzureBlob)
}

func TestBackupServiceConfig_IsXDR(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		config   *BackupServiceConfig
		expected bool
	}{
		{
			name: "both nil",
			config: &BackupServiceConfig{
				BackupXDR: nil,
				Backup:    nil,
			},
			expected: false,
		},
		{
			name: "backupXDR not nil, backup nil",
			config: &BackupServiceConfig{
				BackupXDR: &models.BackupXDR{},
				Backup:    nil,
			},
			expected: true,
		},
		{
			name: "backupXDR nil, backup not nil",
			config: &BackupServiceConfig{
				BackupXDR: nil,
				Backup:    &models.Backup{},
			},
			expected: false,
		},
		{
			name: "both not nil",
			config: &BackupServiceConfig{
				BackupXDR: &models.BackupXDR{},
				Backup:    &models.Backup{},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := tt.config.IsXDR()
			assert.Equal(t, tt.expected, result)
		})
	}
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

func TestBackupServiceConfig_IsStopXDR(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		config   *BackupServiceConfig
		expected bool
	}{
		{
			name: "backupXDR is nil",
			config: &BackupServiceConfig{
				BackupXDR: nil,
			},
			expected: false,
		},
		{
			name: "stopXDR is false",
			config: &BackupServiceConfig{
				BackupXDR: &models.BackupXDR{
					StopXDR: false,
				},
			},
			expected: false,
		},
		{
			name: "stopXDR is true",
			config: &BackupServiceConfig{
				BackupXDR: &models.BackupXDR{
					StopXDR: true,
				},
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := tt.config.IsStopXDR()
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestBackupServiceConfig_IsUnblockMRT(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		config   *BackupServiceConfig
		expected bool
	}{
		{
			name: "backupXDR is nil",
			config: &BackupServiceConfig{
				BackupXDR: nil,
			},
			expected: false,
		},
		{
			name: "unblockMRT is false",
			config: &BackupServiceConfig{
				BackupXDR: &models.BackupXDR{
					UnblockMRT: false,
				},
			},
			expected: false,
		},
		{
			name: "unblockMRT is true",
			config: &BackupServiceConfig{
				BackupXDR: &models.BackupXDR{
					UnblockMRT: true,
				},
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := tt.config.IsUnblockMRT()
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestBackupServiceConfig_SkipWriterInit(t *testing.T) {
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

			result := tt.config.SkipWriterInit()
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

func TestNewBackupConfigs_RegularBackup(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	serviceConfig := &BackupServiceConfig{
		Backup: &models.Backup{
			Common: models.Common{
				Namespace: "test-namespace",
				Parallel:  4,
			},
		},
		BackupXDR:   nil,
		Compression: &models.Compression{},
		Encryption:  &models.Encryption{},
		SecretAgent: &models.SecretAgent{},
	}

	backupConfig, xdrConfig, err := NewBackupConfigs(serviceConfig, logger)

	require.NoError(t, err)
	assert.NotNil(t, backupConfig)
	assert.Nil(t, xdrConfig)
	assert.Equal(t, "test-namespace", backupConfig.Namespace)
	assert.Equal(t, 4, backupConfig.ParallelRead)
	assert.Equal(t, 4, backupConfig.ParallelWrite)
	assert.True(t, backupConfig.MetricsEnabled)
}

func TestNewBackupConfigs_XDRBackup(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	serviceConfig := &BackupServiceConfig{
		Backup: nil,
		BackupXDR: &models.BackupXDR{
			Namespace:     "test-namespace",
			ParallelWrite: 4,
		},
		Compression: &models.Compression{},
		Encryption:  &models.Encryption{},
		SecretAgent: &models.SecretAgent{},
	}

	backupConfig, xdrConfig, err := NewBackupConfigs(serviceConfig, logger)

	require.NoError(t, err)
	assert.NotNil(t, backupConfig)
	assert.NotNil(t, xdrConfig)
	assert.True(t, backupConfig.NoRecords)
	assert.Equal(t, "test-namespace", backupConfig.Namespace)
	assert.Equal(t, "test-namespace", xdrConfig.Namespace)
	assert.Equal(t, 4, xdrConfig.ParallelWrite)
	assert.True(t, xdrConfig.MetricsEnabled)
}

func TestNewBackupConfigs_BandwidthConversion(t *testing.T) {
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
				Compression: &models.Compression{},
				Encryption:  &models.Encryption{},
				SecretAgent: &models.SecretAgent{},
			}

			backupConfig, _, err := NewBackupConfigs(serviceConfig, logger)

			require.NoError(t, err)
			assert.NotNil(t, backupConfig)
			assert.Equal(t, tt.expectedBandwidth, backupConfig.Bandwidth)
		})
	}
}

func TestNewBackupConfigs_StdoutConfiguration(t *testing.T) {
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
		Compression: &models.Compression{},
		Encryption:  &models.Encryption{},
		SecretAgent: &models.SecretAgent{},
	}

	backupConfig, _, err := NewBackupConfigs(serviceConfig, logger)

	require.NoError(t, err)
	assert.NotNil(t, backupConfig)
	assert.Equal(t, uint64(0), backupConfig.FileLimit)
	assert.Equal(t, 1, backupConfig.ParallelWrite)
	assert.Equal(t, 8, backupConfig.ParallelRead)
}

func TestNewBackupConfigs_AllFlags(t *testing.T) {
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
		Compression: &models.Compression{
			Mode:  "zstd",
			Level: 3,
		},
		Encryption: &models.Encryption{
			Mode: "aes256",
		},
		SecretAgent: &models.SecretAgent{},
	}

	backupConfig, _, err := NewBackupConfigs(serviceConfig, logger)

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

func TestNewBackupConfigs_ContinueBackup(t *testing.T) {
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
		Compression: &models.Compression{},
		Encryption:  &models.Encryption{},
		SecretAgent: &models.SecretAgent{},
	}

	backupConfig, _, err := NewBackupConfigs(serviceConfig, logger)

	require.NoError(t, err)
	assert.NotNil(t, backupConfig)
	assert.Equal(t, "/backup/dir/continue.state", backupConfig.StateFile)
	assert.True(t, backupConfig.Continue)
	assert.Equal(t, int64(5000), backupConfig.PageSize)
}

func TestNewBackupConfigs_ParallelNodes(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	serviceConfig := &BackupServiceConfig{
		Backup: &models.Backup{
			NodeList: "node1,node2,node3",
		},
		Compression: &models.Compression{},
		Encryption:  &models.Encryption{},
		SecretAgent: &models.SecretAgent{},
	}

	backupConfig, _, err := NewBackupConfigs(serviceConfig, logger)

	require.NoError(t, err)
	assert.NotNil(t, backupConfig)
	assert.Len(t, backupConfig.NodeList, 3)
	assert.Contains(t, backupConfig.NodeList, "node1")
	assert.Contains(t, backupConfig.NodeList, "node2")
	assert.Contains(t, backupConfig.NodeList, "node3")
}

func TestNewBackupConfigs_RackList(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	serviceConfig := &BackupServiceConfig{
		Backup: &models.Backup{
			RackList: "1,2,3",
		},
		Compression: &models.Compression{},
		Encryption:  &models.Encryption{},
		SecretAgent: &models.SecretAgent{},
	}

	backupConfig, _, err := NewBackupConfigs(serviceConfig, logger)

	require.NoError(t, err)
	assert.NotNil(t, backupConfig)
	assert.NotNil(t, backupConfig.RackList)
	assert.Len(t, backupConfig.RackList, 3)
}

func TestNewBackupConfigs_InvalidRackList(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	serviceConfig := &BackupServiceConfig{
		Backup: &models.Backup{
			RackList: "invalid,rack,list",
		},
		Compression: &models.Compression{},
		Encryption:  &models.Encryption{},
		SecretAgent: &models.SecretAgent{},
	}

	_, _, err := NewBackupConfigs(serviceConfig, logger)

	assert.Error(t, err)
}

func TestNewBackupConfigs_ModifiedBefore(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	serviceConfig := &BackupServiceConfig{
		Backup: &models.Backup{
			ModifiedBefore: "2024-01-01",
		},
		Compression: &models.Compression{},
		Encryption:  &models.Encryption{},
		SecretAgent: &models.SecretAgent{},
	}

	backupConfig, _, err := NewBackupConfigs(serviceConfig, logger)

	require.NoError(t, err)
	assert.NotNil(t, backupConfig)
	assert.NotNil(t, backupConfig.ModBefore)
}

func TestNewBackupConfigs_ModifiedAfter(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	serviceConfig := &BackupServiceConfig{
		Backup: &models.Backup{
			ModifiedAfter: "2024-01-01",
		},
		Compression: &models.Compression{},
		Encryption:  &models.Encryption{},
		SecretAgent: &models.SecretAgent{},
	}

	backupConfig, _, err := NewBackupConfigs(serviceConfig, logger)

	require.NoError(t, err)
	assert.NotNil(t, backupConfig)
	assert.NotNil(t, backupConfig.ModAfter)
}

func TestNewBackupConfigs_InvalidModifiedBefore(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	serviceConfig := &BackupServiceConfig{
		Backup: &models.Backup{
			ModifiedBefore: "invalid-date",
		},
		Compression: &models.Compression{},
		Encryption:  &models.Encryption{},
		SecretAgent: &models.SecretAgent{},
	}

	_, _, err := NewBackupConfigs(serviceConfig, logger)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse modified before date")
}

func TestNewBackupConfigs_InvalidModifiedAfter(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	serviceConfig := &BackupServiceConfig{
		Backup: &models.Backup{
			ModifiedAfter: "invalid-date",
		},
		Compression: &models.Compression{},
		Encryption:  &models.Encryption{},
		SecretAgent: &models.SecretAgent{},
	}

	_, _, err := NewBackupConfigs(serviceConfig, logger)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse modified after date")
}

func TestNewBackupConfigs_XDRDefaultParallelWrite(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	serviceConfig := &BackupServiceConfig{
		Backup: nil,
		BackupXDR: &models.BackupXDR{
			Namespace:     "test-namespace",
			ParallelWrite: 0,
		},
		Compression: &models.Compression{},
		Encryption:  &models.Encryption{},
		SecretAgent: &models.SecretAgent{},
	}

	_, xdrConfig, err := NewBackupConfigs(serviceConfig, logger)

	require.NoError(t, err)
	assert.NotNil(t, xdrConfig)
	assert.Equal(t, runtime.NumCPU(), xdrConfig.ParallelWrite)
}

func TestNewBackupConfigs_XDRTimeouts(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	serviceConfig := &BackupServiceConfig{
		Backup: nil,
		BackupXDR: &models.BackupXDR{
			Namespace:                    "test-namespace",
			ReadTimeoutMilliseconds:      5000,
			WriteTimeoutMilliseconds:     3000,
			InfoPolingPeriodMilliseconds: 1000,
			StartTimeoutMilliseconds:     10000,
		},
		Compression: &models.Compression{},
		Encryption:  &models.Encryption{},
		SecretAgent: &models.SecretAgent{},
	}

	_, xdrConfig, err := NewBackupConfigs(serviceConfig, logger)

	require.NoError(t, err)
	assert.NotNil(t, xdrConfig)
	assert.Equal(t, 5000*time.Millisecond, xdrConfig.ReadTimeout)
	assert.Equal(t, 3000*time.Millisecond, xdrConfig.WriteTimeout)
	assert.Equal(t, 1000*time.Millisecond, xdrConfig.InfoPollingPeriod)
	assert.Equal(t, 10000*time.Millisecond, xdrConfig.StartTimeout)
}

func TestNewBackupConfigs_XDRAllFields(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	serviceConfig := &BackupServiceConfig{
		Backup: nil,
		BackupXDR: &models.BackupXDR{
			Namespace:       "test-namespace",
			ParallelWrite:   8,
			FileLimit:       200,
			DC:              "dc1",
			LocalAddress:    "127.0.0.1",
			LocalPort:       3000,
			Rewind:          "2024-01-01",
			ResultQueueSize: 100,
			AckQueueSize:    50,
			MaxConnections:  64,
			MaxThroughput:   1000,
			Forward:         true,
		},
		Compression: &models.Compression{},
		Encryption:  &models.Encryption{},
		SecretAgent: &models.SecretAgent{},
	}

	_, xdrConfig, err := NewBackupConfigs(serviceConfig, logger)

	require.NoError(t, err)
	assert.NotNil(t, xdrConfig)
	assert.Equal(t, "test-namespace", xdrConfig.Namespace)
	assert.Equal(t, 8, xdrConfig.ParallelWrite)
	assert.Equal(t, uint64(200*1024*1024), xdrConfig.FileLimit)
	assert.Equal(t, "dc1", xdrConfig.DC)
	assert.Equal(t, "127.0.0.1", xdrConfig.LocalAddress)
	assert.Equal(t, 3000, xdrConfig.LocalPort)
	assert.Equal(t, "2024-01-01", xdrConfig.Rewind)
	assert.Equal(t, 100, xdrConfig.ResultQueueSize)
	assert.Equal(t, 50, xdrConfig.AckQueueSize)
	assert.Equal(t, 64, xdrConfig.MaxConnections)
	assert.Equal(t, 1000, xdrConfig.MaxThroughput)
	assert.True(t, xdrConfig.Forward)
	assert.True(t, xdrConfig.MetricsEnabled)
	assert.Equal(t, backup.EncoderTypeASBX, xdrConfig.EncoderType)
}

func TestNewBackupConfigs_EmptyStringLists(t *testing.T) {
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
		Compression: &models.Compression{},
		Encryption:  &models.Encryption{},
		SecretAgent: &models.SecretAgent{},
	}

	backupConfig, _, err := NewBackupConfigs(serviceConfig, logger)

	require.NoError(t, err)
	assert.NotNil(t, backupConfig)
	assert.Nil(t, backupConfig.SetList)
	assert.Nil(t, backupConfig.BinList)
}

func TestNewBackupConfigs_NilValues(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	serviceConfig := &BackupServiceConfig{
		Backup:      &models.Backup{},
		Compression: nil,
		Encryption:  nil,
		SecretAgent: nil,
	}

	// Should not panic.
	backupConfig, _, err := NewBackupConfigs(serviceConfig, logger)

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
		App: &models.App{},
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
		Compression: testCompression(),
		Encryption:  testEncryption(),
		SecretAgent: testSecretAgent(),
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

	modBefore := time.Date(2023, 9, 1, 10, 0, 0, 0, time.UTC)
	modAfter := time.Date(2023, 9, 2, 10, 0, 0, 0, time.UTC)
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
		App: &models.App{},
		Backup: &models.Backup{
			ModifiedBefore: "invalid-date",
			Common: models.Common{
				Namespace: "test-namespace",
			},
		},
		Compression: testCompression(),
		Encryption:  testEncryption(),
		SecretAgent: testSecretAgent(),
	}

	config, err := newBackupConfig(params)
	require.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "failed to parse modified before date")
}

func TestMapBackupConfig_InvalidModifiedAfter(t *testing.T) {
	t.Parallel()

	params := &BackupServiceConfig{
		App: &models.App{},
		Backup: &models.Backup{
			ModifiedAfter: "invalid-date",
			Common: models.Common{
				Namespace: "test-namespace",
			},
		},
		Compression: testCompression(),
		Encryption:  testEncryption(),
		SecretAgent: testSecretAgent(),
	}

	config, err := newBackupConfig(params)
	require.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "failed to parse modified after date")
}

func TestMapBackupConfig_InvalidExpression(t *testing.T) {
	t.Parallel()

	params := &BackupServiceConfig{
		App: &models.App{},
		Backup: &models.Backup{
			FilterExpression: "invalid-exp",
			Common: models.Common{
				Namespace: "test-namespace",
			},
		},
		Compression: testCompression(),
		Encryption:  testEncryption(),
		SecretAgent: testSecretAgent(),
	}

	config, err := newBackupConfig(params)
	require.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "failed to parse filter expression")
}

func TestMapBackupXDRConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		params *BackupServiceConfig
		verify func(*testing.T, *backup.ConfigBackupXDR)
	}{
		{
			name: "Default configuration",
			params: &BackupServiceConfig{
				App: &models.App{},
				BackupXDR: &models.BackupXDR{
					DC:            "dc1",
					LocalAddress:  "127.0.0.1",
					LocalPort:     3004,
					Namespace:     "test",
					MaxThroughput: 10,
				},
				Compression: testCompression(),
				Encryption:  testEncryption(),
				SecretAgent: testSecretAgent(),
			},
			verify: func(t *testing.T, cfg *backup.ConfigBackupXDR) {
				t.Helper()
				assert.Equal(t, "dc1", cfg.DC)
				assert.Equal(t, "127.0.0.1", cfg.LocalAddress)
				assert.Equal(t, 3004, cfg.LocalPort)
				assert.Equal(t, "test", cfg.Namespace)
				assert.Equal(t, backup.EncoderTypeASBX, cfg.EncoderType)

				// Verify compression policy
				assert.NotNil(t, cfg.CompressionPolicy)
				assert.Equal(t, "ZSTD", cfg.CompressionPolicy.Mode)
				assert.Equal(t, 3, cfg.CompressionPolicy.Level)

				// Verify encryption policy
				assert.NotNil(t, cfg.EncryptionPolicy)
				assert.Equal(t, "AES256", cfg.EncryptionPolicy.Mode)
				assert.Equal(t, "/path/to/keyfile", *cfg.EncryptionPolicy.KeyFile)

				// Verify secret agent config
				assert.NotNil(t, cfg.SecretAgentConfig)
				assert.Equal(t, "localhost", *cfg.SecretAgentConfig.Address)
				assert.Equal(t, "tcp", *cfg.SecretAgentConfig.ConnectionType)
				assert.Equal(t, 8080, *cfg.SecretAgentConfig.Port)

				assert.Equal(t, 10, cfg.MaxThroughput)
			},
		},
		{
			name: "Full configuration with all parameters",
			params: &BackupServiceConfig{
				App: &models.App{},
				BackupXDR: &models.BackupXDR{
					DC:                           "dc1",
					LocalAddress:                 "127.0.0.1",
					LocalPort:                    3004,
					Namespace:                    "test",
					FileLimit:                    1000,
					ParallelWrite:                4,
					Rewind:                       "1h",
					ReadTimeoutMilliseconds:      5000,
					WriteTimeoutMilliseconds:     5000,
					ResultQueueSize:              1000,
					AckQueueSize:                 1000,
					MaxConnections:               100,
					InfoPolingPeriodMilliseconds: 1000,
				},
				Compression: testCompression(),
				Encryption:  testEncryption(),
				SecretAgent: testSecretAgent(),
			},
			verify: func(t *testing.T, cfg *backup.ConfigBackupXDR) {
				t.Helper()
				assert.Equal(t, uint64(1000*1024*1024), cfg.FileLimit)
				assert.Equal(t, 4, cfg.ParallelWrite)
				assert.Equal(t, "1h", cfg.Rewind)
				assert.Equal(t, time.Duration(5000)*time.Millisecond, cfg.ReadTimeout)
				assert.Equal(t, time.Duration(5000)*time.Millisecond, cfg.WriteTimeout)
				assert.Equal(t, 1000, cfg.ResultQueueSize)
				assert.Equal(t, 1000, cfg.AckQueueSize)
				assert.Equal(t, 100, cfg.MaxConnections)
				assert.Equal(t, time.Duration(1000)*time.Millisecond, cfg.InfoPollingPeriod)
			},
		},
		{
			name: "Configuration without optional policies",
			params: &BackupServiceConfig{
				App: &models.App{},
				BackupXDR: &models.BackupXDR{
					DC:           "dc1",
					LocalAddress: "127.0.0.1",
					LocalPort:    3004,
					Namespace:    "test",
				},
				// No compression, encryption or secret agent
			},
			verify: func(t *testing.T, cfg *backup.ConfigBackupXDR) {
				t.Helper()
				assert.Nil(t, cfg.CompressionPolicy)
				assert.Nil(t, cfg.EncryptionPolicy)
				assert.Nil(t, cfg.SecretAgentConfig)
			},
		},
		{
			name: "Configuration with only required fields",
			params: &BackupServiceConfig{
				App: &models.App{},
				BackupXDR: &models.BackupXDR{
					DC:        "dc1",
					Namespace: "test",
				},
			},
			verify: func(t *testing.T, cfg *backup.ConfigBackupXDR) {
				t.Helper()
				assert.Equal(t, "dc1", cfg.DC)
				assert.Equal(t, "test", cfg.Namespace)
				assert.Empty(t, cfg.LocalAddress)
				assert.Equal(t, 0, cfg.LocalPort)
				assert.Equal(t, backup.EncoderTypeASBX, cfg.EncoderType)
			},
		},
		{
			name: "Configuration with zero values",
			params: &BackupServiceConfig{
				App: &models.App{},
				BackupXDR: &models.BackupXDR{
					DC:                           "dc1",
					Namespace:                    "test",
					FileLimit:                    0,
					ParallelWrite:                0,
					ReadTimeoutMilliseconds:      0,
					WriteTimeoutMilliseconds:     0,
					ResultQueueSize:              0,
					AckQueueSize:                 0,
					MaxConnections:               0,
					InfoPolingPeriodMilliseconds: 0,
				},
			},
			verify: func(t *testing.T, cfg *backup.ConfigBackupXDR) {
				t.Helper()
				assert.Equal(t, uint64(0), cfg.FileLimit)
				assert.Equal(t, runtime.NumCPU(), cfg.ParallelWrite)
				assert.Equal(t, time.Duration(0), cfg.ReadTimeout)
				assert.Equal(t, time.Duration(0), cfg.WriteTimeout)
				assert.Equal(t, 0, cfg.ResultQueueSize)
				assert.Equal(t, 0, cfg.AckQueueSize)
				assert.Equal(t, 0, cfg.MaxConnections)
				assert.Equal(t, time.Duration(0), cfg.InfoPollingPeriod)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			config := newBackupXDRConfig(tt.params)
			assert.NotNil(t, config)
			tt.verify(t, config)
		})
	}
}
