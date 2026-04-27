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

package service

import (
	"testing"

	"github.com/aerospike/absctl/internal/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseSeedNodes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   []string
		want    int
		wantErr string
	}{
		{
			name:  "host port",
			input: []string{"127.0.0.1:3000"},
			want:  1,
		},
		{
			name:  "host port with tls",
			input: []string{"node1.example:3000/edge-tls"},
			want:  1,
		},
		{
			name:  "multiple",
			input: []string{"a:3000", "b:3001/x"},
			want:  2,
		},
		{
			name:  "ipv6 host",
			input: []string{"[::1]:3000"},
			want:  1,
		},
		{
			name:    "missing port",
			input:   []string{"127.0.0.1"},
			wantErr: "expected host:port",
		},
		{
			name:    "non-integer port",
			input:   []string{"host:port"},
			wantErr: "is not an integer",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			nodes, err := parseSeedNodes(tt.input)

			if tt.wantErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.wantErr)

				return
			}

			require.NoError(t, err)
			require.Len(t, nodes, tt.want)
		})
	}
}

func TestParseSeedNodes_TlsName(t *testing.T) {
	t.Parallel()

	nodes, err := parseSeedNodes([]string{"host:3000/edge-tls"})
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	assert.Equal(t, "host", nodes[0].HostName)
	assert.Equal(t, 3000, nodes[0].Port)
	require.NotNil(t, nodes[0].TlsName)
	assert.Equal(t, "edge-tls", *nodes[0].TlsName)
}

func TestBuildClusterBody_FromFlagsOnly(t *testing.T) {
	t.Parallel()

	f := &models.ConfigClusterFields{
		Name:                       "primary",
		SeedNodes:                  []string{"127.0.0.1:3000/edge"},
		PreferRacks:                []int{1, 2},
		ConnTimeout:                5000,
		Label:                      "primary",
		MaxParallelScans:           4,
		UseServicesAlternate:       true,
		CredentialsUser:            "admin",
		CredentialsPassword:        "secret",
		CredentialsAuthMode:        "INTERNAL",
		CredentialsSecretAgentName: "agent",
		TLSCaFile:                  "/etc/ca.pem",
		TLSName:                    "tls-1",
	}

	body, err := buildClusterBody(f)
	require.NoError(t, err)

	require.Len(t, body.SeedNodes, 1)
	assert.Equal(t, "127.0.0.1", body.SeedNodes[0].HostName)
	assert.Equal(t, 3000, body.SeedNodes[0].Port)
	require.NotNil(t, body.SeedNodes[0].TlsName)
	assert.Equal(t, "edge", *body.SeedNodes[0].TlsName)

	require.NotNil(t, body.PreferRacks)
	assert.Equal(t, []int{1, 2}, *body.PreferRacks)

	require.NotNil(t, body.ConnTimeout)
	assert.Equal(t, 5000, *body.ConnTimeout)

	require.NotNil(t, body.Label)
	assert.Equal(t, "primary", *body.Label)

	require.NotNil(t, body.UseServicesAlternate)
	assert.True(t, *body.UseServicesAlternate)

	require.NotNil(t, body.Credentials)
	require.NotNil(t, body.Credentials.User)
	assert.Equal(t, "admin", *body.Credentials.User)
	require.NotNil(t, body.Credentials.AuthMode)
	assert.Equal(t, "INTERNAL", string(*body.Credentials.AuthMode))
	require.NotNil(t, body.Credentials.SecretAgentName)
	assert.Equal(t, "agent", *body.Credentials.SecretAgentName)

	require.NotNil(t, body.Tls)
	require.NotNil(t, body.Tls.CaFile)
	assert.Equal(t, "/etc/ca.pem", *body.Tls.CaFile)
	require.NotNil(t, body.Tls.Name)
	assert.Equal(t, "tls-1", *body.Tls.Name)
}

func TestBuildClusterBody_FileBaseFlagOverride(t *testing.T) {
	t.Parallel()

	file := writeJSONFile(t, map[string]any{
		"label":        "from-file",
		"conn-timeout": 1000,
		"seed-nodes":   []map[string]any{{"host-name": "10.0.0.1", "port": 3000}},
		"prefer-racks": []int{9},
	})

	f := &models.ConfigClusterFields{
		Name:        "primary",
		File:        file,
		Label:       "from-flag",
		ConnTimeout: 0,
	}

	body, err := buildClusterBody(f)
	require.NoError(t, err)

	require.NotNil(t, body.Label)
	assert.Equal(t, "from-flag", *body.Label, "flag should override file")

	require.NotNil(t, body.ConnTimeout)
	assert.Equal(t, 1000, *body.ConnTimeout, "unset flag must keep file value")

	require.Len(t, body.SeedNodes, 1)
	assert.Equal(t, "10.0.0.1", body.SeedNodes[0].HostName)

	require.NotNil(t, body.PreferRacks)
	assert.Equal(t, []int{9}, *body.PreferRacks)
}

func TestBuildClusterBody_BadSeedNode(t *testing.T) {
	t.Parallel()

	_, err := buildClusterBody(&models.ConfigClusterFields{
		Name:      "primary",
		SeedNodes: []string{"oops"},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "expected host:port")
}

func TestBuildPolicyBody(t *testing.T) {
	t.Parallel()

	f := &models.ConfigPolicyFields{
		Name:                 "daily",
		Parallel:             4,
		Bandwidth:            100,
		NoIndexes:            true,
		Sealed:               true,
		CompressionMode:      "ZSTD",
		CompressionLevel:     3,
		EncryptionMode:       "AES256",
		EncryptionKeyFile:    "/etc/key",
		RetentionFull:        7,
		RetentionIncremental: 1,
		RetryBaseTimeout:     500,
		RetryMaxRetries:      5,
		RetryMultiplier:      2.0,
	}

	body, err := buildPolicyBody(f)
	require.NoError(t, err)

	require.NotNil(t, body.Parallel)
	assert.Equal(t, 4, *body.Parallel)

	require.NotNil(t, body.NoIndexes)
	assert.True(t, *body.NoIndexes)

	require.NotNil(t, body.Compression)
	require.NotNil(t, body.Compression.Mode)
	assert.Equal(t, "ZSTD", string(*body.Compression.Mode))
	require.NotNil(t, body.Compression.Level)
	assert.Equal(t, 3, *body.Compression.Level)

	require.NotNil(t, body.Encryption)
	require.NotNil(t, body.Encryption.Mode)
	assert.Equal(t, "AES256", string(*body.Encryption.Mode))
	require.NotNil(t, body.Encryption.KeyFile)
	assert.Equal(t, "/etc/key", *body.Encryption.KeyFile)

	require.NotNil(t, body.Retention)
	require.NotNil(t, body.Retention.Full)
	assert.Equal(t, 7, *body.Retention.Full)
	require.NotNil(t, body.Retention.Incremental)
	assert.Equal(t, 1, *body.Retention.Incremental)

	require.NotNil(t, body.RetryPolicy)
	require.NotNil(t, body.RetryPolicy.MaxRetries)
	assert.Equal(t, 5, *body.RetryPolicy.MaxRetries)
	require.NotNil(t, body.RetryPolicy.Multiplier)
	assert.InDelta(t, 2.0, float64(*body.RetryPolicy.Multiplier), 0.0001)
}

func TestBuildRoutineBody(t *testing.T) {
	t.Parallel()

	f := &models.ConfigRoutineFields{
		Name:             "hourly",
		BackupPolicy:     "daily",
		SourceCluster:    "primary",
		Storage:          "local",
		IntervalCron:     "0 * * * *",
		IncrIntervalCron: "*/15 * * * *",
		Disabled:         true,
		Namespaces:       []string{"test", "bar"},
		BinList:          []string{"a", "b"},
		SetList:          []string{"s1"},
		NodeList:         []string{"node1:3000"},
		RackList:         []int{1, 2},
		PartitionList:    "0,100-50",
		SecretAgent:      "agent",
	}

	body, err := buildRoutineBody(f)
	require.NoError(t, err)

	require.NotNil(t, body.BackupPolicy)
	assert.Equal(t, "daily", *body.BackupPolicy)

	assert.Equal(t, "primary", body.SourceCluster)
	assert.Equal(t, "local", body.Storage)
	assert.Equal(t, "0 * * * *", body.IntervalCron)

	require.NotNil(t, body.IncrIntervalCron)
	assert.Equal(t, "*/15 * * * *", *body.IncrIntervalCron)

	require.NotNil(t, body.Disabled)
	assert.True(t, *body.Disabled)

	assert.Equal(t, []string{"test", "bar"}, body.Namespaces)

	require.NotNil(t, body.BinList)
	assert.Equal(t, []string{"a", "b"}, *body.BinList)

	require.NotNil(t, body.RackList)
	assert.Equal(t, []int{1, 2}, *body.RackList)

	require.NotNil(t, body.PartitionList)
	assert.Equal(t, "0,100-50", *body.PartitionList)

	require.NotNil(t, body.SecretAgent)
	assert.Equal(t, "agent", *body.SecretAgent)
}

func TestBuildStorageBody_Local(t *testing.T) {
	t.Parallel()

	body, err := buildStorageBody(&models.ConfigStorageFields{
		Name:             "local",
		LocalPath:        "/tmp/backup",
		LocalMinPartSize: 1024,
	})
	require.NoError(t, err)

	require.NotNil(t, body.LocalStorage)
	assert.Equal(t, "/tmp/backup", body.LocalStorage.Path)
	require.NotNil(t, body.LocalStorage.MinPartSize)
	assert.Equal(t, 1024, *body.LocalStorage.MinPartSize)

	assert.Nil(t, body.S3Storage)
	assert.Nil(t, body.GcpStorage)
	assert.Nil(t, body.AzureStorage)
}

func TestBuildStorageBody_S3(t *testing.T) {
	t.Parallel()

	body, err := buildStorageBody(&models.ConfigStorageFields{
		Name:                  "remote",
		S3Bucket:              "my-bucket",
		S3Region:              "us-east-1",
		S3AccessKeyID:         "AKIA",
		S3SecretAccessKey:     "secret",
		S3Path:                "backups",
		S3LogLevel:            "DEBUG",
		S3MaxAsyncConnections: 10,
		S3SecretAgentName:     "agent",
	})
	require.NoError(t, err)

	require.NotNil(t, body.S3Storage)
	assert.Equal(t, "my-bucket", body.S3Storage.Bucket)
	assert.Equal(t, "us-east-1", body.S3Storage.S3Region)

	require.NotNil(t, body.S3Storage.AccessKeyId)
	assert.Equal(t, "AKIA", *body.S3Storage.AccessKeyId)

	require.NotNil(t, body.S3Storage.S3LogLevel)
	assert.Equal(t, "DEBUG", string(*body.S3Storage.S3LogLevel))

	require.NotNil(t, body.S3Storage.MaxAsyncConnections)
	assert.Equal(t, 10, *body.S3Storage.MaxAsyncConnections)

	require.NotNil(t, body.S3Storage.SecretAgentName)
	assert.Equal(t, "agent", *body.S3Storage.SecretAgentName)
}

func TestBuildStorageBody_GcpAndAzure(t *testing.T) {
	t.Parallel()

	gcp, err := buildStorageBody(&models.ConfigStorageFields{
		Name:                "gcp",
		GcpBucketName:       "bk",
		GcpStorageClassData: "STANDARD",
	})
	require.NoError(t, err)
	require.NotNil(t, gcp.GcpStorage)
	assert.Equal(t, "bk", gcp.GcpStorage.BucketName)
	require.NotNil(t, gcp.GcpStorage.StorageClass)
	require.NotNil(t, gcp.GcpStorage.StorageClass.Data)
	assert.Equal(t, "STANDARD", string(*gcp.GcpStorage.StorageClass.Data))

	az, err := buildStorageBody(&models.ConfigStorageFields{
		Name:                  "azure",
		AzureContainerName:    "container",
		AzureEndpoint:         "https://az.example",
		AzureStorageClassData: "Hot",
		AzureStorageClassMeta: "Cool",
	})
	require.NoError(t, err)
	require.NotNil(t, az.AzureStorage)
	assert.Equal(t, "container", az.AzureStorage.ContainerName)
	assert.Equal(t, "https://az.example", az.AzureStorage.Endpoint)
	require.NotNil(t, az.AzureStorage.StorageClass)
	require.NotNil(t, az.AzureStorage.StorageClass.Data)
	assert.Equal(t, "Hot", string(*az.AzureStorage.StorageClass.Data))
	require.NotNil(t, az.AzureStorage.StorageClass.Metadata)
	assert.Equal(t, "Cool", string(*az.AzureStorage.StorageClass.Metadata))
}

func TestBuildBody_FileNotFound(t *testing.T) {
	t.Parallel()

	_, err := buildClusterBody(&models.ConfigClusterFields{Name: "p", File: "/no/such.json"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to read request file")
}
