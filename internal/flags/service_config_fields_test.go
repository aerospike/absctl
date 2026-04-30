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

package flags

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestServiceConfigCluster_NewFlagSet(t *testing.T) {
	t.Parallel()

	f := NewServiceConfigCluster()
	flagSet := f.NewFlagSet()

	args := []string{
		"--name", "primary",
		"--seed-node", "127.0.0.1:3000",
		"--seed-node", "node2.example:3000/edge",
		"--prefer-racks", "1,2,3",
		"--conn-timeout", "5000",
		"--label", "primary",
		"--max-parallel-scans", "4",
		"--use-services-alternate",
		"--credentials-user", "admin",
		"--credentials-password", "secret",
		"--credentials-auth-mode", "INTERNAL",
		"--tls-ca-file", "/etc/ca.pem",
		"--tls-name", "tls-1",
	}

	require.NoError(t, flagSet.Parse(args))

	assert.Equal(t, "primary", f.Name)
	assert.Equal(t, []string{"127.0.0.1:3000", "node2.example:3000/edge"}, f.SeedNodes)
	assert.Equal(t, []int{1, 2, 3}, f.PreferRacks)
	assert.Equal(t, 5000, f.ConnTimeout)
	assert.Equal(t, "primary", f.Label)
	assert.Equal(t, 4, f.MaxParallelScans)
	assert.True(t, f.UseServicesAlternate)
	assert.Equal(t, "admin", f.CredentialsUser)
	assert.Equal(t, "secret", f.CredentialsPassword)
	assert.Equal(t, "INTERNAL", f.CredentialsAuthMode)
	assert.Equal(t, "/etc/ca.pem", f.TLSCaFile)
	assert.Equal(t, "tls-1", f.TLSName)
}

func TestServiceConfigCluster_Defaults(t *testing.T) {
	t.Parallel()

	f := NewServiceConfigCluster()
	flagSet := f.NewFlagSet()

	require.NoError(t, flagSet.Parse([]string{}))

	assert.Empty(t, f.Name)
	assert.Empty(t, f.SeedNodes)
	assert.Empty(t, f.PreferRacks)
	assert.Zero(t, f.ConnTimeout)
	assert.False(t, f.UseServicesAlternate)
}

func TestServiceConfigPolicy_NewFlagSet(t *testing.T) {
	t.Parallel()

	f := NewServiceConfigPolicy()
	flagSet := f.NewFlagSet()

	args := []string{
		"--name", "daily",
		"--bandwidth", "100",
		"--parallel", "4",
		"--no-indexes",
		"--sealed",
		"--compression-mode", "ZSTD",
		"--compression-level", "3",
		"--retention-full", "7",
		"--retention-incremental", "1",
		"--retry-base-timeout", "500",
		"--retry-max-retries", "5",
		"--retry-multiplier", "2.0",
	}

	require.NoError(t, flagSet.Parse(args))

	assert.Equal(t, "daily", f.Name)
	assert.Equal(t, 100, f.Bandwidth)
	assert.Equal(t, 4, f.Parallel)
	assert.True(t, f.NoIndexes)
	assert.True(t, f.Sealed)
	assert.Equal(t, "ZSTD", f.CompressionMode)
	assert.Equal(t, 3, f.CompressionLevel)
	assert.Equal(t, 7, f.RetentionFull)
	assert.Equal(t, 1, f.RetentionIncremental)
	assert.Equal(t, 500, f.RetryBaseTimeout)
	assert.Equal(t, 5, f.RetryMaxRetries)
	assert.InDelta(t, 2.0, float64(f.RetryMultiplier), 0.0001)
}

func TestServiceConfigRoutine_NewFlagSet(t *testing.T) {
	t.Parallel()

	f := NewServiceConfigRoutine()
	flagSet := f.NewFlagSet()

	args := []string{
		"--name", "hourly",
		"--source-cluster", "primary",
		"--storage", "local",
		"--interval-cron", "0 * * * *",
		"--incr-interval-cron", "*/15 * * * *",
		"--namespaces", "test,bar",
		"--bin-list", "a,b",
		"--rack-list", "1,2",
		"--node-list", "node1:3000,node2:3000",
		"--partition-list", "0,100-50",
		"--disabled",
	}

	require.NoError(t, flagSet.Parse(args))

	assert.Equal(t, "hourly", f.Name)
	assert.Equal(t, "primary", f.SourceCluster)
	assert.Equal(t, "local", f.Storage)
	assert.Equal(t, "0 * * * *", f.IntervalCron)
	assert.Equal(t, "*/15 * * * *", f.IncrIntervalCron)
	assert.Equal(t, []string{"test", "bar"}, f.Namespaces)
	assert.Equal(t, []string{"a", "b"}, f.BinList)
	assert.Equal(t, []int{1, 2}, f.RackList)
	assert.Equal(t, []string{"node1:3000", "node2:3000"}, f.NodeList)
	assert.Equal(t, "0,100-50", f.PartitionList)
	assert.True(t, f.Disabled)
}

func TestServiceConfigStorage_NewFlagSet(t *testing.T) {
	t.Parallel()

	f := NewServiceConfigStorage()
	flagSet := f.NewFlagSet()

	args := []string{
		"--name", "remote",
		"--s3-storage-bucket", "my-bucket",
		"--s3-storage-s3-region", "us-east-1",
		"--s3-storage-access-key-id", "AKIA",
		"--s3-storage-secret-access-key", "secret",
		"--s3-storage-path", "backups",
		"--s3-storage-min-part-size", "1024",
	}

	require.NoError(t, flagSet.Parse(args))

	assert.Equal(t, "remote", f.Name)
	assert.Equal(t, "my-bucket", f.S3Bucket)
	assert.Equal(t, "us-east-1", f.S3Region)
	assert.Equal(t, "AKIA", f.S3AccessKeyID)
	assert.Equal(t, "secret", f.S3SecretAccessKey)
	assert.Equal(t, "backups", f.S3Path)
	assert.Equal(t, 1024, f.S3MinPartSize)
}

func TestServiceConfigStorage_LocalAndAzure(t *testing.T) {
	t.Parallel()

	f := NewServiceConfigStorage()
	flagSet := f.NewFlagSet()

	args := []string{
		"--name", "az",
		"--azure-storage-account-name", "myacct",
		"--azure-storage-container-name", "container",
		"--azure-storage-endpoint", "https://az.example",
	}

	require.NoError(t, flagSet.Parse(args))

	assert.Equal(t, "myacct", f.AzureAccountName)
	assert.Equal(t, "container", f.AzureContainerName)
	assert.Equal(t, "https://az.example", f.AzureEndpoint)
}
