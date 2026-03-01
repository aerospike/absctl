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

	"github.com/aerospike/absctl/internal/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAzureBlob_NewFlagSetRestore(t *testing.T) {
	t.Parallel()
	azureBlob := NewAzureBlob(OperationRestore)

	flagSet := azureBlob.NewFlagSet()

	args := []string{
		"--" + flagAzureAccountName, "myaccount",
		"--" + flagAzureAccountKey, "mykey",
		"--" + flagAzureTenantID, "tenant-id",
		"--" + flagAzureClientID, "client-id",
		"--" + flagAzureClientSecret, "client-secret",
		"--" + flagAzureEndpoint, "https://custom-endpoint.com",
		"--" + flagAzureContainerName, "my-container",
		"--" + flagAzureAccessTier, "Standard",
		"--" + flagAzureRehydratePollDur, "1000",
		"--" + flagAzureRetryMaxAttempts, "10",
		"--" + flagAzureRetryMaxDelay, "10",
		"--" + flagAzureRetryDelay, "10",
		"--" + flagAzureRetryReadBackoff, "900",
		"--" + flagAzureRetryReadMultiplier, "1.5",
		"--" + flagAzureRetryReadMaxAttempts, "5",
	}

	err := flagSet.Parse(args)
	require.NoError(t, err)

	result := azureBlob.GetAzureBlob()

	assert.Equal(t, "myaccount", result.AccountName, "The azure-account-name flag should be parsed correctly")
	assert.Equal(t, "mykey", result.AccountKey, "The azure-account-key flag should be parsed correctly")
	assert.Equal(t, "tenant-id", result.TenantID, "The azure-tenant-id flag should be parsed correctly")
	assert.Equal(t, "client-id", result.ClientID, "The azure-client-id flag should be parsed correctly")
	assert.Equal(t, "client-secret", result.ClientSecret, "The azure-client-secret flag should be parsed correctly")
	assert.Equal(t, "https://custom-endpoint.com", result.Endpoint, "The azure-endpoint flag should be parsed correctly")
	assert.Equal(t, "my-container", result.ContainerName, "The azure-container-name flag should be parsed correctly")
	assert.Equal(t, "Standard", result.AccessTier, "The azure-access-tier flag should be parsed correctly")
	assert.Equal(t, int64(1000), result.RestorePollDuration, "The azure-rehydrate-poll-duration flag should be parsed correctly")
	assert.Equal(t, 10, result.RetryMaxAttempts, "The azure-retry-max-attempts flag should be parsed correctly")
	assert.Equal(t, 10, result.RetryMaxDelay, "The azure-retry-max-delay flag should be parsed correctly")
	assert.Equal(t, 10, result.RetryDelay, "The azure-retry-delay flag should be parsed correctly")
	assert.Equal(t, 900, result.RetryReadBackoff, "The azure-retry-read-backoff flag should be parsed correctly")
	assert.InEpsilon(t, 1.5, result.RetryReadMultiplier, 0.0, "The azure-retry-read-multiplier flag should be parsed correctly")
	assert.Equal(t, uint(5), result.RetryReadMaxAttempts, "The azure-retry-read-max-attempts flag should be parsed correctly")
}

func TestAzureBlob_NewFlagSet_DefaultValuesRestore(t *testing.T) {
	t.Parallel()
	azureBlob := NewAzureBlob(OperationRestore)

	flagSet := azureBlob.NewFlagSet()

	err := flagSet.Parse([]string{})
	require.NoError(t, err)

	result := azureBlob.GetAzureBlob()

	assert.Empty(t, result.AccountName, "The default value for azure-account-name should be an empty string")
	assert.Empty(t, result.AccountKey, "The default value for azure-account-key should be an empty string")
	assert.Empty(t, result.TenantID, "The default value for azure-tenant-id should be an empty string")
	assert.Empty(t, result.ClientID, "The default value for azure-client-id should be an empty string")
	assert.Empty(t, result.ClientSecret, "The default value for azure-client-secret should be an empty string")
	assert.Empty(t, result.Endpoint, "The default value for azure-endpoint should be an empty string")
	assert.Empty(t, result.ContainerName, "The default value for azure-container-name should be an empty string")
	assert.Empty(t, result.AccessTier, "The default value for azure-access-tier should be an empty string")
	assert.Equal(t, models.DefaultAzureRestorePollDuration, result.RestorePollDuration, "The default value for azure-rehydrate-poll-duration should be 60000")
	assert.Equal(t, models.DefaultAzureRetryMaxAttempts, result.RetryMaxAttempts, "The default value for azure-retry-max-attempts flag should be 100")
	assert.Equal(t, models.DefaultAzureRetryMaxDelay, result.RetryMaxDelay, "The default value for azure-retry-max-delay flag should be 90")
	assert.Equal(t, models.DefaultAzureRetryDelay, result.RetryDelay, "The default value for azure-retry-delay flag should be 60")
	assert.InEpsilon(t, models.DefaultCloudRetryReadBackoff, result.RetryReadBackoff, 0.0, "The default value for azure-retry-read-backoff should be 0")
	assert.InEpsilon(t, models.DefaultCloudRetryReadMultiplier, result.RetryReadMultiplier, 0.0, "The default value for azure-retry-read-multiplier should be 0")
	assert.Equal(t, models.DefaultCloudRetryReadMaxAttempts, result.RetryReadMaxAttempts, "The default value for azure-retry-read-max-attempts should be 0")
}

func TestAzureBlob_NewFlagSetBackup(t *testing.T) {
	t.Parallel()
	azureBlob := NewAzureBlob(OperationBackup)

	flagSet := azureBlob.NewFlagSet()

	args := []string{
		"--" + flagAzureAccountName, "myaccount",
		"--" + flagAzureAccountKey, "mykey",
		"--" + flagAzureTenantID, "tenant-id",
		"--" + flagAzureClientID, "client-id",
		"--" + flagAzureClientSecret, "client-secret",
		"--" + flagAzureEndpoint, "https://custom-endpoint.com",
		"--" + flagAzureContainerName, "my-container",
		"--" + flagAzureAccessTier, "Standard",
		"--" + flagAzureBlockSize, "1",
		"--" + flagAzureUploadConcurrency, "10",
		"--" + flagAzureMaxConnsPerHost, "10",
		"--" + flagAzureRequestTimeout, "10",
	}

	err := flagSet.Parse(args)
	require.NoError(t, err)

	result := azureBlob.GetAzureBlob()

	assert.Equal(t, "myaccount", result.AccountName, "The azure-account-name flag should be parsed correctly")
	assert.Equal(t, "mykey", result.AccountKey, "The azure-account-key flag should be parsed correctly")
	assert.Equal(t, "tenant-id", result.TenantID, "The azure-tenant-id flag should be parsed correctly")
	assert.Equal(t, "client-id", result.ClientID, "The azure-client-id flag should be parsed correctly")
	assert.Equal(t, "client-secret", result.ClientSecret, "The azure-client-secret flag should be parsed correctly")
	assert.Equal(t, "https://custom-endpoint.com", result.Endpoint, "The azure-endpoint flag should be parsed correctly")
	assert.Equal(t, "my-container", result.ContainerName, "The azure-container-name flag should be parsed correctly")
	assert.Equal(t, "Standard", result.AccessTier, "The azure-access-tier flag should be parsed correctly")
	assert.Equal(t, 1, result.BlockSize, "The azure-block-size flag should be parsed correctly")
	assert.Equal(t, 10, result.UploadConcurrency, "The azure-upload-concurrency flag should be parsed correctly")
	assert.Equal(t, 10, result.MaxConnsPerHost, "The azure-max-conns-per-host flag should be parsed correctly")
	assert.Equal(t, 10, result.RequestTimeout, "The azure-request-timeout flag should be parsed correctly")
}

func TestAzureBlob_NewFlagSet_DefaultValuesBackup(t *testing.T) {
	t.Parallel()
	azureBlob := NewAzureBlob(OperationBackup)

	flagSet := azureBlob.NewFlagSet()

	err := flagSet.Parse([]string{})
	require.NoError(t, err)

	result := azureBlob.GetAzureBlob()

	assert.Empty(t, result.AccountName, "The default value for azure-account-name should be an empty string")
	assert.Empty(t, result.AccountKey, "The default value for azure-account-key should be an empty string")
	assert.Empty(t, result.TenantID, "The default value for azure-tenant-id should be an empty string")
	assert.Empty(t, result.ClientID, "The default value for azure-client-id should be an empty string")
	assert.Empty(t, result.ClientSecret, "The default value for azure-client-secret should be an empty string")
	assert.Empty(t, result.Endpoint, "The default value for azure-endpoint should be an empty string")
	assert.Empty(t, result.ContainerName, "The default value for azure-container-name should be an empty string")
	assert.Empty(t, result.AccessTier, "The default value for azure-access-tier should be an empty string")
	assert.Equal(t, models.DefaultAzureBlockSize, result.BlockSize, "The default value for azure-block-size should be 5MB")
	assert.Equal(t, 0, result.MaxConnsPerHost, "The default value for s3-max-conns-per-host should be 0")
	assert.Equal(t, models.DefaultCloudRequestTimeout, result.RequestTimeout, "The default value for azure-request-timeout should be 0")
}
