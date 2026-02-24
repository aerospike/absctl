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
	"github.com/aerospike/absctl/internal/models"
	"github.com/spf13/pflag"
)

const (
	descAccessTierBackup = "Azure access tier is applied to created backup files.\n" +
		"If not set, tier will be determined by the Azure storage account settings and rules."
	descAccessTierRestore = "If is set, tool will try to rehydrate archived files to the specified tier.\n" +
		"Attention! This triggers an asynchronous process that cannot be terminated."
	descAzureMaxConnsPerHostBackup = "Max connections per host optionally" +
		" limits the total number of connections per host,\n" +
		"including connections in the dialing, active, and idle states. On limit violation, dials will block.\n" +
		"Should be greater than --parallel * --azure-upload-concurrency to avoid upload speed degradation.\n" +
		"0 means no limit."
	descAzureMaxConnsPerHostRestore = "Max connections per host optionally" +
		" limits the total number of connections per host,\n" +
		"including connections in the dialing, active, and idle states. On limit violation, dials will block.\n" +
		"Should be greater than --parallel to avoid download speed degradation.\n" +
		"0 means no limit."
)

const (
	flagAzureAccountName          = "azure-account-name"
	flagAzureAccountKey           = "azure-account-key"
	flagAzureTenantID             = "azure-tenant-id"
	flagAzureClientID             = "azure-client-id"
	flagAzureClientSecret         = "azure-client-secret"
	flagAzureEndpoint             = "azure-endpoint"
	flagAzureContainerName        = "azure-container-name"
	flagAzureAccessTier           = "azure-access-tier"
	flagAzureBlockSize            = "azure-block-size"
	flagAzureUploadConcurrency    = "azure-upload-concurrency"
	flagAzureCalculateChecksum    = "azure-calculate-checksum"
	flagAzureRehydratePollDur     = "azure-rehydrate-poll-duration"
	flagAzureRetryMaxAttempts     = "azure-retry-max-attempts"
	flagAzureRetryMaxDelay        = "azure-retry-max-delay"
	flagAzureRetryDelay           = "azure-retry-delay"
	flagAzureRetryReadBackoff     = "azure-retry-read-backoff"
	flagAzureRetryReadMultiplier  = "azure-retry-read-multiplier"
	flagAzureRetryReadMaxAttempts = "azure-retry-read-max-attempts"
	flagAzureMaxConnsPerHost      = "azure-max-conns-per-host"
	flagAzureRequestTimeout       = "azure-request-timeout"
)

type AzureBlob struct {
	operation int
	models.AzureBlob
}

func NewAzureBlob(operation int) *AzureBlob {
	return &AzureBlob{
		operation: operation,
	}
}

func (f *AzureBlob) NewFlagSet() *pflag.FlagSet {
	flagSet := &pflag.FlagSet{}

	var descAccessTier, descMaxConnsPerHost string

	switch f.operation {
	case OperationBackup:
		descAccessTier = descAccessTierBackup
		descMaxConnsPerHost = descAzureMaxConnsPerHostBackup
	case OperationRestore:
		descAccessTier = descAccessTierRestore
		descMaxConnsPerHost = descAzureMaxConnsPerHostRestore
	}

	flagSet.StringVar(&f.AccountName, flagAzureAccountName,
		models.DefaultAzureAccountName,
		"Azure account name for account name, key authorization.")

	flagSet.StringVar(&f.AccountKey, flagAzureAccountKey,
		models.DefaultAzureAccountKey,
		"Azure account key for account name, key authorization.")

	flagSet.StringVar(&f.TenantID, flagAzureTenantID,
		models.DefaultAzureTenantID,
		"Azure tenant ID for Azure Active Directory authorization.")

	flagSet.StringVar(&f.ClientID, flagAzureClientID,
		models.DefaultAzureClientID,
		"Azure client ID for Azure Active Directory authorization.")

	flagSet.StringVar(&f.ClientSecret, flagAzureClientSecret,
		models.DefaultAzureClientSecret,
		"Azure client secret for Azure Active Directory authorization.")

	flagSet.StringVar(&f.Endpoint, flagAzureEndpoint,
		models.DefaultAzureEndpoint,
		"Azure endpoint.")

	flagSet.StringVar(&f.ContainerName, flagAzureContainerName,
		models.DefaultAzureContainerName,
		"Azure container Name.")

	flagSet.StringVar(&f.AccessTier, flagAzureAccessTier,
		models.DefaultAzureAccessTier,
		descAccessTier+
			"\nTiers are: Cold, Cool, Hot.")

	switch f.operation {
	case OperationBackup:
		flagSet.IntVar(&f.BlockSize, flagAzureBlockSize,
			models.DefaultAzureBlockSize,
			"Block size in MiB defines the size of the buffer used during upload.")

		flagSet.IntVar(&f.UploadConcurrency, flagAzureUploadConcurrency,
			models.DefaultAzureUploadConcurrency,
			"Defines the max number of concurrent uploads to be performed to upload the file.\n"+
				"Each concurrent upload will create a buffer of size azure-block-size.")

		flagSet.BoolVar(&f.CalculateChecksum, flagAzureCalculateChecksum,
			models.DefaultCloudCalculateChecksum,
			"Calculate checksum for each uploaded object.")
	case OperationRestore:
		flagSet.Int64Var(&f.RestorePollDuration, flagAzureRehydratePollDur,
			models.DefaultAzureRestorePollDuration,
			"How often ((in ms)) a backup client checks object status when restoring an archived object.")

		flagSet.IntVar(&f.RetryReadBackoff, flagAzureRetryReadBackoff,
			models.DefaultCloudRetryReadBackoff,
			"The initial delay (in ms) between retry attempts. In case of connection errors\n"+
				"tool will retry reading the object from the last known position.")

		flagSet.Float64Var(&f.RetryReadMultiplier, flagAzureRetryReadMultiplier,
			models.DefaultCloudRetryReadMultiplier,
			"Multiplier is used to increase the delay between subsequent retry attempts.\n"+
				"Used in combination with initial delay.")

		flagSet.UintVar(&f.RetryReadMaxAttempts, flagAzureRetryReadMaxAttempts,
			models.DefaultCloudRetryReadMaxAttempts,
			"The maximum number of retry attempts that will be made. If set to 0, no retries will be performed.")
	}

	flagSet.IntVar(&f.RetryMaxAttempts, flagAzureRetryMaxAttempts,
		models.DefaultAzureRetryMaxAttempts,
		"Max retries specifies the maximum number of attempts a failed operation will be retried\n"+
			"before producing an error.")

	flagSet.IntVar(&f.RetryMaxDelay, flagAzureRetryMaxDelay,
		models.DefaultAzureRetryMaxDelay,
		"Max retry delay specifies the maximum delay (in ms) allowed before retrying an operation.\n"+
			"Typically the value is greater than or equal to the value specified in azure-retry-delay.")

	flagSet.IntVar(&f.RetryDelay, flagAzureRetryDelay,
		models.DefaultAzureRetryDelay,
		"Retry delay specifies the initial amount of delay (in ms) to use before retrying an operation.\n"+
			"The value is used only if the HTTP response does not contain a Retry-After header.\n"+
			"The delay increases exponentially with each retry up to the maximum specified by azure-retry-max-delay.")

	flagSet.IntVar(&f.MaxConnsPerHost, flagAzureMaxConnsPerHost,
		models.DefaultCloudMaxConnsPerHost,
		descMaxConnsPerHost)

	flagSet.IntVar(&f.RequestTimeout, flagAzureRequestTimeout,
		models.DefaultCloudRequestTimeout,
		"Timeout (in ms) specifies a time limit for requests made by this Client.\n"+
			"The timeout includes connection time, any redirects, and reading the response body.\n"+
			"0 means no limit.")

	return flagSet
}

func (f *AzureBlob) GetAzureBlob() *models.AzureBlob {
	return &f.AzureBlob
}
