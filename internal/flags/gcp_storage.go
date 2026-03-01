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
	flagGcpKeyPath              = "gcp-key-path"
	flagGcpBucketName           = "gcp-bucket-name"
	flagGcpEndpoint             = "gcp-endpoint-override"
	flagGcpChunkSize            = "gcp-chunk-size"
	flagGcpCalculateChecksum    = "gcp-calculate-checksum"
	flagGcpRetryReadBackoff     = "gcp-retry-read-backoff"
	flagGcpRetryReadMultiplier  = "gcp-retry-read-multiplier"
	flagGcpRetryReadMaxAttempts = "gcp-retry-read-max-attempts"
	flagGcpRetryMaxAttempts     = "gcp-retry-max-attempts"
	flagGcpRetryMaxBackoff      = "gcp-retry-max-backoff"
	flagGcpRetryInitBackoff     = "gcp-retry-init-backoff"
	flagGcpRetryBackoffMult     = "gcp-retry-backoff-multiplier"
	flagGcpMaxConnsPerHost      = "gcp-max-conns-per-host"
	flagGcpRequestTimeout       = "gcp-request-timeout"
)

type GcpStorage struct {
	operation int
	models.GcpStorage
}

func NewGcpStorage(operation int) *GcpStorage {
	return &GcpStorage{
		operation: operation,
	}
}

func (f *GcpStorage) NewFlagSet() *pflag.FlagSet {
	flagSet := &pflag.FlagSet{}

	flagSet.StringVar(&f.KeyFile, flagGcpKeyPath,
		models.DefaultGcpKeyFile,
		"Path to file containing service account JSON key.")

	flagSet.StringVar(&f.BucketName, flagGcpBucketName,
		models.DefaultGcpBucketName,
		"Name of the Google cloud storage bucket.")

	flagSet.StringVar(&f.Endpoint, flagGcpEndpoint,
		models.DefaultGcpEndpoint,
		"An alternate url endpoint to send GCP API calls to.")

	switch f.operation {
	case OperationBackup:
		flagSet.IntVar(&f.ChunkSize, flagGcpChunkSize,
			models.DefaultGcpChunkSize,
			"Chunk size controls the maximum number of megabytes of the object that the app will attempt to send to\n"+
				"the storage in a single request. Objects smaller than the size will be sent in a single request,\n"+
				"while larger objects will be split over multiple requests.")

		flagSet.BoolVar(&f.CalculateChecksum, flagGcpCalculateChecksum,
			models.DefaultCloudCalculateChecksum,
			"Calculate checksum for each uploaded object.")
	case OperationRestore:
		flagSet.IntVar(&f.RetryReadBackoff, flagGcpRetryReadBackoff,
			models.DefaultCloudRetryReadBackoff,
			"The initial delay (in ms) between retry attempts. In case of connection errors\n"+
				"tool will retry reading the object from the last known position.")

		flagSet.Float64Var(&f.RetryReadMultiplier, flagGcpRetryReadMultiplier,
			models.DefaultCloudRetryReadMultiplier,
			"Multiplier is used to increase the delay between subsequent retry attempts.\n"+
				"Used in combination with initial delay.")

		flagSet.UintVar(&f.RetryReadMaxAttempts, flagGcpRetryReadMaxAttempts,
			models.DefaultCloudRetryReadMaxAttempts,
			"The maximum number of retry attempts that will be made. If set to 0, no retries will be performed.")
	}

	flagSet.IntVar(&f.RetryMaxAttempts, flagGcpRetryMaxAttempts,
		models.DefaultGcpRetryMaxAttempts,
		"Max retries specifies the maximum number of attempts a failed operation will be retried\n"+
			"before producing an error.")

	flagSet.IntVar(&f.RetryBackoffMax, flagGcpRetryMaxBackoff,
		models.DefaultGcpRetryBackoffMax,
		"Max backoff is the maximum value (in ms) of the retry period.")

	flagSet.IntVar(&f.RetryBackoffInit, flagGcpRetryInitBackoff,
		models.DefaultGcpRetryBackoffInit,
		"Initial backoff is the initial value (in ms) of the retry period.")

	flagSet.Float64Var(&f.RetryBackoffMultiplier, flagGcpRetryBackoffMult,
		models.DefaultGcpRetryBackoffMultiplier,
		"Multiplier is the factor by which the retry period increases.\n"+
			"It should be greater than 1.")

	flagSet.IntVar(&f.MaxConnsPerHost, flagGcpMaxConnsPerHost,
		models.DefaultCloudMaxConnsPerHost,
		"Max connections per host optionally limits the total number of connections per host,\n"+
			"including connections in the dialing, active, and idle states. On limit violation, dials will block.\n"+
			"Should be greater than --parallel to avoid speed degradation.\n"+
			"0 means no limit.")

	flagSet.IntVar(&f.RequestTimeout, flagGcpRequestTimeout,
		models.DefaultCloudRequestTimeout,
		"Timeout (in ms) specifies a time limit for requests made by this Client.\n"+
			"The timeout includes connection time, any redirects, and reading the response body.\n"+
			"0 means no limit.")

	return flagSet
}

func (f *GcpStorage) GetGcpStorage() *models.GcpStorage {
	return &f.GcpStorage
}
