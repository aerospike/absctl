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
	descS3MaxConnsPerHostBackup = "Max connections per host optionally" +
		" limits the total number of connections per host,\n" +
		"including connections in the dialing, active, and idle states. On limit violation, dials will block.\n" +
		"Should be greater than --parallel * --s3-upload-concurrency to avoid upload speed degradation.\n" +
		"0 means no limit."
	descS3MaxConnsPerHostRestore = "Max connections per host optionally" +
		" limits the total number of connections per host,\n" +
		"including connections in the dialing, active, and idle states. On limit violation, dials will block.\n" +
		"Should be greater than --parallel to avoid download speed degradation.\n" +
		"0 means no limit."
)

type AwsS3 struct {
	operation int
	models.AwsS3
}

func NewAwsS3(operation int) *AwsS3 {
	return &AwsS3{
		operation: operation,
	}
}

func (f *AwsS3) NewFlagSet() *pflag.FlagSet {
	flagSet := &pflag.FlagSet{}

	var descMaxConnsPerHost string

	switch f.operation {
	case OperationBackup:
		descMaxConnsPerHost = descS3MaxConnsPerHostBackup
	case OperationRestore:
		descMaxConnsPerHost = descS3MaxConnsPerHostRestore
	}

	flagSet.StringVar(&f.BucketName, "s3-bucket-name",
		models.DefaultS3BucketName,
		"Existing S3 bucket name")

	flagSet.StringVar(&f.Region, "s3-region",
		models.DefaultS3Region,
		"The S3 region that the bucket(s) exist in.")

	flagSet.StringVar(&f.Profile, "s3-profile",
		models.DefaultS3Profile,
		"The S3 profile to use for credentials.")

	flagSet.StringVar(&f.AccessKeyID, "s3-access-key-id",
		models.DefaultS3AccessKeyID,
		"S3 access key ID. If not set, profile auth info will be used.")

	flagSet.StringVar(&f.SecretAccessKey, "s3-secret-access-key",
		models.DefaultS3SecretAccessKey,
		"S3 secret access key. If not set, profile auth info will be used.")

	flagSet.StringVar(&f.Endpoint, "s3-endpoint-override",
		models.DefaultS3Endpoint,
		"An alternate URL endpoint to send S3 API calls to.")

	switch f.operation {
	case OperationBackup:
		flagSet.StringVar(&f.StorageClass, "s3-storage-class",
			models.DefaultS3StorageClass,
			"Apply storage class to backup files. Storage classes are:\n"+
				"STANDARD,\n"+
				"REDUCED_REDUNDANCY,\n"+
				"STANDARD_IA,\n"+
				"ONEZONE_IA,\n"+
				"INTELLIGENT_TIERING,\n"+
				"GLACIER,\n"+
				"DEEP_ARCHIVE,\n"+
				"OUTPOSTS,\n"+
				"GLACIER_IR,\n"+
				"SNOW,\n"+
				"EXPRESS_ONEZONE.")

		flagSet.IntVar(&f.ChunkSize, "s3-chunk-size",
			models.DefaultS3ChunkSize,
			"Chunk size controls the maximum number of megabytes of the object that the app will attempt to send to\n"+
				"the storage in a single request. Objects smaller than the size will be sent in a single request,\n"+
				"while larger objects will be split over multiple requests.")

		flagSet.IntVar(&f.UploadConcurrency, "s3-upload-concurrency",
			models.DefaultS3UploadConcurrency,
			"Defines the max number of concurrent uploads to be performed to upload the file.\n"+
				"Each concurrent upload will create a buffer of size s3-chunk-size.")

		flagSet.BoolVar(&f.CalculateChecksum, "s3-calculate-checksum",
			models.DefaultCloudCalculateChecksum,
			"Calculate checksum for each uploaded object.")
	case OperationRestore:
		flagSet.StringVar(&f.AccessTier, "s3-tier",
			models.DefaultS3AccessTier,
			"If is set, tool will try to restore archived files to the specified tier.\n"+
				"Attention! This triggers an asynchronous process that cannot be terminated.\n"+
				"Tiers are: Standard, Bulk, Expedited.",
		)

		flagSet.Int64Var(&f.RestorePollDuration, "s3-restore-poll-duration",
			models.DefaultS3RestorePollDuration,
			"How often ((in ms)) a backup client checks object status when restoring an archived object.",
		)

		flagSet.IntVar(&f.RetryReadBackoff, "s3-retry-read-backoff",
			models.DefaultCloudRetryReadBackoff,
			"The initial delay (in ms) between retry attempts. In case of connection errors\n"+
				"tool will retry reading the object from the last known position.")

		flagSet.Float64Var(&f.RetryReadMultiplier, "s3-retry-read-multiplier",
			models.DefaultCloudRetryReadMultiplier,
			"Multiplier is used to increase the delay between subsequent retry attempts.\n"+
				"Used in combination with initial delay.")

		flagSet.UintVar(&f.RetryReadMaxAttempts, "s3-retry-read-max-attempts",
			models.DefaultCloudRetryReadMaxAttempts,
			"The maximum number of retry attempts that will be made. If set to 0, no retries will be performed.")
	}

	flagSet.IntVar(&f.RetryMaxAttempts, "s3-retry-max-attempts",
		models.DefaultS3RetryMaxAttempts,
		"Maximum number of attempts that should be made in case of an error.")

	flagSet.IntVar(&f.RetryMaxBackoff, "s3-retry-max-backoff",
		models.DefaultS3RetryMaxBackoff,
		"Max backoff duration (in ms) between retried attempts.\n"+
			"The delay increases exponentially with each retry up to the maximum specified by s3-retry-max-backoff.")

	flagSet.IntVar(&f.MaxConnsPerHost, "s3-max-conns-per-host",
		models.DefaultCloudMaxConnsPerHost,
		descMaxConnsPerHost,
	)

	flagSet.IntVar(&f.RequestTimeout, "s3-request-timeout",
		models.DefaultCloudRequestTimeout,
		"Timeout (in ms) specifies a time limit for requests made by this Client.\n"+
			"The timeout includes connection time, any redirects, and reading the response body.\n"+
			"0 means no limit.")

	return flagSet
}

func (f *AwsS3) GetAwsS3() *models.AwsS3 {
	return &f.AwsS3
}
