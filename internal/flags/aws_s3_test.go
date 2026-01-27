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
)

func TestAwsS3_NewFlagSet(t *testing.T) {
	t.Parallel()
	awsS3 := NewAwsS3(OperationBackup)

	flagSet := awsS3.NewFlagSet()

	args := []string{
		"--s3-region", "us-west-2",
		"--s3-profile", "my-profile",
		"--s3-endpoint-override", "https://s3.custom-endpoint.com",
		"--s3-access-key-id", "my-access-key-id",
		"--s3-secret-access-key", "my-secret-access-key",
		"--s3-storage-class", "my-storage-class",
		"--s3-chunk-size", "1",
		"--s3-retry-max-attempts", "10",
		"--s3-retry-max-backoff", "10",
		"--s3-upload-concurrency", "10",
		"--s3-max-conns-per-host", "10",
		"--s3-request-timeout", "10",
	}

	err := flagSet.Parse(args)
	assert.NoError(t, err)

	result := awsS3.GetAwsS3()

	assert.Equal(t, "us-west-2", result.Region, "The s3-region flag should be parsed correctly")
	assert.Equal(t, "my-profile", result.Profile, "The s3-profile flag should be parsed correctly")
	assert.Equal(t, "https://s3.custom-endpoint.com", result.Endpoint, "The s3-endpoint-override flag should be parsed correctly")
	assert.Equal(t, "my-access-key-id", result.AccessKeyID, "The s3-access-key-id flag should be parsed correctly")
	assert.Equal(t, "my-secret-access-key", result.SecretAccessKey, "The s3-secret-access-key flag should be parsed correctly")
	assert.Equal(t, "my-storage-class", result.StorageClass, "The s3-storage-class flag should be parsed correctly")
	assert.Equal(t, 1, result.ChunkSize, "The s3-chunk-size flag should be parsed correctly")
	assert.Equal(t, 10, result.RetryMaxAttempts, "The s3-retry-max-attempts flag should be parsed correctly")
	assert.Equal(t, 10, result.RetryMaxBackoff, "The s3-retry-max-backoff flag should be parsed correctly")
	assert.Equal(t, 10, result.UploadConcurrency, "The s3-upload-concurrency flag should be parsed correctly")
	assert.Equal(t, 10, result.MaxConnsPerHost, "The s3-max-conns-per-host flag should be parsed correctly")
	assert.Equal(t, 10, result.RequestTimeout, "The s3-request-timeout flag should be parsed correctly")

	awsS3 = NewAwsS3(OperationRestore)
	flagSet = awsS3.NewFlagSet()
	args = []string{
		"--s3-retry-read-backoff", "900",
		"--s3-retry-read-multiplier", "1.5",
		"--s3-retry-read-max-attempts", "5",
	}

	err = flagSet.Parse(args)
	assert.NoError(t, err)

	result = awsS3.GetAwsS3()

	assert.Equal(t, 900, result.RetryReadBackoff, "The s3-retry-read-backoff flag should be parsed correctly")
	assert.Equal(t, 1.5, result.RetryReadMultiplier, "The s3-retry-read-multiplier flag should be parsed correctly")
	assert.Equal(t, uint(5), result.RetryReadMaxAttempts, "The s3-retry-read-max-attempts flag should be parsed correctly")
}

func TestAwsS3_NewFlagSet_DefaultValues(t *testing.T) {
	t.Parallel()
	awsS3 := NewAwsS3(OperationBackup)

	flagSet := awsS3.NewFlagSet()

	err := flagSet.Parse([]string{})
	assert.NoError(t, err)

	result := awsS3.GetAwsS3()

	assert.Equal(t, "", result.Region, "The default value for s3-region should be an empty string")
	assert.Equal(t, "", result.Profile, "The default value for s3-profile should be 'default'")
	assert.Equal(t, "", result.Endpoint, "The default value for s3-endpoint-override should be an empty string")
	assert.Equal(t, "", result.AccessKeyID, "The default value for s3-access-key-id should be an empty string")
	assert.Equal(t, "", result.SecretAccessKey, "The default value for s3-secret-access-key should be an empty string")
	assert.Equal(t, "", result.StorageClass, "The default value for s3-storage-class should be an empty string")
	assert.Equal(t, models.DefaultS3ChunkSize, result.ChunkSize, "The default value for s3-chunk-size should be 5mb")
	assert.Equal(t, models.DefaultS3RetryMaxAttempts, result.RetryMaxAttempts, "The default value for s3-retry-max-attempts should be 100")
	assert.Equal(t, models.DefaultS3RetryMaxBackoff, result.RetryMaxBackoff, "The default value for s3-retry-max-backoff should be 90")
	assert.Equal(t, models.DefaultS3UploadConcurrency, result.UploadConcurrency, "The default value for s3-upload-concurrency should be 1")
	assert.Equal(t, models.DefaultCloudMaxConnsPerHost, result.MaxConnsPerHost, "The default value for s3-max-conns-per-host should be 0")
	assert.Equal(t, models.DefaultCloudRequestTimeout, result.RequestTimeout, "The default value for s3-request-timeout should be 0")

	awsS3 = NewAwsS3(OperationRestore)
	flagSet = awsS3.NewFlagSet()
	err = flagSet.Parse([]string{})
	assert.NoError(t, err)
	result = awsS3.GetAwsS3()

	assert.Equal(t, models.DefaultS3RestorePollDuration, result.RestorePollDuration, "The default value for s3-restore-poll-duration should be 6000")
	assert.Equal(t, models.DefaultCloudRetryReadBackoff, result.RetryReadBackoff, "The default value for s3-retry-read-backoff should be 0")
	assert.Equal(t, models.DefaultCloudRetryReadMultiplier, result.RetryReadMultiplier, "The default value for s3-retry-read-multiplier should be 0")
	assert.Equal(t, models.DefaultCloudRetryReadMaxAttempts, result.RetryReadMaxAttempts, "The default value for s3-retry-read-max-attempts should be 0")
}
