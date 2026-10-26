// Copyright 2026 Aerospike, Inc.
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

package models

import (
	"fmt"
)

// maxSnapshotS3Throughput is the highest S3 throughput the server accepts, in Gbit/s.
const maxSnapshotS3Throughput = 400

// SnapshotS3 represents the S3 configuration of the server-integrated snapshot commands.
//
// The bucket, region, profile and endpoint are sent to the server, which transfers the
// data itself. The access keys and the client settings (retries, connections, timeout)
// are used only locally, by the S3 client that reads backup metadata and segments.
type SnapshotS3 struct {
	BucketName      string
	Region          string
	Profile         string
	Endpoint        string
	AccessKeyID     string
	SecretAccessKey string

	RetryMaxAttempts int
	RetryMaxBackoff  int
	MaxConnsPerHost  int
	RequestTimeout   int

	// Throughput is the target throughput of the server S3 client per node, in Gbit/s.
	// Like the other backup job settings, it is sent only when it is not zero, so that
	// the server applies its own default otherwise.
	Throughput int
}

// ValidateS3Throughput checks that throughput, the target throughput of the server S3
// client in Gbit/s, is within the limits the server accepts. Zero leaves it to the
// server default.
func ValidateS3Throughput(throughput int) error {
	if throughput < 0 || throughput > maxSnapshotS3Throughput {
		return fmt.Errorf("s3-throughput must be between 0 and %d, got %d",
			maxSnapshotS3Throughput, throughput)
	}

	return nil
}

// ToAwsS3 maps the snapshot S3 configuration onto the configuration of the local S3
// client. The settings the snapshot commands do not expose are filled with their
// defaults, so that the shared validation passes.
func (s *SnapshotS3) ToAwsS3() *AwsS3 {
	if s == nil {
		return nil
	}

	return &AwsS3{
		BucketName:       s.BucketName,
		Region:           s.Region,
		Endpoint:         s.Endpoint,
		Profile:          s.Profile,
		AccessKeyID:      s.AccessKeyID,
		SecretAccessKey:  s.SecretAccessKey,
		RetryMaxAttempts: s.RetryMaxAttempts,
		RetryMaxBackoff:  s.RetryMaxBackoff,
		MaxConnsPerHost:  s.MaxConnsPerHost,
		RequestTimeout:   s.RequestTimeout,
		// Set defaults to pass validation.
		RestorePollDuration:  DefaultS3RestorePollDuration,
		ChunkSize:            DefaultS3ChunkSize,
		RetryReadBackoff:     DefaultCloudRetryReadBackoff,
		RetryReadMultiplier:  DefaultCloudRetryReadMultiplier,
		RetryReadMaxAttempts: DefaultCloudRetryReadMaxAttempts,
	}
}
