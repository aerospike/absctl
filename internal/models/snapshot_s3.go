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
	"errors"
	"fmt"
	"strings"
	"unicode"
)

// maxSnapshotS3Throughput is the highest S3 throughput the server accepts, in Gbit/s.
const maxSnapshotS3Throughput = 400

// Limits of the S3 settings the server is sent, as enforced by the server.
const (
	maxServerS3BucketLen   = 255
	maxServerS3RegionLen   = 63
	maxServerS3ProfileLen  = 63
	maxServerS3EndpointLen = 511
)

// unsetEndpointValues are values the server refuses as an endpoint, since they read as
// an endpoint someone meant to leave unset.
var unsetEndpointValues = []string{"none", "null", "nil", "undefined", "(none)", "n/a"}

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

// ValidateServerS3 checks the S3 settings a start command sends to the server against
// the rules the server applies: the bucket and the region are required, and every
// setting is printable ASCII within its length limit. The access keys and the client
// settings stay local and are not checked here.
func ValidateServerS3(a *AwsS3) error {
	// An unconfigured section is reported by the object storage type check first.
	if a == nil {
		return errors.New("object storage is not configured")
	}

	switch {
	case a.BucketName == "":
		return errors.New("s3-bucket-name is required")
	case a.Region == "":
		return errors.New("s3-region is required")
	}

	if err := validateServerText("s3-bucket-name", a.BucketName, maxServerS3BucketLen); err != nil {
		return err
	}

	if err := validateServerText("s3-region", a.Region, maxServerS3RegionLen); err != nil {
		return err
	}

	if err := validateServerText("s3-profile", a.Profile, maxServerS3ProfileLen); err != nil {
		return err
	}

	return validateServerS3Endpoint(a.Endpoint)
}

// validateServerS3Endpoint checks the endpoint, when it is set: an http:// or https://
// URL with a host, the scheme in lower case as the server matches it case-sensitively.
func validateServerS3Endpoint(endpoint string) error {
	const name = "s3-endpoint-override"

	if endpoint == "" {
		return nil
	}

	if err := validateServerText(name, endpoint, maxServerS3EndpointLen); err != nil {
		return err
	}

	for _, unset := range unsetEndpointValues {
		if strings.EqualFold(endpoint, unset) {
			return fmt.Errorf("%s looks like an unset value, omit it to use the default AWS endpoint", name)
		}
	}

	if strings.ContainsFunc(endpoint, unicode.IsSpace) {
		return fmt.Errorf("%s must not contain whitespace", name)
	}

	var host string

	switch lower := strings.ToLower(endpoint); {
	case strings.HasPrefix(endpoint, "http://"):
		host = strings.TrimPrefix(endpoint, "http://")
	case strings.HasPrefix(endpoint, "https://"):
		host = strings.TrimPrefix(endpoint, "https://")
	case strings.HasPrefix(lower, "http://"), strings.HasPrefix(lower, "https://"):
		return fmt.Errorf("%s scheme must be lowercase", name)
	default:
		return fmt.Errorf("%s scheme must be http:// or https://", name)
	}

	if host == "" || strings.HasPrefix(host, "/") {
		return fmt.Errorf("%s has no host", name)
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
