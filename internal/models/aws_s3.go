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

//nolint:dupl //This file is not a duplication of azure_blob.
package models

import (
	"context"
	"fmt"

	"github.com/aerospike/backup-go"
)

// AwsS3 represents the configuration for AWS S3 storage integration.
type AwsS3 struct {
	BucketName      string
	Region          string
	Profile         string
	Endpoint        string
	AccessKeyID     string
	SecretAccessKey string

	StorageClass        string
	AccessTier          string
	RestorePollDuration int64

	RetryMaxAttempts int
	RetryMaxBackoff  int

	ChunkSize         int
	UploadConcurrency int

	StorageCommon
}

// LoadSecrets tries to load field values from secret agent.
func (a *AwsS3) LoadSecrets(ctx context.Context, cfg *backup.SecretAgentConfig) error {
	var err error

	a.BucketName, err = backup.ParseSecret(ctx, cfg, a.BucketName)
	if err != nil {
		return fmt.Errorf("failed to load bucket name from secret agent: %w", err)
	}

	a.Region, err = backup.ParseSecret(ctx, cfg, a.Region)
	if err != nil {
		return fmt.Errorf("failed to load region from secret agent: %w", err)
	}

	a.Profile, err = backup.ParseSecret(ctx, cfg, a.Profile)
	if err != nil {
		return fmt.Errorf("failed to load profile from secret agent: %w", err)
	}

	a.Endpoint, err = backup.ParseSecret(ctx, cfg, a.Endpoint)
	if err != nil {
		return fmt.Errorf("failed to load endpoint from secret agent: %w", err)
	}

	a.AccessKeyID, err = backup.ParseSecret(ctx, cfg, a.AccessKeyID)
	if err != nil {
		return fmt.Errorf("failed to load access key id from secret agent: %w", err)
	}

	a.SecretAccessKey, err = backup.ParseSecret(ctx, cfg, a.SecretAccessKey)
	if err != nil {
		return fmt.Errorf("failed to load secret access key from secret agent: %w", err)
	}

	a.StorageClass, err = backup.ParseSecret(ctx, cfg, a.StorageClass)
	if err != nil {
		return fmt.Errorf("failed to load storage class from secret agent: %w", err)
	}

	a.AccessTier, err = backup.ParseSecret(ctx, cfg, a.AccessTier)
	if err != nil {
		return fmt.Errorf("failed to load access tier key from secret agent: %w", err)
	}

	return nil
}

// Validate internal validation for struct params.
func (a *AwsS3) Validate(isBackup bool) error {
	if a.BucketName == "" {
		return fmt.Errorf("bucket name is required")
	}

	if a.RetryMaxAttempts < 0 {
		return fmt.Errorf("retry maximum attempts must be non-negative")
	}

	if a.RetryMaxBackoff < 0 {
		return fmt.Errorf("retry max backoff must be non-negative")
	}

	switch isBackup {
	case true:
		if a.ChunkSize < 5 {
			return fmt.Errorf("chunk size can't be less than 5")
		}

		if a.UploadConcurrency < 0 {
			return fmt.Errorf("upload concurrency can't be less than 0")
		}
	case false:
		if a.RestorePollDuration < 1 {
			return fmt.Errorf("restore poll duration can't be less than 1")
		}
	}

	if err := a.StorageCommon.Validate(isBackup); err != nil {
		return err
	}

	return nil
}

func (a *AwsS3) IsConfigured() bool {
	if a == nil {
		return false
	}

	return a.BucketName != "" || a.Region != "" || a.Profile != "" || a.Endpoint != ""
}
