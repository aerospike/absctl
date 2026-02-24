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

//nolint:dupl //This file is not a duplication of aws_s3.
package models

import (
	"context"
	"fmt"

	"github.com/aerospike/backup-go"
)

// AzureBlob represents the configuration for Azure Blob storage integration.
type AzureBlob struct {
	// Account name + key auth
	AccountName string
	AccountKey  string
	// Azure Active directory
	TenantID     string
	ClientID     string
	ClientSecret string // #nosec G117

	Endpoint      string
	ContainerName string

	AccessTier          string
	RestorePollDuration int64

	RetryMaxAttempts int
	RetryDelay       int
	RetryMaxDelay    int

	BlockSize         int
	UploadConcurrency int

	StorageCommon
}

// LoadSecrets tries to load field values from secret agent.
func (a *AzureBlob) LoadSecrets(ctx context.Context, cfg *backup.SecretAgentConfig) error {
	if a == nil {
		return nil
	}

	var err error

	a.AccountName, err = backup.ParseSecret(ctx, cfg, a.AccountName)
	if err != nil {
		return fmt.Errorf("failed to load account name from secret agent: %w", err)
	}

	a.AccountKey, err = backup.ParseSecret(ctx, cfg, a.AccountKey)
	if err != nil {
		return fmt.Errorf("failed to load account key from secret agent: %w", err)
	}

	a.TenantID, err = backup.ParseSecret(ctx, cfg, a.TenantID)
	if err != nil {
		return fmt.Errorf("failed to load tenant id from secret agent: %w", err)
	}

	a.ClientID, err = backup.ParseSecret(ctx, cfg, a.ClientID)
	if err != nil {
		return fmt.Errorf("failed to load client id from secret agent: %w", err)
	}

	a.ClientSecret, err = backup.ParseSecret(ctx, cfg, a.ClientSecret)
	if err != nil {
		return fmt.Errorf("failed to load client secret from secret agent: %w", err)
	}

	a.Endpoint, err = backup.ParseSecret(ctx, cfg, a.Endpoint)
	if err != nil {
		return fmt.Errorf("failed to load endpoint from secret agent: %w", err)
	}

	a.ContainerName, err = backup.ParseSecret(ctx, cfg, a.ContainerName)
	if err != nil {
		return fmt.Errorf("failed to load container name from secret agent: %w", err)
	}

	a.AccessTier, err = backup.ParseSecret(ctx, cfg, a.AccessTier)
	if err != nil {
		return fmt.Errorf("failed to load access tier key from secret agent: %w", err)
	}

	return nil
}

// Validate internal validation for struct params.
func (a *AzureBlob) Validate(isBackup bool) error {
	if a.ContainerName == "" {
		return fmt.Errorf("container name is required")
	}

	if a.Endpoint == "" {
		return fmt.Errorf("endpoint is required")
	}

	if a.RetryMaxAttempts < 0 {
		return fmt.Errorf("retry maximum attempts must be non-negative")
	}

	if a.RetryDelay < 0 {
		return fmt.Errorf("retry delay must be non-negative")
	}

	if a.RetryMaxDelay < 0 {
		return fmt.Errorf("retry max delay must be non-negative")
	}

	switch isBackup {
	case true:
		if a.BlockSize < 1 {
			return fmt.Errorf("block size can't be less than 1")
		}

		if a.UploadConcurrency < 1 {
			return fmt.Errorf("upload concurrency can't be less than 1")
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

func (a *AzureBlob) IsConfigured() bool {
	if a == nil {
		return false
	}

	return a.ContainerName != "" || a.AccountName != "" || a.AccountKey != "" ||
		a.Endpoint != "" || a.TenantID != "" || a.ClientID != "" ||
		a.ClientSecret != ""
}
