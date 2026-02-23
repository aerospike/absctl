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

package models

import (
	"context"
	"fmt"

	"github.com/aerospike/backup-go"
)

// GcpStorage represents the configuration for GCP storage integration.
type GcpStorage struct {
	// Path to file containing Service Account JSON Key.
	KeyFile string
	// For GPC storage bucket is not part of the path as in S3.
	// So we should set it separately.
	BucketName string
	// Alternative url.
	// It is not recommended to use an alternate URL in a production environment.
	Endpoint string

	RetryMaxAttempts       int
	RetryBackoffMax        int
	RetryBackoffInit       int
	RetryBackoffMultiplier float64

	ChunkSize int

	StorageCommon
}

// LoadSecrets tries to load field values from secret agent.
func (g *GcpStorage) LoadSecrets(ctx context.Context, cfg *backup.SecretAgentConfig) error {
	var err error

	g.KeyFile, err = backup.ParseSecret(ctx, cfg, g.KeyFile)
	if err != nil {
		return fmt.Errorf("failed to load key file from secret agent: %w", err)
	}

	g.BucketName, err = backup.ParseSecret(ctx, cfg, g.BucketName)
	if err != nil {
		return fmt.Errorf("failed to load bucket name from secret agent: %w", err)
	}

	g.Endpoint, err = backup.ParseSecret(ctx, cfg, g.Endpoint)
	if err != nil {
		return fmt.Errorf("failed to load endpoint from secret agent: %w", err)
	}

	return nil
}

// Validate internal validation for struct params.
func (g *GcpStorage) Validate(isBackup bool) error {
	if g.BucketName == "" {
		return fmt.Errorf("bucket name is required")
	}

	if g.RetryMaxAttempts < 0 {
		return fmt.Errorf("retry maximum attempts must be non-negative")
	}

	if g.RetryBackoffMax < 0 {
		return fmt.Errorf("retry max backoff must be non-negative")
	}

	if g.RetryBackoffInit < 0 {
		return fmt.Errorf("retry backoff must be non-negative")
	}

	if g.RetryBackoffMultiplier < 1 {
		return fmt.Errorf("retry backoff multiplier must be positive")
	}

	if isBackup {
		if g.ChunkSize < 1 {
			return fmt.Errorf("chunk size can't be less than 1")
		}
	}

	if g.MaxConnsPerHost < 0 {
		return fmt.Errorf("max connections per host must be non-negative")
	}

	if g.RequestTimeout < 0 {
		return fmt.Errorf("request timeout must be non-negative")
	}

	if err := g.StorageCommon.Validate(isBackup); err != nil {
		return err
	}

	return nil
}

func (g *GcpStorage) IsConfigured() bool {
	if g == nil {
		return false
	}

	return g.BucketName != "" || g.KeyFile != "" || g.Endpoint != ""
}
