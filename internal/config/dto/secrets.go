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

package dto

import (
	"context"
	"fmt"
	"strings"

	"github.com/aerospike/backup-go"
)

const secretsPrefix = "secrets:"

// collectSecretableFields gathers all string pointer fields that may contain
// secret agent references across the various config sections.
func collectSecretableFields(
	cluster *Cluster,
	encryption *Encryption,
	s3 *AwsS3,
	azure *AzureBlob,
	gcp *GcpStorage,
) []*string {
	var fields []*string

	// Cluster auth.
	fields = append(fields, cluster.User, cluster.Password)

	// TLS.
	if cluster.TLS != nil {
		fields = append(fields,
			cluster.TLS.CaFile, cluster.TLS.CertFile,
			cluster.TLS.KeyFile, cluster.TLS.KeyFilePassword,
		)
	}

	// Encryption.
	if encryption != nil {
		fields = append(fields, encryption.KeyFile, encryption.KeyEnv)
	}

	// AWS S3.
	if s3 != nil {
		fields = append(fields,
			s3.BucketName, s3.Region, s3.Profile,
			s3.EndpointOverride, s3.AccessKeyID, s3.SecretAccessKey,
			s3.StorageClass, s3.AccessTier,
		)
	}

	// Azure Blob.
	if azure != nil {
		fields = append(fields,
			azure.AccountName, azure.AccountKey,
			azure.TenantID, azure.ClientID,
			azure.ClientSecret, azure.EndpointOverride,
			azure.ContainerName, azure.AccessTier,
		)
	}

	// GCP Storage.
	if gcp != nil {
		fields = append(fields, gcp.KeyFile, gcp.BucketName, gcp.EndpointOverride)
	}

	return fields
}

// resolveSecretFields iterates over string pointer fields and resolves any
// that start with the secrets: prefix via the secret agent.
func resolveSecretFields(ctx context.Context, saCfg *backup.SecretAgentConfig, fields ...*string) error {
	for _, field := range fields {
		if field == nil || *field == "" {
			continue
		}

		if !strings.HasPrefix(*field, secretsPrefix) {
			continue
		}

		resolved, err := backup.ParseSecret(ctx, saCfg, *field)
		if err != nil {
			return fmt.Errorf("failed to resolve secret %q: %w", *field, err)
		}

		*field = resolved
	}

	return nil
}
