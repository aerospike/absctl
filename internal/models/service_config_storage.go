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

import "fmt"

// ConfigStorageFields holds CLI inputs for adding or updating a single backup
// storage definition (DtoStorage). Storage is a wrapper around exactly one of
// local/s3/gcp/azure provider sub-objects; populating any field of a provider
// group implicitly selects that provider.
//
// See ConfigClusterFields for the override semantics when File is combined
// with individual flags.
type ConfigStorageFields struct {
	// Name is the storage name (URL path parameter).
	Name string

	// File is an optional path to a JSON file with a DtoStorage body.
	File string

	// LocalStorage.*
	LocalPath        string
	LocalMinPartSize int

	// S3Storage.*
	S3Bucket              string
	S3Region              string
	S3AccessKeyID         string
	S3SecretAccessKey     string
	S3Path                string
	S3EndpointOverride    string
	S3Profile             string
	S3LogLevel            string
	S3MinPartSize         int
	S3MaxAsyncConnections int
	S3SecretAgentName     string

	// GcpStorage.*
	GcpBucketName       string
	GcpKey              string
	GcpKeyFilePath      string
	GcpEndpoint         string
	GcpPath             string
	GcpMinPartSize      int
	GcpSecretAgentName  string
	GcpStorageClassData string

	// AzureStorage.*
	AzureAccountName      string
	AzureAccountKey       string
	AzureClientID         string
	AzureClientSecret     string
	AzureContainerName    string
	AzureEndpoint         string
	AzureTenantID         string
	AzurePath             string
	AzureMinPartSize      int
	AzureSecretAgentName  string
	AzureStorageClassData string
	AzureStorageClassMeta string
}

// Validate checks the bare minimum required by the CLI itself.
func (c *ConfigStorageFields) Validate() error {
	if c.Name == "" {
		return fmt.Errorf("--name is required")
	}

	return nil
}
