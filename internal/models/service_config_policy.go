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

// ConfigPolicyFields holds CLI inputs for adding or updating a single backup
// policy (DtoBackupPolicy). See ConfigClusterFields for the override semantics
// when File is combined with individual flags.
type ConfigPolicyFields struct {
	// Name is the policy name (URL path parameter).
	Name string

	// File is an optional path to a JSON file with a DtoBackupPolicy body.
	File string

	Bandwidth                int
	FileLimit                int
	MaxConcurrentNodes       int
	Parallel                 int
	ParallelWrite            int
	RecordsPerSecond         int
	SocketTimeout            int
	TotalTimeout             int
	ConcurrentIncremental    bool
	NoIndexes                bool
	NoRecords                bool
	NoUdfs                   bool
	Sealed                   bool
	UseScanCompression       bool
	WithClusterConfiguration bool

	// Compression.* override fields under the compression object.
	CompressionMode  string
	CompressionLevel int

	// Encryption.* override fields under the encryption object.
	EncryptionMode      string
	EncryptionKeyEnv    string
	EncryptionKeyFile   string
	EncryptionKeySecret string

	// Retention.* override fields under the retention object.
	RetentionFull        int
	RetentionIncremental int

	// Retry.* override fields under the retry-policy object.
	RetryBaseTimeout int
	RetryMaxRetries  int
	RetryMultiplier  float32
}

// Validate checks the bare minimum required by the CLI itself.
func (c *ConfigPolicyFields) Validate() error {
	if c.Name == "" {
		return fmt.Errorf("--name is required")
	}

	return nil
}
