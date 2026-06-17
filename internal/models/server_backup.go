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

// ServerBackup contains flags that will be mapped to ServerBackup.
type ServerBackup struct {
	Namespace   string
	JobID       string
	StorageType string
	// ListPath is the path to list backups from.
	ListPath string

	// SampleSize specifies the sample size limit for validation operations.
	SampleSize int
}
