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

package lister

import (
	"errors"
	"time"
)

type BackupEntry struct {
	path string

	*BackupMetadata
}

func newBackupEntry(path string, backupMetadata *BackupMetadata) *BackupEntry {
	return &BackupEntry{
		path:           path,
		BackupMetadata: backupMetadata,
	}
}

// BackupMetadata is an internal container for storing backup metadata.
// It is stored as a separate metadata file within each backup.
type BackupMetadata struct {
	// The backup time in the ISO 8601 format.
	Created time.Time `yaml:"created" json:"created"`
	// The time the backup operation completed.
	Finished time.Time `yaml:"finished" json:"finished"`
	// The lower time bound of backup entities in the ISO 8601 format (for incremental backups).
	// It's 0 for full backups.
	From time.Time `yaml:"from" json:"from"`
	// The namespace of a backup.
	Namespace string `yaml:"namespace" json:"namespace"`
	// The total number of records backed up.
	RecordCount uint64 `yaml:"record-count" json:"record-count"`
	// The size of the backup in bytes.
	ByteCount uint64 `yaml:"byte-count" json:"byte-count"`
	// The number of backup files created.
	FileCount uint64 `yaml:"file-count" json:"file-count"`
	// The number of secondary indexes backed up.
	SecondaryIndexCount uint64 `yaml:"secondary-index-count" json:"secondary-index-count"`
	// The number of UDF files backed up.
	UDFCount uint64 `yaml:"udf-count" json:"udf-count"`
	// Compression specifies the compression mode used for the backup (ZSTD or NONE).
	Compression string
	// Encryption specifies the encryption mode used for the backup (NONE, AES128, AES256).
	Encryption string
}

func (m *BackupMetadata) validate() error {
	if m.Created.IsZero() {
		return errors.New("`created` is required")
	}

	if m.Finished.IsZero() {
		return errors.New("`finished` is required")
	}

	if m.Namespace == "" {
		return errors.New("`namespace` is required")
	}

	return nil
}
