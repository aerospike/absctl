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
	"fmt"
	"time"
)

// ServerBackup contains flags that will be mapped to ServerBackup.
type ServerBackup struct {
	ServerCommon

	ModifiedBefore     string
	ModifiedAfter      string
	SetList            string
	NoIndexes          bool
	NoUDFs             bool
	EnableChangeStream bool
}

func (s *ServerBackup) Validate() error {
	if s == nil {
		return nil
	}

	var modifiedAfter, modifiedBefore time.Time

	if s.ModifiedAfter != "" {
		ma, err := s.ModifiedAfterTime()
		if err != nil {
			return fmt.Errorf("failed to parse modified after: %w", err)
		}
		modifiedAfter = ma
	}

	if s.ModifiedBefore != "" {
		mb, err := s.ModifiedBeforeTime()
		if err != nil {
			return fmt.Errorf("failed to parse modified before: %w", err)
		}
		modifiedBefore = mb
	}

	if s.ModifiedAfter != "" && s.ModifiedBefore != "" && !modifiedBefore.After(modifiedAfter) {
		return fmt.Errorf("modified-before must be strictly greater than modified-after")
	}

	return s.ServerCommon.Validate()
}

// ModifiedBeforeTime maps the ModifiedBefore string into a UTC time.
func (s *ServerBackup) ModifiedBeforeTime() (time.Time, error) {
	return ParseLocalTimeToUTC(s.ModifiedBefore)
}

// ModifiedAfterTime maps the ModifiedAfter string into a UTC time.
func (s *ServerBackup) ModifiedAfterTime() (time.Time, error) {
	return ParseLocalTimeToUTC(s.ModifiedAfter)
}

type ServerBackupList struct {
	// ListPath is the path to list backups from.
	ListPath string
}

func (s *ServerBackupList) Validate() error {
	if s == nil {
		return nil
	}

	if s.ListPath == "" {
		return fmt.Errorf("list-path is required")
	}

	return nil
}

type ServerBackupValidate struct {
	// ListPath is the path to list backups from.
	JobID string
	// SampleSize specifies the sample size limit for validation operations.
	SampleSize int
}

func (s *ServerBackupValidate) Validate() error {
	if s == nil {
		return nil
	}

	if s.JobID == "" {
		return fmt.Errorf("backup-id is required")
	}

	return nil
}

// ServerBackupProgress contains flags that will be mapped to ServerBackupProgress.
type ServerBackupProgress struct {
	JobID string
	ServerCommon
}

func (s *ServerBackupProgress) Validate() error {
	if s == nil {
		return nil
	}

	if s.JobID == "" {
		return fmt.Errorf("backup-id is required")
	}

	return s.ServerCommon.Validate()
}
