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

// RestoreJobID identifies a single restore job by ID.
type RestoreJobID struct {
	JobID int64
}

func (r *RestoreJobID) Validate() error {
	if r.JobID <= 0 {
		return fmt.Errorf("job-id must be a positive integer")
	}

	return nil
}

// RestoreJobsFilter holds filters for listing restore jobs.
type RestoreJobsFilter struct {
	From   int64
	To     int64
	Status string
}

// RestoreRequest holds inputs for full/incremental restore operations.
// Either RequestFile or BackupDataPath must be set.
// Flag values override fields loaded from RequestFile when both are provided.
type RestoreRequest struct {
	// RequestFile is a path to a JSON file containing the full DtoRestoreRequest body.
	RequestFile string

	// BackupDataPath is the path to the backup data inside the storage root.
	BackupDataPath string

	// DestinationName references a preconfigured destination cluster by name.
	DestinationName string

	// SourceName references a preconfigured storage source by name.
	SourceName string

	// SecretAgentName references a preconfigured secret agent by name.
	SecretAgentName string
}

func (r *RestoreRequest) Validate() error {
	if r.RequestFile == "" && r.BackupDataPath == "" {
		return fmt.Errorf("either --request-file or --backup-data-path must be provided")
	}

	return nil
}

// RestoreTimestampRequest holds inputs for the timestamp-based restore operation.
// Either RequestFile or both Routine and Time must be set.
type RestoreTimestampRequest struct {
	// RequestFile is a path to a JSON file containing the full DtoRestoreTimestampRequest body.
	RequestFile string

	// Routine is the backup routine name to restore from.
	Routine string

	// Time is the epoch time in milliseconds for point-in-time recovery.
	Time int64

	// DestinationName references a preconfigured destination cluster by name.
	DestinationName string

	// SecretAgentName references a preconfigured secret agent by name.
	SecretAgentName string

	// DisableReordering disables the reverse order of incremental backups optimization.
	DisableReordering bool
}

func (r *RestoreTimestampRequest) Validate() error {
	if r.RequestFile != "" {
		return nil
	}

	if r.Routine == "" {
		return fmt.Errorf("--routine is required when --request-file is not provided")
	}

	if r.Time <= 0 {
		return fmt.Errorf("--time must be a positive epoch in milliseconds when --request-file is not provided")
	}

	return nil
}
