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

// BackupRoutine identifies a backup routine by name. Used by cancel and status commands.
type BackupRoutine struct {
	Name string
}

func (b *BackupRoutine) Validate() error {
	if b.Name == "" {
		return fmt.Errorf("routine name is required")
	}

	return nil
}

// BackupListFilter holds filters for listing full or incremental backups.
// All fields are optional.
type BackupListFilter struct {
	// Name is an optional backup routine name. If empty, backups for all routines are returned.
	Name string

	// From is an optional lower-bound timestamp filter (Unix milliseconds).
	From int64

	// To is an optional upper-bound timestamp filter (Unix milliseconds).
	To int64
}

// BackupTrigger holds inputs for triggering full or incremental backups.
type BackupTrigger struct {
	// Name is the backup routine name to trigger.
	Name string

	// Delay is the optional delay before the backup starts, in milliseconds.
	Delay int
}

func (b *BackupTrigger) Validate() error {
	if b.Name == "" {
		return fmt.Errorf("routine name is required")
	}

	if b.Delay < 0 {
		return fmt.Errorf("delay must be non-negative")
	}

	return nil
}
