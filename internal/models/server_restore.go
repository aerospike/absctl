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

// ServerRestore contains flags that will be mapped to ServerRestore.
type ServerRestore struct {
	ServerCommon
	JobID        string
	Path         string
	FuzzyRestore bool
}

func (s *ServerRestore) Validate() error {
	if s == nil {
		return nil
	}

	if s.JobID == "" {
		return fmt.Errorf("backup-id is required")
	}

	if s.StorageType == "" {
		return fmt.Errorf("storage-type is required")
	}

	return s.ServerCommon.Validate()
}

// ServerRestorePrepare contains flags that will be mapped to ServerRestorePrepare.
type ServerRestorePrepare struct {
	ServerCommon
	JobID string
}

func (s *ServerRestorePrepare) Validate() error {
	if s == nil {
		return nil
	}

	if s.JobID == "" {
		return fmt.Errorf("backup-id is required")
	}

	return s.ServerCommon.Validate()
}

// ServerRestoreProgress contains flags that will be mapped to ServerRestorePrepare.
type ServerRestoreProgress struct {
	ServerCommon
}

func (s *ServerRestoreProgress) Validate() error {
	if s == nil {
		return nil
	}

	return s.ServerCommon.Validate()
}
