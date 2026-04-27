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

// ConfigRoutineFields holds CLI inputs for adding or updating a single backup
// routine (DtoBackupRoutine). See ConfigClusterFields for the override
// semantics when File is combined with individual flags.
type ConfigRoutineFields struct {
	// Name is the routine name (URL path parameter).
	Name string

	// File is an optional path to a JSON file with a DtoBackupRoutine body.
	File string

	BackupPolicy     string
	SourceCluster    string
	Storage          string
	SecretAgent      string
	IntervalCron     string
	IncrIntervalCron string
	PartitionList    string
	Disabled         bool

	Namespaces []string
	BinList    []string
	SetList    []string
	NodeList   []string
	RackList   []int
}

// Validate checks the bare minimum required by the CLI itself.
func (c *ConfigRoutineFields) Validate() error {
	if c.Name == "" {
		return fmt.Errorf("--name is required")
	}

	return nil
}
