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

// ConfigResourceName identifies a named configuration resource (cluster, policy,
// routine or storage). Used by show/delete/enable/disable commands.
type ConfigResourceName struct {
	Name string
}

func (c *ConfigResourceName) Validate() error {
	if c.Name == "" {
		return fmt.Errorf("--name is required")
	}

	return nil
}

// ConfigUpdate holds inputs for replacing the full service configuration in a
// single PUT request.
type ConfigUpdate struct {
	// File is a path to a JSON file with the full DtoConfig body.
	File string
}

func (c *ConfigUpdate) Validate() error {
	if c.File == "" {
		return fmt.Errorf("--file is required")
	}

	return nil
}
