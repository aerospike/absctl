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
	"strings"

	"github.com/aerospike/backup-go"
)

const noneVal = "NONE"

// Compression contains flags that will be mapped to CompressionPolicy for backup and restore operations.
type Compression struct {
	Mode  string
	Level int
}

func (c *Compression) ToPolicy() *backup.CompressionPolicy {
	if c == nil {
		return nil
	}

	if c.Mode == "" || strings.EqualFold(c.Mode, noneVal) {
		return nil
	}

	return backup.NewCompressionPolicy(strings.ToUpper(c.Mode), c.Level)
}
