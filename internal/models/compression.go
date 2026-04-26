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
	"strings"

	"github.com/aerospike/backup-go"
)

const (
	compressionModeNone = "NONE"
	compressionModeZstd = "ZSTD"
)

// Compression contains flags that will be mapped to CompressionPolicy for backup and restore operations.
type Compression struct {
	Mode  string
	Level int
}

// Policy converts Compression to backup.CompressionPolicy.
func (c *Compression) Policy() *backup.CompressionPolicy {
	if c == nil {
		return nil
	}

	if c.Mode == "" || strings.EqualFold(c.Mode, compressionModeNone) {
		return nil
	}

	return backup.NewCompressionPolicy(strings.ToUpper(c.Mode), c.Level)
}

func (c *Compression) Validate() error {
	if c.Mode != "" {
		if !strings.EqualFold(c.Mode, compressionModeNone) &&
			!strings.EqualFold(c.Mode, compressionModeZstd) {
			return fmt.Errorf("invalid compression mode: %s", c.Mode)
		}
	}

	if c.Level > 0 && (c.Mode == "") {
		return fmt.Errorf("--compress is required when --compression-level is set")
	}

	return nil
}
