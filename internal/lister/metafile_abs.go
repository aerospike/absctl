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
	"fmt"

	"gopkg.in/yaml.v3"
)

type MetafileParserAbs struct{}

func (p *MetafileParserAbs) Parse(path string, content []byte) (*BackupEntry, error) {
	md, err := newMetadataFromBytes(content)
	if err != nil {
		return nil, fmt.Errorf("failed to parse metadata: %w", err)
	}

	return newBackupEntry(path, md), nil
}

// newMetadataFromBytes creates a new Metadata object from a byte slice.
func newMetadataFromBytes(data []byte) (*BackupMetadata, error) {
	if len(data) == 0 {
		return nil, errors.New("empty metadata file")
	}
	var metadata BackupMetadata
	err := yaml.Unmarshal(data, &metadata)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal YAML: %w", err)
	}

	err = metadata.validate()
	if err != nil {
		return nil, fmt.Errorf("corrupted metadata: %w", err)
	}

	return &metadata, nil
}
