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
	"encoding/json"
	"fmt"
)

// Root represents the root backup metadata
type Root struct {
	BackupID      string      `json:"backup_id"`
	Namespace     string      `json:"namespace"`
	FormatVersion int         `json:"format_version"`
	NodeID        string      `json:"node_id"`
	Partitions    []Partition `json:"partitions"`
}

type Partition struct {
	PartitionID  int    `json:"partition_id"`
	ManifestName string `json:"manifest_name"`
}

// Manifest represents a partition manifest
type Manifest struct {
	BackupID          string    `json:"backup_id"`
	Namespace         string    `json:"namespace"`
	PartitionID       int       `json:"partition_id"`
	FormatVersion     int       `json:"format_version"`
	NodeID            string    `json:"node_id"`
	ChecksumAlgorithm string    `json:"checksum_algorithm"`
	IDRangeStart      int       `json:"id_range_start"`
	IDRangeEnd        int       `json:"id_range_end"`
	EntryCount        int       `json:"entry_count"`
	Segments          []Segment `json:"segments"`
}

type Segment struct {
	SegmentID   int    `json:"segment_id"`
	SegmentName string `json:"segment_name"`
	Size        int    `json:"size"`
	Checksum    string `json:"checksum"`
}

type MetafileParserSSb struct{}

func (p *MetafileParserSSb) Parse(path string, content []byte) (*BackupEntry, error) {
	root, err := parseRoot(content)
	if err != nil {
		return nil, fmt.Errorf("failed to parse root: %w", err)
	}

	return newBackupFromRoot(path, root), nil
}

func parseRoot(data []byte) (*Root, error) {
	var r Root
	if err := json.Unmarshal(data, &r); err != nil {
		return nil, err
	}
	return &r, nil
}

func parseManifest(data []byte) (*Manifest, error) {
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return &m, nil
}
