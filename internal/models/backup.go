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
)

// Backup flags that will be mapped to (scan) backup config.
// (common for backup and restore flags are in Common).
type Backup struct {
	Common
	MaxRetries          int
	OutputFile          string
	RemoveFiles         bool
	ModifiedBefore      string
	ModifiedAfter       string
	FileLimit           uint64
	AfterDigest         string
	MaxRecords          int64
	NoBins              bool
	SleepBetweenRetries int
	FilterExpression    string
	RemoveArtifacts     bool
	Compact             bool
	NodeList            string
	NoTTLOnly           bool
	PreferRacks         string
	PartitionList       string
	Estimate            bool
	EstimateSamples     int64
	StateFileDst        string
	Continue            string
	ScanPageSize        int64
	OutputFilePrefix    string
	RackList            string
}

// ShouldClearTarget check if we should clean target directory.
func (b *Backup) ShouldClearTarget() bool {
	return (b.RemoveFiles || b.RemoveArtifacts) && b.Continue == ""
}

func (b *Backup) ShouldSaveState() bool {
	return b.StateFileDst != "" || b.Continue != ""
}

//nolint:gocyclo // Long validation function.
func (b *Backup) Validate() error {
	if b == nil {
		return nil
	}

	if !b.Estimate && b.OutputFile == "" && b.Directory == "" {
		return fmt.Errorf("must specify either estimate, output-file or directory")
	}

	if b.Directory != "" && b.OutputFile != "" {
		return fmt.Errorf("only one of output-file and directory may be configured at the same time")
	}

	if err := validateFilePrefix(b.OutputFilePrefix); err != nil {
		return fmt.Errorf("invalid output-file-prefix: %w", err)
	}

	if b.OutputFile != "" && b.OutputFilePrefix != "" {
		return fmt.Errorf("using output-file-prefix is not allowed with output-file")
	}

	// Only one filter is allowed.
	if err := b.validateSingleFilter(); err != nil {
		return err
	}

	if b.Continue != "" && b.StateFileDst != "" {
		return fmt.Errorf("continue and state-file-dst are mutually exclusive")
	}

	if b.Continue != "" && b.RemoveFiles {
		return fmt.Errorf("continue and remove-files are mutually exclusive, as remove-files will delete the backup files")
	}

	if b.Estimate {
		// Estimate with filter not allowed.
		if b.PartitionList != "" ||
			b.NodeList != "" ||
			b.AfterDigest != "" ||
			b.FilterExpression != "" ||
			b.ModifiedAfter != "" ||
			b.ModifiedBefore != "" ||
			b.NoTTLOnly {
			return fmt.Errorf("estimate with any filter is not allowed")
		}
		// For estimate directory or file must not be set.
		if b.OutputFile != "" || b.Directory != "" {
			return fmt.Errorf("estimate with output-file or directory is not allowed")
		}
		// Check estimate samples size.
		if b.EstimateSamples < 0 {
			return fmt.Errorf("estimate with estimate-samples < 0 is not allowed")
		}
	}

	// Validate nested common in the end.
	return b.Common.Validate()
}

// validateSingleFilter ensures only one filtering option is specified.
func (b *Backup) validateSingleFilter() error {
	filtersSet := 0
	setFilters := make([]string, 0, 4)

	if b.AfterDigest != "" {
		filtersSet++

		setFilters = append(setFilters, "after-digest")
	}

	if b.PartitionList != "" {
		filtersSet++

		setFilters = append(setFilters, "partition-list")
	}

	if b.NodeList != "" {
		filtersSet++

		setFilters = append(setFilters, "node-list")
	}

	if b.RackList != "" {
		filtersSet++

		setFilters = append(setFilters, "rack-list")
	}

	if filtersSet > 1 {
		return fmt.Errorf("only one of %s can be configured", strings.Join(setFilters, " or "))
	}

	return nil
}

func validateFilePrefix(prefix string) error {
	if prefix == "" {
		return nil
	}

	invalidChars := `\/:*?"<>|`

	// Check for invalid characters
	for i, char := range prefix {
		if strings.ContainsRune(invalidChars, char) {
			return fmt.Errorf(
				"file prefix contains invalid character '%c' at position %d: %q",
				char, i, prefix,
			)
		}

		// Check for controlling symbols (ASCII 0-31)
		if char < 32 {
			return fmt.Errorf(
				"file prefix contains control character (0x%02X) at position %d: %q",
				char, i, prefix,
			)
		}

		// Check for DEL symbol (127)
		if char == 127 {
			return fmt.Errorf(
				"file prefix contains DEL character at position %d: %q",
				i, prefix,
			)
		}
	}

	// Check for whitespace at start/end
	trimmed := strings.TrimSpace(prefix)
	if trimmed != prefix {
		return fmt.Errorf(
			"file prefix cannot start or end with whitespace: %q",
			prefix,
		)
	}

	return nil
}
