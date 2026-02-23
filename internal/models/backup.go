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
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/backup-go"
	"github.com/aerospike/backup-go/models"
)

// MaxRack max number of racks that can exist.
const MaxRack = 1000000

var (
	// Time parsing expressions.
	expTimeOnly = regexp.MustCompile(`^\d{2}:\d{2}:\d{2}$`)
	expDateOnly = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}$`)
	expDateTime = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}_\d{2}:\d{2}:\d{2}$`)
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

	if b.MaxRecords != 0 && b.Parallel != 1 {
		return fmt.Errorf("max-records must be used with parallel = 1")
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

// ScanPolicy map backup config to scan policy.
func (b *Backup) ScanPolicy() (*aerospike.ScanPolicy, error) {
	p := aerospike.NewScanPolicy()
	p.MaxRecords = b.MaxRecords
	p.MaxRetries = b.MaxRetries
	p.SleepBetweenRetries = time.Duration(b.SleepBetweenRetries) * time.Millisecond
	p.TotalTimeout = time.Duration(b.TotalTimeout) * time.Millisecond
	p.SocketTimeout = time.Duration(b.SocketTimeout) * time.Millisecond
	// If we selected racks we must set replica policy to aerospike.PREFER_RACK
	if b.PreferRacks != "" {
		p.ReplicaPolicy = aerospike.PREFER_RACK
	}

	if b.RackList != "" || b.NodeList != "" {
		p.ReplicaPolicy = aerospike.MASTER
	}

	if b.NoBins {
		p.IncludeBinData = false
	}

	if b.FilterExpression != "" {
		exp, err := aerospike.ExpFromBase64(b.FilterExpression)
		if err != nil {
			return nil, fmt.Errorf("failed to parse filter expression: %w", err)
		}

		p.FilterExpression = exp
	}

	return p, nil
}

// PartitionFilters map backup config to partition filters.
func (b *Backup) PartitionFilters() ([]*aerospike.PartitionFilter, error) {
	pf, err := b.resolveFilters()
	if err != nil {
		return nil, err
	}

	if err = validatePartitionFilters(pf); err != nil {
		return nil, fmt.Errorf("failed to validate partition filters: %w", err)
	}

	return pf, nil
}

// resolveFilters encapsulates the logic for choosing the filter type.
func (b *Backup) resolveFilters() ([]*aerospike.PartitionFilter, error) {
	if b.AfterDigest != "" {
		filter, err := backup.NewPartitionFilterAfterDigest(b.Namespace, b.AfterDigest)
		if err != nil {
			return nil, fmt.Errorf("failed to parse after digest filter: %w", err)
		}
		return []*aerospike.PartitionFilter{filter}, nil
	}

	if b.PartitionList != "" {
		filters, err := backup.ParsePartitionFilterListString(b.Namespace, b.PartitionList)
		if err != nil {
			return nil, fmt.Errorf("failed to parse partition filter list: %w", err)
		}
		return filters, nil
	}

	return []*aerospike.PartitionFilter{backup.NewPartitionFilterAll()}, nil
}

// Racks parses a comma-separated string of rack IDs into a slice of positive integers.
// Returns an error if any ID is invalid or exceeds the allowed maximum limit.
func (b *Backup) Racks() ([]int, error) {
	racksStringSlice := SplitByComma(b.RackList)
	racksIntSlice := make([]int, 0, len(racksStringSlice))

	for i := range racksStringSlice {
		rackID, err := strconv.Atoi(racksStringSlice[i])
		if err != nil {
			return nil, fmt.Errorf("failed to parse racks: %w", err)
		}

		if rackID < 0 {
			return nil, fmt.Errorf("rack id %d invalid, should be non-negative number", rackID)
		}

		if rackID > MaxRack {
			return nil, fmt.Errorf("rack id %d invalid, should not exceed %d", rackID, MaxRack)
		}

		racksIntSlice = append(racksIntSlice, rackID)
	}

	return racksIntSlice, nil
}

// Nodes maps the NodeList string into a slice of node names by splitting it using commas.
// Returns nil if empty.
func (b *Backup) Nodes() []string {
	return SplitByComma(b.NodeList)
}

// Sets maps the Sets string into a slice of set names by splitting it using commas.
// Returns nil if empty.
func (b *Backup) Sets() []string {
	return SplitByComma(b.SetList)
}

// Bins maps the BinList string into a slice of bin names by splitting it using commas.
// Returns nil if empty.
func (b *Backup) Bins() []string {
	return SplitByComma(b.BinList)
}

// ModifiedBeforeTime maps the ModifiedBefore string into a UTC time.
func (b *Backup) ModifiedBeforeTime() (time.Time, error) {
	return parseLocalTimeToUTC(b.ModifiedBefore)
}

// ModifiedAfterTime maps the ModifiedAfter string into a UTC time.
func (b *Backup) ModifiedAfterTime() (time.Time, error) {
	return parseLocalTimeToUTC(b.ModifiedAfter)
}

// InfoPolicy maps the backup configuration into an Aerospike InfoPolicy.
func (b *Backup) InfoPolicy() *aerospike.InfoPolicy {
	p := aerospike.NewInfoPolicy()
	p.Timeout = time.Duration(b.InfoTimeout) * time.Millisecond

	return p
}

// RetryPolicy maps backup configuration parameters to a retry policy,
// including interval, multiplier, and max retries.
func (b *Backup) RetryPolicy() *models.RetryPolicy {
	return models.NewRetryPolicy(
		time.Duration(b.InfoRetryIntervalMilliseconds)*time.Millisecond,
		b.InfoRetriesMultiplier,
		b.InfoMaxRetries,
	)
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

func validatePartitionFilters(partitionFilters []*aerospike.PartitionFilter) error {
	if len(partitionFilters) < 1 {
		return nil
	}

	beginMap := make(map[int]bool)
	intervals := make([][2]int, 0)

	for _, filter := range partitionFilters {
		switch {
		case filter.Count == 1:
			if beginMap[filter.Begin] {
				return fmt.Errorf("duplicate begin value %d for count = 1", filter.Begin)
			}

			beginMap[filter.Begin] = true
		case filter.Count > 1:
			begin := filter.Begin
			// To calculate an interval, we start from `Begin` and go till `Count`,
			// so we should do -1 as we start counting from 0.
			end := filter.Begin + filter.Count - 1
			intervals = append(intervals, [2]int{begin, end})
		default:
			return fmt.Errorf("invalid partition filter count: %d", filter.Count)
		}
	}

	sort.Slice(intervals, func(i, j int) bool {
		return intervals[i][0] < intervals[j][0]
	})

	for i := 1; i < len(intervals); i++ {
		prevEnd := intervals[i-1][1]
		currBegin := intervals[i][0]

		if currBegin <= prevEnd {
			return fmt.Errorf("overlapping intervals: [%d, %d] and [%d, %d]",
				intervals[i-1][0], prevEnd, currBegin, intervals[i][1])
		}
	}

	return nil
}

// SplitByComma splits a comma-separated string into a slice of strings. Returns nil if the input string is empty.
func SplitByComma(s string) []string {
	if s == "" {
		return nil
	}

	return strings.Split(s, ",")
}

func parseLocalTimeToUTC(timeString string) (time.Time, error) {
	location, err := time.LoadLocation("Local")
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to load timezone location: %w", err)
	}

	var validTime string

	switch {
	case expDateTime.MatchString(timeString):
		validTime = timeString
	case expTimeOnly.MatchString(timeString):
		currentTime := time.Now().In(location)
		validTime = currentTime.Format("2006-01-02") + "_" + timeString
	case expDateOnly.MatchString(timeString):
		validTime = timeString + "_00:00:00"
	default:
		return time.Time{}, fmt.Errorf("unknown time format: %s", timeString)
	}

	localTime, err := time.ParseInLocation("2006-01-02_15:04:05", validTime, location)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to parse time %s: %w", timeString, err)
	}

	utcTime := localTime.UTC()

	return utcTime, nil
}
