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
	"time"

	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/backup-go"
	"github.com/aerospike/backup-go/models"
)

const (
	RestoreModeAuto = "auto"
	RestoreModeASB  = "asb"
	RestoreModeASBX = "asbx"
)

// Restore contains flags that will be mapped to restore config.
type Restore struct {
	Common

	InputFile          string
	DirectoryList      string
	ParentDirectory    string
	DisableBatchWrites bool
	BatchSize          int
	MaxAsyncBatches    int
	// For optimal performance, should be at least MaxAsyncBatches.
	// This is applicable only to batch writes.
	WarmUp            int
	ExtraTTL          int64
	IgnoreRecordError bool
	Uniq              bool
	Replace           bool
	NoGeneration      bool

	RetryBaseInterval int64
	RetryMultiplier   float64
	RetryMaxAttempts  uint

	Mode string

	ValidateOnly      bool
	ApplyMetadataLast bool
}

func (r *Restore) IsDirectoryRestore() bool {
	return r.DirectoryList == "" && r.InputFile == ""
}

func (r *Restore) Validate() error {
	if r == nil {
		return nil
	}

	switch r.Mode {
	case RestoreModeAuto, RestoreModeASB, RestoreModeASBX:
		// ok.
	default:
		return fmt.Errorf("invalid restore mode: %s", r.Mode)
	}

	if r.InputFile == "" &&
		r.Directory == "" &&
		r.DirectoryList == "" {
		return fmt.Errorf("input file or directory required")
	}

	if r.Directory != "" && r.InputFile != "" {
		return fmt.Errorf("only one of directory and input-file may be configured at the same time")
	}

	if r.DirectoryList != "" && (r.Directory != "" || r.InputFile != "") {
		return fmt.Errorf("only one of directory, input-file and directory-list may be configured at the same time")
	}

	if r.ParentDirectory != "" && r.DirectoryList == "" {
		return fmt.Errorf("must specify directory-list list")
	}

	if r.WarmUp < 0 {
		return fmt.Errorf("warm-up must be non-negative")
	}

	if !r.ValidateOnly {
		// Validate common backup only if restore is not in validate only mode.
		if err := r.Common.Validate(); err != nil {
			return err
		}
	}

	return nil
}

// NamespaceConfig creates and returns a RestoreNamespaceConfig with source and destination namespaces
// derived from input. Took value from r.Namespace. If one namespace is provided,
// it sets both source and destination to the same value.
// Returns nil if invalid input (e.g., more than two namespaces) is provided.
func (r *Restore) NamespaceConfig() *backup.RestoreNamespaceConfig {
	nsArr := splitByComma(r.Namespace)

	var source, destination string

	switch len(nsArr) {
	case 1:
		source, destination = nsArr[0], nsArr[0]
	case 2:
		source, destination = nsArr[0], nsArr[1]
	default:
		return nil
	}

	return &backup.RestoreNamespaceConfig{
		Source:      &source,
		Destination: &destination,
	}
}

// WritePolicy map restore config to write policy.
func (r *Restore) WritePolicy() *aerospike.WritePolicy {
	p := aerospike.NewWritePolicy(0, 0)

	p.SendKey = true
	p.TotalTimeout = time.Duration(r.TotalTimeout) * time.Millisecond
	p.SocketTimeout = time.Duration(r.SocketTimeout) * time.Millisecond
	p.RecordExistsAction = recordExistsAction(r.Replace, r.Uniq)
	p.GenerationPolicy = aerospike.EXPECT_GEN_GT

	if r.NoGeneration {
		p.GenerationPolicy = aerospike.NONE
	}

	return p
}

// InfoPolicy maps the restore configuration into an Aerospike InfoPolicy.
func (r *Restore) InfoPolicy() *aerospike.InfoPolicy {
	p := aerospike.NewInfoPolicy()
	p.Timeout = time.Duration(r.InfoTimeout) * time.Millisecond

	return p
}

// RetryPolicy maps restore configuration parameters to a retry policy,
// including interval, multiplier, and max retries.
func (r *Restore) RetryPolicy() *models.RetryPolicy {
	return models.NewRetryPolicy(
		time.Duration(r.InfoRetryIntervalMilliseconds)*time.Millisecond,
		r.InfoRetriesMultiplier,
		r.InfoMaxRetries,
	)
}

// Sets converts the Sets string into a slice of set names by splitting it using commas.
// Returns nil if empty.
func (r *Restore) Sets() []string {
	return splitByComma(r.SetList)
}

// Bins converts the BinList string into a slice of bin names by splitting it using commas.
// Returns nil if empty.
func (r *Restore) Bins() []string {
	return splitByComma(r.BinList)
}

func recordExistsAction(replace, unique bool) aerospike.RecordExistsAction {
	switch {
	case replace:
		return aerospike.REPLACE
	case unique:
		return aerospike.CREATE_ONLY
	default:
		return aerospike.UPDATE
	}
}
