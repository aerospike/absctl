// Copyright 2026 Aerospike, Inc.
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
	"errors"
	"fmt"
	"strings"
	"time"

	infomodels "github.com/aerospike/backup-go/pkg/asinfo/models"
)

// Limits of the backup job settings, as enforced by the server. Zero leaves a setting
// to the server default, so it is accepted by each of them.
const (
	maxServerBackupParallel = 128
	// serverDefaultBackupParallelReaders is the number of scan threads the server uses
	// when none is sent.
	serverDefaultBackupParallelReaders = 4
	// minServerBackupRPSPerReader is how many records per second each scan thread
	// needs at least: a non-zero limit below it is refused by the server.
	minServerBackupRPSPerReader = 5
)

// Bounds of the modified time range, in Unix seconds, as enforced by the server.
const (
	// minServerModifiedTime is 2010-01-01T00:00:00Z: both bounds must be after it.
	minServerModifiedTime = 1_262_304_000
	// maxServerModifiedTime is the largest modified-before the server accepts, the
	// largest value a 64-bit nanosecond clock holds in seconds.
	maxServerModifiedTime = 18_446_744_073
)

// ServerBackup contains flags that will be mapped to ServerBackup.
type ServerBackup struct {
	ServerCommon

	ModifiedBefore string
	ModifiedAfter  string
	BinList        string
	// NoConfig skips the upload of the config file of each node with the backup.
	NoConfig bool
	// ParallelReaders is the number of scan threads per node. Zero leaves it to the
	// server default.
	ParallelReaders int
	// ParallelUploadThreads is the number of upload threads per node. Zero leaves it to
	// the server default.
	ParallelUploadThreads int
}

// Validate checks the backup settings against the rules the server applies, so that a
// value the server refuses fails the command before the cluster is reached.
func (s *ServerBackup) Validate() error {
	if s == nil {
		return nil
	}

	if err := s.ServerCommon.Validate(); err != nil {
		return err
	}

	if err := s.validateSetList(); err != nil {
		return err
	}

	if err := validateServerNameList("bin-list", s.BinList, maxServerBinListLen,
		maxServerBinNameLen); err != nil {
		return err
	}

	if err := s.validateModifiedRange(time.Now()); err != nil {
		return err
	}

	if err := s.validatePolicy(); err != nil {
		return err
	}

	return s.validateJobSettings()
}

// validateSetList rejects the set of the multi-record transaction monitor records, which
// a backup cannot name. The rest of the list is checked by ServerCommon.
func (s *ServerBackup) validateSetList() error {
	if s.SetList == "" {
		return nil
	}

	for name := range strings.SplitSeq(s.SetList, serverNameListSeparator) {
		if name == mrtMonitorSet {
			return fmt.Errorf("set-list must not name the MRT monitor set %s", mrtMonitorSet)
		}
	}

	return nil
}

// validateModifiedRange checks the modified time range against now: both bounds after
// 2010-01-01, the lower one not in the future, as it would match no record, the upper
// one within the range the server reads, and the lower one before the upper one.
func (s *ServerBackup) validateModifiedRange(now time.Time) error {
	var modifiedAfter, modifiedBefore time.Time

	if s.ModifiedAfter != "" {
		ma, err := s.ModifiedAfterTime()
		if err != nil {
			return fmt.Errorf("failed to parse modified after: %w", err)
		}

		switch {
		case ma.Unix() <= minServerModifiedTime:
			return errors.New("modified-after must be after 2010-01-01")
		case ma.After(now):
			return errors.New("modified-after is in the future, it would match no records")
		}

		modifiedAfter = ma
	}

	if s.ModifiedBefore != "" {
		mb, err := s.ModifiedBeforeTime()
		if err != nil {
			return fmt.Errorf("failed to parse modified before: %w", err)
		}

		switch {
		case mb.Unix() <= minServerModifiedTime:
			return errors.New("modified-before must be after 2010-01-01")
		case mb.Unix() > maxServerModifiedTime:
			return fmt.Errorf("modified-before must be at most %d in Unix seconds", maxServerModifiedTime)
		}

		modifiedBefore = mb
	}

	if s.ModifiedAfter != "" && s.ModifiedBefore != "" && !modifiedBefore.After(modifiedAfter) {
		return errors.New("modified-before must be strictly greater than modified-after")
	}

	return nil
}

// BackupPolicy returns the backup policy in the lower case the server expects. The policy
// is accepted in any case.
func (s *ServerBackup) BackupPolicy() infomodels.BackupPolicy {
	return infomodels.BackupPolicy(strings.ToLower(s.Policy))
}

// validatePolicy checks that the policy, when set, is one the server accepts.
func (s *ServerBackup) validatePolicy() error {
	switch s.BackupPolicy() {
	case "",
		infomodels.BackupPolicyStrict,
		infomodels.BackupPolicyBestEffort,
		infomodels.BackupPolicyFuzzy:
		return nil
	default:
		return fmt.Errorf("policy must be %s, %s or %s, got %q",
			infomodels.BackupPolicyStrict, infomodels.BackupPolicyBestEffort,
			infomodels.BackupPolicyFuzzy, s.Policy)
	}
}

// validateJobSettings checks the limits of the backup job settings, so that a value the
// server refuses fails the command before the cluster is reached.
func (s *ServerBackup) validateJobSettings() error {
	switch {
	case s.ParallelReaders < 0 || s.ParallelReaders > maxServerBackupParallel:
		return fmt.Errorf("parallel-readers must be between 0 and %d, got %d",
			maxServerBackupParallel, s.ParallelReaders)
	case s.ParallelUploadThreads < 0 || s.ParallelUploadThreads > maxServerBackupParallel:
		return fmt.Errorf("parallel-upload-threads must be between 0 and %d, got %d",
			maxServerBackupParallel, s.ParallelUploadThreads)
	}

	// The minimum depends on the number of scan threads: the server checks the limit
	// against its own default when none is sent.
	readers := s.ParallelReaders
	if readers == 0 {
		readers = serverDefaultBackupParallelReaders
	}

	minRPS := minServerBackupRPSPerReader * readers
	if s.RecordsPerSecond > 0 && s.RecordsPerSecond < minRPS {
		if s.ParallelReaders == 0 {
			return fmt.Errorf("records-per-second must be 0 or at least %d for the %d scan threads "+
				"the server uses when parallel-readers is 0, got %d", minRPS, readers, s.RecordsPerSecond)
		}

		return fmt.Errorf("records-per-second must be 0 or at least %d for %d parallel-readers, got %d",
			minRPS, readers, s.RecordsPerSecond)
	}

	return nil
}

// ModifiedBeforeTime maps the ModifiedBefore string into a UTC time.
func (s *ServerBackup) ModifiedBeforeTime() (time.Time, error) {
	return ParseLocalTimeToUTC(s.ModifiedBefore)
}

// ModifiedAfterTime maps the ModifiedAfter string into a UTC time.
func (s *ServerBackup) ModifiedAfterTime() (time.Time, error) {
	return ParseLocalTimeToUTC(s.ModifiedAfter)
}

type ServerBackupList struct {
	// Path is the path to list backups from.
	Path string
}

func (s *ServerBackupList) Validate() error {
	if s == nil {
		return nil
	}

	return validatePath(s.Path)
}

type ServerBackupValidate struct {
	// JobID is the id of the backup to validate.
	JobID string
	// Path is the key prefix the backup was written under.
	Path string
	// SampleSize specifies the sample size limit for validation operations.
	// Zero means that every segment is validated.
	SampleSize int
}

func (s *ServerBackupValidate) Validate() error {
	if s == nil {
		return nil
	}

	if s.JobID == "" {
		return fmt.Errorf("backup-id is required")
	}

	if err := validateBackupID("backup-id", s.JobID); err != nil {
		return err
	}

	if err := validatePath(s.Path); err != nil {
		return err
	}

	if s.SampleSize < 0 {
		return fmt.Errorf("sample-size must be non-negative")
	}

	return nil
}

// ServerBackupProgress contains flags that will be mapped to ServerBackupProgress.
type ServerBackupProgress struct {
	// JobID is the id of the backup to follow.
	JobID string
	// Path is the key prefix the backup is written under.
	Path string
	// Watch keeps reporting the backup until it reaches a terminal state.
	Watch bool
}

func (s *ServerBackupProgress) Validate() error {
	if s == nil {
		return nil
	}

	if s.JobID == "" {
		return fmt.Errorf("backup-id is required")
	}

	if err := validateBackupID("backup-id", s.JobID); err != nil {
		return err
	}

	return validatePath(s.Path)
}

// ServerBackupAbort contains flags that will be mapped to ServerBackupAbort.
type ServerBackupAbort struct {
	JobID string
	// Async returns as soon as the cluster accepts the abort, instead of waiting for the
	// backup to stop.
	Async bool
}

func (s *ServerBackupAbort) Validate() error {
	if s == nil {
		return nil
	}

	if s.JobID == "" {
		return fmt.Errorf("backup-id is required")
	}

	return validateBackupID("backup-id", s.JobID)
}
