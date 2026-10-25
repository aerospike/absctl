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
	"fmt"
	"math"
	"time"

	infomodels "github.com/aerospike/backup-go/pkg/asinfo/models"
)

// Limits of the backup job settings, as enforced by the server. Zero leaves a setting
// to the server default, so it is accepted by each of them.
const (
	maxServerBackupParallel     = 128
	maxServerBackupS3Throughput = 400
	// minServerBackupRPSPerReader is how many records per second each scan thread
	// needs at least: a non-zero limit below it is refused by the server.
	minServerBackupRPSPerReader = 5
)

// ServerBackup contains flags that will be mapped to ServerBackup.
type ServerBackup struct {
	ServerCommon

	// Path is the key prefix the backup is written under.
	Path           string
	ModifiedBefore string
	ModifiedAfter  string
	SetList        string
	BinList        string
	// FilterExp is a base64-encoded filter expression.
	FilterExp string
	NoIndexes bool
	NoUDFs    bool
	// Policy is how consistent the backup must be: strict, best-effort or fuzzy. Empty
	// leaves it to the server default.
	Policy string
	// Async returns from the start command as soon as the cluster accepts the job,
	// instead of following the backup until it ends.
	Async bool

	// The settings below are sent only when they are not zero, so that the server
	// applies its own default otherwise.

	// RecordsPerSecond limits the records per second of each scan on each node.
	RecordsPerSecond int
	// ParallelRead is the number of scan threads per node.
	ParallelRead int
	// ParallelWrite is the number of upload threads per node.
	ParallelWrite int
	// S3Throughput is the target throughput of the S3 client per node, in Gbit/s.
	S3Throughput int
}

func (s *ServerBackup) Validate() error {
	if s == nil {
		return nil
	}

	var modifiedAfter, modifiedBefore time.Time

	if s.ModifiedAfter != "" {
		ma, err := s.ModifiedAfterTime()
		if err != nil {
			return fmt.Errorf("failed to parse modified after: %w", err)
		}
		modifiedAfter = ma
	}

	if s.ModifiedBefore != "" {
		mb, err := s.ModifiedBeforeTime()
		if err != nil {
			return fmt.Errorf("failed to parse modified before: %w", err)
		}
		modifiedBefore = mb
	}

	if s.ModifiedAfter != "" && s.ModifiedBefore != "" && !modifiedBefore.After(modifiedAfter) {
		return fmt.Errorf("modified-before must be strictly greater than modified-after")
	}

	if err := validatePath(s.Path); err != nil {
		return err
	}

	if err := validateFilterExp(s.FilterExp); err != nil {
		return err
	}

	if err := s.validatePolicy(); err != nil {
		return err
	}

	if err := s.validateJobSettings(); err != nil {
		return err
	}

	return s.ServerCommon.Validate()
}

// validatePolicy checks that the policy, when set, is one the server accepts.
func (s *ServerBackup) validatePolicy() error {
	switch infomodels.BackupPolicy(s.Policy) {
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
	case s.RecordsPerSecond < 0 || int64(s.RecordsPerSecond) > math.MaxUint32:
		return fmt.Errorf("records-per-second must be between 0 and %d, got %d",
			uint32(math.MaxUint32), s.RecordsPerSecond)
	case s.ParallelRead < 0 || s.ParallelRead > maxServerBackupParallel:
		return fmt.Errorf("parallel-read must be between 0 and %d, got %d",
			maxServerBackupParallel, s.ParallelRead)
	case s.ParallelWrite < 0 || s.ParallelWrite > maxServerBackupParallel:
		return fmt.Errorf("parallel-write must be between 0 and %d, got %d",
			maxServerBackupParallel, s.ParallelWrite)
	case s.S3Throughput < 0 || s.S3Throughput > maxServerBackupS3Throughput:
		return fmt.Errorf("s3-throughput must be between 0 and %d, got %d",
			maxServerBackupS3Throughput, s.S3Throughput)
	}

	// The minimum depends on the number of scan threads, which is only known here when
	// it is set: the server checks the limit against its own default otherwise.
	minRPS := minServerBackupRPSPerReader * s.ParallelRead
	if s.RecordsPerSecond > 0 && s.RecordsPerSecond < minRPS {
		return fmt.Errorf("records-per-second must be 0 or at least %d for %d parallel-read, got %d",
			minRPS, s.ParallelRead, s.RecordsPerSecond)
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
