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
	"math"
	"slices"
	"strings"
)

// Limits of the restore settings, as enforced by the server.
const (
	minServerRestoreParallelWrite   = 1
	maxServerRestoreParallelWrite   = 128
	minServerRestoreMaxInflight     = 1
	maxServerRestoreMaxInflight     = 100_000
	minServerRestoreRetryMultiplier = 1.0
	minServerRestoreParallelRead    = 1
	maxServerRestoreParallelRead    = 128
	// maxServerRestoreExtraTTL is ten years, in seconds.
	maxServerRestoreExtraTTL = 315_360_000
)

// backupIDsSeparator separates the ids of the backups a single restore reads. The
// server takes them as one comma-separated parameter, so they are carried as a
// string rather than a slice all the way down to the request.
const backupIDsSeparator = ","

// ServerRestore contains flags that will be mapped to ServerRestore.
type ServerRestore struct {
	ServerCommon
	// JobID is the id of the restore job. When a single backup is restored and
	// JobID is empty, the id of that backup is used instead.
	JobID string
	// BackupIDs is a comma-separated list of the ids of the backups to restore from.
	BackupIDs string
	// Path is the key prefix the backup was written under.
	Path string
	// IncrementalPath is the key prefix the incremental backups, every backup id after
	// the first one, were written under, when it differs from Path.
	IncrementalPath string
	SetList         string
	// FilterExp is a base64-encoded filter expression. Cold restore only.
	FilterExp    string
	NoIndexes    bool
	NoUDFs       bool
	FuzzyRestore bool
	// Async returns from the start command as soon as the cluster accepts the job,
	// instead of following the restore until it ends.
	Async bool

	// The fields below apply to both cold and fuzzy restore.

	// ParallelRead is the number of threads per node that download the backup files.
	ParallelRead int
	// ParallelWrite is the number of threads per node that apply the records.
	ParallelWrite int
	// RecordsPerSecond limits the records per second each node applies.
	RecordsPerSecond int
	// ExtraTTL is added to the expiry of every restored record that expires, in seconds.
	ExtraTTL int

	// The fields below apply to a fuzzy restore only.

	AllowUnhosted bool
	MaxInflight   int
	// RetryBaseInterval is the base delay before a retry, in milliseconds.
	RetryBaseInterval int
	RetryMultiplier   float64
	RetryMaxAttempts  int
	IgnoreRecordError bool
	// ApplyMetadataLast creates the secondary indexes and UDFs after the records
	// instead of before.
	ApplyMetadataLast bool
}

// RestoreJobID returns the id of the restore job, which defaults to the id of the
// backup being restored. Validate rejects the empty JobID of a multi-backup restore,
// so the fallback never names the job after more than one backup.
func (s *ServerRestore) RestoreJobID() string {
	if s.JobID != "" {
		return s.JobID
	}

	return s.BackupIDs
}

// BackupIDList returns the ids of the backups to restore from, one per element.
func (s *ServerRestore) BackupIDList() []string {
	if s.BackupIDs == "" {
		return nil
	}

	return strings.Split(s.BackupIDs, backupIDsSeparator)
}

func (s *ServerRestore) Validate() error {
	if s == nil {
		return nil
	}

	if err := s.validateBackupIDs(); err != nil {
		return err
	}

	if err := validatePath(s.Path); err != nil {
		return err
	}

	if err := s.validateIncrementalPath(); err != nil {
		return err
	}

	if err := validateFilterExp(s.FilterExp); err != nil {
		return err
	}

	if err := s.validateRestoreMode(); err != nil {
		return err
	}

	if err := s.validateJobSettings(); err != nil {
		return err
	}

	return s.ServerCommon.Validate()
}

// validateIncrementalPath checks the path of the incremental backups. The server refuses
// it for a restore of a single backup, which has no incremental backups to read.
func (s *ServerRestore) validateIncrementalPath() error {
	if s.IncrementalPath == "" {
		return nil
	}

	if len(s.BackupIDList()) < 2 {
		return errors.New("incremental-path requires more than one backup id")
	}

	return validatePath(s.IncrementalPath)
}

// validateJobSettings checks the settings that apply to both cold and fuzzy restore.
func (s *ServerRestore) validateJobSettings() error {
	switch {
	case s.ParallelRead < minServerRestoreParallelRead ||
		s.ParallelRead > maxServerRestoreParallelRead:
		return fmt.Errorf("parallel-read must be between %d and %d, got %d",
			minServerRestoreParallelRead, maxServerRestoreParallelRead,
			s.ParallelRead)
	case s.ParallelWrite < minServerRestoreParallelWrite || s.ParallelWrite > maxServerRestoreParallelWrite:
		return fmt.Errorf("parallel-write must be between %d and %d, got %d",
			minServerRestoreParallelWrite, maxServerRestoreParallelWrite, s.ParallelWrite)
	case s.RecordsPerSecond < 0 || int64(s.RecordsPerSecond) > math.MaxUint32:
		return fmt.Errorf("records-per-second must be between 0 and %d, got %d",
			uint32(math.MaxUint32), s.RecordsPerSecond)
	case s.ExtraTTL < 0 || s.ExtraTTL > maxServerRestoreExtraTTL:
		return fmt.Errorf("extra-ttl must be between 0 and %d, got %d",
			maxServerRestoreExtraTTL, s.ExtraTTL)
	}

	return nil
}

// validateBackupIDs checks the backup id list the server is sent verbatim. The server
// rejects an empty element of the list, and an id list cannot name the restore job, so
// both are caught here rather than after the cluster and the object storage are reached.
func (s *ServerRestore) validateBackupIDs() error {
	if s.BackupIDs == "" {
		return errors.New("backup-ids is required")
	}

	ids := s.BackupIDList()
	if slices.Contains(ids, "") {
		return errors.New("backup-ids must not contain an empty id")
	}

	if s.JobID == "" && len(ids) > 1 {
		return errors.New("job-id is required when more than one backup id is given")
	}

	return nil
}

// validateRestoreMode checks the settings that depend on the restore mode. The server
// ignores the fuzzy restore settings of a cold restore, so a value changed from its
// default without fuzzy-restore is rejected rather than silently dropped.
func (s *ServerRestore) validateRestoreMode() error {
	if !s.FuzzyRestore {
		return s.validateColdRestore()
	}

	if s.FilterExp != "" {
		return fmt.Errorf("filter-exp is not supported with fuzzy-restore")
	}

	switch {
	case s.MaxInflight < minServerRestoreMaxInflight || s.MaxInflight > maxServerRestoreMaxInflight:
		return fmt.Errorf("max-inflight must be between %d and %d, got %d",
			minServerRestoreMaxInflight, maxServerRestoreMaxInflight, s.MaxInflight)
	case s.RetryBaseInterval < 0 || int64(s.RetryBaseInterval) > math.MaxUint32:
		return fmt.Errorf("retry-base-interval must be between 0 and %d, got %d",
			uint32(math.MaxUint32), s.RetryBaseInterval)
	case s.RetryMultiplier < minServerRestoreRetryMultiplier:
		return fmt.Errorf("retry-multiplier must be at least %.1f, got %g",
			minServerRestoreRetryMultiplier, s.RetryMultiplier)
	case s.RetryMaxAttempts <= 0:
		return fmt.Errorf("retry-max-attempts must be positive, got %d", s.RetryMaxAttempts)
	case s.ApplyMetadataLast && s.NoIndexes && s.NoUDFs:
		return errors.New("apply-metadata-last has nothing to apply with no-indexes and no-udfs")
	}

	return nil
}

// validateColdRestore rejects the fuzzy restore settings changed from their defaults.
// A value passed explicitly but equal to its default cannot be told apart from an unset
// one, and is accepted.
func (s *ServerRestore) validateColdRestore() error {
	fuzzyOnly := []struct {
		name string
		set  bool
	}{
		{"allow-unhosted", changed(s.AllowUnhosted, DefaultServerRestoreAllowUnhosted)},
		{"max-inflight", changed(s.MaxInflight, DefaultServerRestoreMaxInflight)},
		{"retry-base-interval", changed(s.RetryBaseInterval, DefaultServerRestoreRetryBaseInterval)},
		{"retry-multiplier", changed(s.RetryMultiplier, DefaultServerRestoreRetryMultiplier)},
		{"retry-max-attempts", changed(s.RetryMaxAttempts, DefaultServerRestoreRetryMaxAttempts)},
		{"ignore-record-error", changed(s.IgnoreRecordError, DefaultServerRestoreIgnoreRecordError)},
		{"apply-metadata-last", changed(s.ApplyMetadataLast, DefaultServerRestoreApplyMetadataLast)},
	}

	for _, f := range fuzzyOnly {
		if f.set {
			return fmt.Errorf("%s requires fuzzy-restore", f.name)
		}
	}

	return nil
}

// changed reports whether v differs from its default def. Comparing a bool against a
// constant default inline reads as "x != false", which linters rewrite into something
// that loses the reference to the default, so every field goes through this instead.
func changed[T comparable](v, def T) bool {
	return v != def
}

// ServerRestorePrepare contains flags that will be mapped to ServerRestorePrepare.
type ServerRestorePrepare struct {
	Namespace string
	// JobID is the id of the restore job. The restore must be started with the same id.
	JobID string
	// HydrateReplica restores all replicas when true, and only the master when false.
	HydrateReplica bool
}

func (s *ServerRestorePrepare) Validate() error {
	if s == nil {
		return nil
	}

	if s.JobID == "" {
		return fmt.Errorf("job-id is required")
	}

	if s.Namespace == "" {
		return fmt.Errorf("namespace is required")
	}

	return nil
}

// ServerRestoreProgress contains flags that will be mapped to ServerRestoreProgress.
type ServerRestoreProgress struct {
	Namespace string
	// JobID is the id of the restore job to follow.
	JobID string
	// Watch keeps reporting the restore until it reaches a terminal state.
	Watch bool
}

func (s *ServerRestoreProgress) Validate() error {
	if s == nil {
		return nil
	}

	return validateRestoreJob(s.Namespace, s.JobID)
}

// ServerRestoreAbort contains flags that will be mapped to ServerRestoreAbort.
// Unlike a backup job, a restore job is named by the namespace it restores into
// as well as by its id, so both are required.
type ServerRestoreAbort struct {
	Namespace string
	JobID     string
}

func (s *ServerRestoreAbort) Validate() error {
	if s == nil {
		return nil
	}

	return validateRestoreJob(s.Namespace, s.JobID)
}

// ServerRestoreOpen contains flags that will be mapped to ServerRestoreOpen.
type ServerRestoreOpen struct {
	Namespace string
	// JobID is the id of the aborted cold restore job that holds the namespace.
	JobID string
	// Async returns as soon as the cluster accepts the request, instead of waiting for
	// the namespace to be released.
	Async bool
}

// Validate checks that the restore job to open is named.
func (s *ServerRestoreOpen) Validate() error {
	if s == nil {
		return nil
	}

	return validateRestoreJob(s.Namespace, s.JobID)
}

// ServerRestoreClearStorage contains flags that will be mapped to ServerRestoreClearStorage.
type ServerRestoreClearStorage struct {
	Namespace string
	// JobID is the id of the cold restore job being prepared.
	JobID string
	// Async returns as soon as the nodes accept the clear, instead of waiting for the
	// namespace to become ready for the restore.
	Async bool
}

// Validate checks that the restore job whose storage is cleared is named.
func (s *ServerRestoreClearStorage) Validate() error {
	if s == nil {
		return nil
	}

	return validateRestoreJob(s.Namespace, s.JobID)
}

// validateRestoreJob checks the namespace and the job id that name a restore job.
func validateRestoreJob(namespace, jobID string) error {
	if jobID == "" {
		return errors.New("job-id is required")
	}

	if namespace == "" {
		return errors.New("namespace is required")
	}

	return nil
}
