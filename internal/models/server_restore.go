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

	infomodels "github.com/aerospike/backup-go/pkg/asinfo/models"
)

// Limits of the restore settings, as enforced by the server. Zero leaves a thread count
// to the server default, so it is accepted by each of them.
const (
	maxServerRestoreParallelWriters         = 128
	minServerRestoreMaxInflight             = 1
	maxServerRestoreMaxInflight             = 100_000
	minServerRestoreRetryMultiplier         = 1.0
	maxServerRestoreParallelDownloadThreads = 128
	// maxServerRestoreExtraTTL is ten years, in seconds.
	maxServerRestoreExtraTTL = 315_360_000
	// maxServerRestoreBackupIDs is the number of backups a single restore reads at most.
	maxServerRestoreBackupIDs = 32
)

// backupIDsSeparator separates the ids of the backups a single restore reads. The
// server takes them as one comma-separated parameter, so they are carried as a
// string rather than a slice all the way down to the request.
const backupIDsSeparator = ","

// ServerRestore contains flags that will be mapped to ServerRestore.
type ServerRestore struct {
	ServerCommon
	// BackupIDs is a comma-separated list of the ids of the backups to restore from.
	BackupIDs string
	// IncrementalPath is the key prefix the incremental backups, every backup id after
	// the first one, were written under, when it differs from Path.
	IncrementalPath string

	// ExtraTTL is added to the expiry of every restored record that expires, in seconds.
	ExtraTTL int
	// ParallelDownloadThreads is the number of threads per node that download the backup
	// files. Zero leaves it to the server default.
	ParallelDownloadThreads int
	// ParallelWriters is the number of threads per node that apply the records. Zero
	// leaves it to the server default.
	ParallelWriters int

	// The fields below apply to one policy only, and the server refuses each of them,
	// whatever its value, with the other policy. Nil leaves a field unset: it is not
	// sent, and the server applies its default.

	// HydrateReplica restores every replica of each partition when true, and only the
	// master when false. It applies to a strict (cold) restore only.
	HydrateReplica *bool

	// The fields below apply to a fuzzy restore only.

	MaxInflight *int
	// RetryBaseInterval is the base delay before a retry, in milliseconds.
	RetryBaseInterval *int
	RetryMultiplier   *float64
	// RetryMaxAttempts is the number of retries per record after its first failed
	// write, 0 for none.
	RetryMaxAttempts  *int
	IgnoreRecordError *bool
	// ApplyMetadataLast creates the secondary indexes and UDFs after the records
	// instead of before.
	ApplyMetadataLast *bool
}

// RestorePolicy returns the restore policy in the lower case the server expects. The
// policy is accepted in any case.
func (s *ServerRestore) RestorePolicy() infomodels.RestorePolicy {
	return infomodels.RestorePolicy(strings.ToLower(s.Policy))
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

	if err := s.ServerCommon.Validate(); err != nil {
		return err
	}

	if err := s.validateBackupIDs(); err != nil {
		return err
	}

	if err := s.validateIncrementalPath(); err != nil {
		return err
	}

	if err := s.validateRestoreMode(); err != nil {
		return err
	}

	return s.validateJobSettings()
}

// IncrementalPrefix returns the key prefix the incremental backups are read from, in the
// normalized form the server is sent. Empty means none is set, and the server reads them
// from under the path.
func (s *ServerRestore) IncrementalPrefix() string {
	return NormalizeS3Prefix(s.IncrementalPath)
}

// validateIncrementalPath checks the path of the incremental backups in the form it is
// sent. The server refuses it for a restore of a single backup, which has no incremental
// backups to read, and an empty one is not sent at all, so it cannot name the bucket root
// while the path names a prefix.
func (s *ServerRestore) validateIncrementalPath() error {
	if err := validatePath(s.IncrementalPath); err != nil {
		return err
	}

	if s.IncrementalPrefix() == "" {
		if s.IncrementalPath != "" && NormalizeS3Prefix(s.Path) != "" {
			return fmt.Errorf("incremental-path %q names the bucket root, which cannot be "+
				"told apart from no incremental-path: omit it to read the incremental backups "+
				"from under path", s.IncrementalPath)
		}

		return nil
	}

	if len(s.BackupIDList()) < 2 {
		return errors.New("incremental-path requires more than one backup id")
	}

	return validateServerPath("incremental-path", s.IncrementalPath)
}

// validateJobSettings checks the settings that apply to both cold and fuzzy restore.
func (s *ServerRestore) validateJobSettings() error {
	switch {
	case s.ParallelDownloadThreads < 0 ||
		s.ParallelDownloadThreads > maxServerRestoreParallelDownloadThreads:
		return fmt.Errorf("parallel-download-threads must be between 0 and %d, got %d",
			maxServerRestoreParallelDownloadThreads, s.ParallelDownloadThreads)
	case s.ParallelWriters < 0 || s.ParallelWriters > maxServerRestoreParallelWriters:
		return fmt.Errorf("parallel-writers must be between 0 and %d, got %d",
			maxServerRestoreParallelWriters, s.ParallelWriters)
	case s.ExtraTTL < 0 || s.ExtraTTL > maxServerRestoreExtraTTL:
		return fmt.Errorf("extra-ttl must be between 0 and %d, got %d",
			maxServerRestoreExtraTTL, s.ExtraTTL)
	}

	return nil
}

// validateBackupIDs checks the backup id list the server is sent verbatim. The server
// rejects an empty element of the list, and every backup is looked up by its id, so both
// are caught here rather than after the cluster and the object storage are reached.
func (s *ServerRestore) validateBackupIDs() error {
	if s.BackupIDs == "" {
		return errors.New("backup-ids is required")
	}

	ids := s.BackupIDList()
	if slices.Contains(ids, "") {
		return errors.New("backup-ids must not contain an empty id")
	}

	if len(ids) > maxServerRestoreBackupIDs {
		return fmt.Errorf("backup-ids takes at most %d ids, got %d", maxServerRestoreBackupIDs, len(ids))
	}

	seen := make(map[string]struct{}, len(ids))

	for _, id := range ids {
		if err := validateBackupID("backup-ids", id); err != nil {
			return err
		}

		if _, ok := seen[id]; ok {
			return fmt.Errorf("backup-ids must not repeat an id, got %q twice", id)
		}

		seen[id] = struct{}{}
	}

	return nil
}

// validateRestoreMode checks the policy and the settings that depend on it. The policy is
// strict (the cold restore, which hydrates the partitions of a closed namespace) or fuzzy
// (which writes into the live namespace); empty leaves it to the server default, strict.
// The server rejects the settings of one policy, whatever their values, with the other
// one, so a setting given for the other policy fails the command before the cluster is
// reached.
func (s *ServerRestore) validateRestoreMode() error {
	switch s.RestorePolicy() {
	case "", infomodels.RestorePolicyStrict:
		return s.validateColdRestore()
	case infomodels.RestorePolicyFuzzy:
		return s.validateFuzzyRestore()
	default:
		return fmt.Errorf("policy must be %s or %s, got %q",
			infomodels.RestorePolicyStrict, infomodels.RestorePolicyFuzzy, s.Policy)
	}
}

// validateFuzzyRestore checks the settings of a restore with the fuzzy policy.
func (s *ServerRestore) validateFuzzyRestore() error {
	switch {
	case s.HydrateReplica != nil:
		return fmt.Errorf("hydrate-replica requires policy %s", infomodels.RestorePolicyStrict)
	case s.MaxInflight != nil &&
		(*s.MaxInflight < minServerRestoreMaxInflight || *s.MaxInflight > maxServerRestoreMaxInflight):
		return fmt.Errorf("max-inflight must be between %d and %d, got %d",
			minServerRestoreMaxInflight, maxServerRestoreMaxInflight, *s.MaxInflight)
	case s.RetryBaseInterval != nil && !inUint32Range(*s.RetryBaseInterval):
		return fmt.Errorf("retry-base-interval must be between 0 and %d, got %d",
			uint32(math.MaxUint32), *s.RetryBaseInterval)
	// NaN compares false with everything, and the server reads neither it nor an
	// infinity as a number.
	case s.RetryMultiplier != nil && (math.IsNaN(*s.RetryMultiplier) || math.IsInf(*s.RetryMultiplier, 0) ||
		*s.RetryMultiplier < minServerRestoreRetryMultiplier):
		return fmt.Errorf("retry-multiplier must be at least %.1f, got %g",
			minServerRestoreRetryMultiplier, *s.RetryMultiplier)
	case s.RetryMaxAttempts != nil && !inUint32Range(*s.RetryMaxAttempts):
		return fmt.Errorf("retry-max-attempts must be between 0 and %d, got %d",
			uint32(math.MaxUint32), *s.RetryMaxAttempts)
	// The server refuses apply-metadata-last, whatever its value, when there is no
	// metadata to apply.
	case s.ApplyMetadataLast != nil && s.NoIndexes && s.NoUDFs:
		return errors.New("apply-metadata-last has nothing to apply with no-indexes and no-udfs")
	}

	return nil
}

// validateColdRestore rejects the fuzzy policy settings given for a strict restore.
func (s *ServerRestore) validateColdRestore() error {
	fuzzyOnly := []struct {
		name string
		set  bool
	}{
		{"max-inflight", s.MaxInflight != nil},
		{"retry-base-interval", s.RetryBaseInterval != nil},
		{"retry-multiplier", s.RetryMultiplier != nil},
		{"retry-max-attempts", s.RetryMaxAttempts != nil},
		{"ignore-record-error", s.IgnoreRecordError != nil},
		{"apply-metadata-last", s.ApplyMetadataLast != nil},
	}

	for _, f := range fuzzyOnly {
		if f.set {
			return fmt.Errorf("%s requires policy %s", f.name, infomodels.RestorePolicyFuzzy)
		}
	}

	return nil
}

// inUint32Range reports whether v fits the unsigned 32-bit range the server reads it in.
func inUint32Range(v int) bool {
	return v >= 0 && int64(v) <= math.MaxUint32
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
	// Async returns as soon as the cluster accepts the abort, instead of waiting for the
	// restore to stop.
	Async bool
}

func (s *ServerRestoreAbort) Validate() error {
	if s == nil {
		return nil
	}

	return validateRestoreJob(s.Namespace, s.JobID)
}

// ServerRestoreOpen contains flags that will be mapped to ServerRestoreOpen.
// A namespace has at most one restore job, so the cluster finds the failed or aborted
// cold restore that holds the namespace by the namespace alone.
type ServerRestoreOpen struct {
	Namespace string
	// JobID is the optional id of the failed or aborted cold restore job that holds the
	// namespace. It is not sent with the request: the status of the job is read by it, so
	// the release is followed only when it is set.
	JobID string
	// Async returns as soon as the cluster accepts the request, instead of waiting for
	// the namespace to be released. It applies only when JobID is set.
	Async bool
}

// Validate checks that the namespace to open is named, and that the job id, when one is
// given, is one the server accepts.
func (s *ServerRestoreOpen) Validate() error {
	if s == nil {
		return nil
	}

	if s.JobID != "" {
		if err := validateServerJobID("job-id", s.JobID); err != nil {
			return err
		}
	}

	return validateRestoreNamespace(s.Namespace)
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

// validateRestoreJob checks the namespace and the job id that name a restore job, as the
// server does.
func validateRestoreJob(namespace, jobID string) error {
	if err := validateServerJobID("job-id", jobID); err != nil {
		return err
	}

	return validateRestoreNamespace(namespace)
}

// validateRestoreNamespace checks the namespace a restore subcommand acts on, as the
// server does.
func validateRestoreNamespace(namespace string) error {
	if namespace == "" {
		return errors.New("namespace is required")
	}

	return validateServerText("namespace", namespace, maxServerNamespaceLen)
}
