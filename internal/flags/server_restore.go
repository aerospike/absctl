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

package flags

import (
	"github.com/aerospike/absctl/internal/models"
	"github.com/spf13/pflag"
)

// ServerRestore holds flags for the server restore start command.
type ServerRestore struct {
	models.ServerRestore
}

func NewServerRestore() *ServerRestore {
	return &ServerRestore{}
}

func (f *ServerRestore) NewFlagSet() *pflag.FlagSet {
	flagSet := &pflag.FlagSet{}

	flagSet.StringVarP(&f.Namespace, "namespace", "n",
		models.DefaultCommonNamespace,
		"The namespace to restore.")
	flagSet.StringVar(&f.BackupIDs, "backup-ids",
		models.DefaultServerBackupJobID,
		"Comma separated list of the ids of the backups to restore from.")
	flagSet.StringVar(&f.JobID, "job-id",
		models.DefaultServerRestoreJobID,
		"Id of the restore job. A strict (cold) restore must use the job id\n"+
			"passed to the restore preparation. If empty, the id of the single backup\n"+
			"being restored is used, so it is required when several backup ids are given.")
	flagSet.StringVar(&f.Path, "path",
		models.DefaultServerBackupPath,
		"Key prefix in the bucket the backup was written under.")
	flagSet.StringVar(&f.IncrementalPath, "incremental-path",
		models.DefaultServerRestoreIncrementalPath,
		"Key prefix in the bucket the incremental backups, every backup id\n"+
			"after the first one, were written under. If empty, --path is used.\n"+
			"Requires more than one backup id.")
	flagSet.StringVarP(&f.SetList, "set-list", "s",
		models.DefaultCommonSetList,
		descSetListRestore)
	flagSet.StringVar(&f.FilterExp, "filter-exp",
		models.DefaultServerFilterExp,
		"Base64 encoded filter expression. Strict policy only.")
	flagSet.BoolVarP(&f.NoIndexes, "no-indexes", "I",
		models.DefaultCommonNoIndexes,
		descNoIndexesRestore)
	flagSet.BoolVar(&f.NoUDFs, "no-udfs",
		models.DefaultCommonNoUDFs,
		descNoUDFsRestore)
	flagSet.BoolVar(&f.Async, "async",
		models.DefaultServerRestoreAsync,
		"Return as soon as the cluster accepts the restore, without\n"+
			"following its progress. Use the progress command to follow\n"+
			"the restore later.")
	flagSet.IntVar(&f.ParallelRead, "parallel-read",
		models.DefaultServerRestoreParallelRead,
		"Number of threads per node that download the backup files, from 1 to 128.")
	flagSet.IntVarP(&f.ParallelWrite, "parallel-write", "w",
		models.DefaultServerRestoreParallelWrite,
		"Number of threads per node that write the records, from 1 to 128.")
	flagSet.IntVarP(&f.RecordsPerSecond, "records-per-second", "L",
		models.DefaultServerRestoreRecordsPerSecond,
		"Limit the records per second each node writes. If 0, no limit is applied.")
	flagSet.IntVar(&f.ExtraTTL, "extra-ttl",
		models.DefaultServerRestoreExtraTTL,
		"Seconds to add to the expiry of every restored record that expires,\n"+
			"from 0 to 315360000 (10 years). Records that never expire are unchanged.")
	flagSet.StringVar(&f.Policy, "policy",
		models.DefaultServerRestorePolicy,
		"Restore policy: strict or fuzzy. strict, the cold restore, hydrates the\n"+
			"partitions of a prepared namespace from strict backups only. fuzzy\n"+
			"writes the records of a single backup of any consistency into the\n"+
			"live namespace.\n"+
			"If empty, the server default is used, which is strict.\n"+
			"The flags below apply to the fuzzy policy only.")
	flagSet.BoolVar(&f.AllowUnhosted, "allow-unhosted",
		models.DefaultServerRestoreAllowUnhosted,
		"Allow the restore even if the principal does not host the namespace.")
	flagSet.IntVar(&f.MaxInflight, "max-inflight",
		models.DefaultServerRestoreMaxInflight,
		"Maximum number of concurrent writes, up to 100000.")
	flagSet.IntVar(&f.RetryBaseInterval, "retry-base-interval",
		models.DefaultServerRestoreRetryBaseInterval,
		"Base delay before a retry (in ms). 0 retries without a delay.")
	flagSet.Float64Var(&f.RetryMultiplier, "retry-multiplier",
		models.DefaultServerRestoreRetryMultiplier,
		"Backoff multiplier of the retry delay. Must be at least 1.0.")
	flagSet.IntVar(&f.RetryMaxAttempts, "retry-max-attempts",
		models.DefaultServerRestoreRetryMaxAttempts,
		"Maximum number of attempts per record, the first one included.")
	flagSet.BoolVar(&f.IgnoreRecordError, "ignore-record-error",
		models.DefaultServerRestoreIgnoreRecordError,
		"Continue the restore on errors of individual records.")
	flagSet.BoolVar(&f.ApplyMetadataLast, "apply-metadata-last",
		models.DefaultServerRestoreApplyMetadataLast,
		"Create secondary indexes and UDFs after the records instead of before.")

	return flagSet
}

func (f *ServerRestore) GetServerRestore() *models.ServerRestore {
	return &f.ServerRestore
}

// ServerRestorePrepare holds flags for the server restore prepare command.
type ServerRestorePrepare struct {
	models.ServerRestorePrepare
}

func NewServerRestorePrepare() *ServerRestorePrepare {
	return &ServerRestorePrepare{}
}

func (f *ServerRestorePrepare) NewFlagSet() *pflag.FlagSet {
	flagSet := &pflag.FlagSet{}

	flagSet.StringVarP(&f.Namespace, "namespace", "n",
		models.DefaultCommonNamespace,
		"The namespace to restore.")
	flagSet.StringVar(&f.JobID, "job-id",
		models.DefaultServerRestoreJobID,
		"Id of the restore job. The restore must be started with the same job id.")
	flagSet.BoolVar(&f.HydrateReplica, "hydrate-replica",
		models.DefaultServerRestoreHydrateReplica,
		"Restore all replicas. If false, only the master is restored and\n"+
			"the other replicas are filled by migrations.")
	flagSet.BoolVar(&f.Async, "async",
		models.DefaultServerRestoreAsync,
		"Return as soon as the cluster accepts the request, without\n"+
			"waiting for the namespace to become ready for the restore.")

	return flagSet
}

func (f *ServerRestorePrepare) GetServerRestorePrepare() *models.ServerRestorePrepare {
	return &f.ServerRestorePrepare
}

type ServerRestoreProgress struct {
	models.ServerRestoreProgress
}

func NewServerRestoreProgress() *ServerRestoreProgress {
	return &ServerRestoreProgress{}
}

func (f *ServerRestoreProgress) NewFlagSet() *pflag.FlagSet {
	flagSet := &pflag.FlagSet{}

	flagSet.StringVarP(&f.Namespace, "namespace", "n",
		models.DefaultCommonNamespace,
		"The namespace the restore writes into.")
	flagSet.StringVar(&f.JobID, "job-id",
		models.DefaultServerRestoreJobID,
		"Id of the restore job to follow.")
	flagSet.BoolVar(&f.Watch, "watch",
		models.DefaultServerRestoreProgressWatch,
		"Watch the progress of the restore.")

	return flagSet
}

func (f *ServerRestoreProgress) GetServerRestoreProgress() *models.ServerRestoreProgress {
	return &f.ServerRestoreProgress
}

// ServerRestoreAbort holds flags for the server restore abort command.
type ServerRestoreAbort struct {
	models.ServerRestoreAbort
}

// NewServerRestoreAbort initializes and returns a new instance of ServerRestoreAbort.
func NewServerRestoreAbort() *ServerRestoreAbort {
	return &ServerRestoreAbort{}
}

func (f *ServerRestoreAbort) NewFlagSet() *pflag.FlagSet {
	flagSet := &pflag.FlagSet{}

	flagSet.StringVarP(&f.Namespace, "namespace", "n",
		models.DefaultCommonNamespace,
		"The namespace the restore is aborted for.")
	flagSet.StringVar(&f.JobID, "job-id",
		models.DefaultServerRestoreJobID,
		"Id of the restore job to abort.")
	flagSet.BoolVar(&f.Async, "async",
		models.DefaultServerRestoreAsync,
		"Return as soon as the cluster accepts the abort, without\n"+
			"waiting for the restore to stop.")

	return flagSet
}

func (f *ServerRestoreAbort) GetServerRestoreAbort() *models.ServerRestoreAbort {
	return &f.ServerRestoreAbort
}

// ServerRestoreOpen holds flags for the server restore open command.
type ServerRestoreOpen struct {
	models.ServerRestoreOpen
}

// NewServerRestoreOpen initializes and returns a new instance of ServerRestoreOpen.
func NewServerRestoreOpen() *ServerRestoreOpen {
	return &ServerRestoreOpen{}
}

// NewFlagSet returns the flags of the server restore open command.
func (f *ServerRestoreOpen) NewFlagSet() *pflag.FlagSet {
	flagSet := &pflag.FlagSet{}

	flagSet.StringVarP(&f.Namespace, "namespace", "n",
		models.DefaultCommonNamespace,
		"The namespace to release.")
	flagSet.StringVar(&f.JobID, "job-id",
		models.DefaultServerRestoreJobID,
		"Id of the aborted cold restore job that holds the namespace.")
	flagSet.BoolVar(&f.Async, "async",
		models.DefaultServerRestoreAsync,
		"Return as soon as the cluster accepts the request, without\n"+
			"waiting for the namespace to be released.")

	return flagSet
}

// GetServerRestoreOpen returns the restore open settings the flags were parsed into.
func (f *ServerRestoreOpen) GetServerRestoreOpen() *models.ServerRestoreOpen {
	return &f.ServerRestoreOpen
}

// ServerRestoreClearStorage holds flags for the server restore clear-storage command.
type ServerRestoreClearStorage struct {
	models.ServerRestoreClearStorage
}

// NewServerRestoreClearStorage initializes and returns a new instance of ServerRestoreClearStorage.
func NewServerRestoreClearStorage() *ServerRestoreClearStorage {
	return &ServerRestoreClearStorage{}
}

// NewFlagSet returns the flags of the server restore clear-storage command.
func (f *ServerRestoreClearStorage) NewFlagSet() *pflag.FlagSet {
	flagSet := &pflag.FlagSet{}

	flagSet.StringVarP(&f.Namespace, "namespace", "n",
		models.DefaultCommonNamespace,
		"The namespace to clear.")
	flagSet.StringVar(&f.JobID, "job-id",
		models.DefaultServerRestoreJobID,
		"Id of the cold restore job being prepared.")
	flagSet.BoolVar(&f.Async, "async",
		models.DefaultServerRestoreAsync,
		"Return as soon as the nodes accept the clear, without waiting\n"+
			"for the namespace to become ready for the restore.")

	return flagSet
}

// GetServerRestoreClearStorage returns the restore clear-storage settings the flags were
// parsed into.
func (f *ServerRestoreClearStorage) GetServerRestoreClearStorage() *models.ServerRestoreClearStorage {
	return &f.ServerRestoreClearStorage
}
