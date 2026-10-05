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
	flagSet.StringVar(&f.StorageType, "object-storage-type",
		models.DefaultServerBackupObjectStorageType,
		"Type of object storage. Example: aws-s3")
	flagSet.StringVar(&f.BackupID, "backup-id",
		models.DefaultServerBackupJobID,
		"Id of the backup to restore from.")
	flagSet.StringVar(&f.JobID, "job-id",
		models.DefaultServerRestoreJobID,
		"Id of the restore job. A cold restore must use the job id passed to\n"+
			"the restore preparation. If empty, the backup id is used.")
	flagSet.StringVar(&f.Path, "path",
		models.DefaultServerBackupPath,
		"Key prefix in the bucket the backup was written under.")
	flagSet.StringVarP(&f.SetList, "set-list", "s",
		models.DefaultCommonSetList,
		descSetListRestore)
	flagSet.StringVar(&f.FilterExp, "filter-exp",
		models.DefaultServerFilterExp,
		"Base64 encoded filter expression. Cold restore only.")
	flagSet.BoolVarP(&f.NoIndexes, "no-indexes", "I",
		models.DefaultCommonNoIndexes,
		descNoIndexesRestore)
	flagSet.BoolVar(&f.NoUDFs, "no-udfs",
		models.DefaultCommonNoUDFs,
		descNoUDFsRestore)
	flagSet.BoolVar(&f.FuzzyRestore, "fuzzy-restore",
		models.DefaultServerRestoreFuzzyRestore,
		"Restore by writing records into the live namespace instead of\n"+
			"the cold partition hydration. The flags below apply to a fuzzy restore only.")
	flagSet.BoolVar(&f.AllowUnhosted, "allow-unhosted",
		models.DefaultServerRestoreAllowUnhosted,
		"Allow the restore even if the principal does not host the namespace.")
	flagSet.IntVarP(&f.Parallel, "parallel", "w",
		models.DefaultServerRestoreParallel,
		"Number of worker threads, from 1 to 128.")
	flagSet.IntVarP(&f.RecordsPerSecond, "records-per-second", "L",
		models.DefaultServerRestoreRecordsPerSecond,
		"Limit total written records per second. If 0, no limit is applied.")
	flagSet.IntVar(&f.MaxInflight, "max-inflight",
		models.DefaultServerRestoreMaxInflight,
		"Maximum number of concurrent writes, up to 100000.")
	flagSet.IntVar(&f.RetryBaseInterval, "retry-base-interval",
		models.DefaultServerRestoreRetryBaseInterval,
		"Base delay before a retry (in ms).")
	flagSet.Float64Var(&f.RetryMultiplier, "retry-multiplier",
		models.DefaultServerRestoreRetryMultiplier,
		"Backoff multiplier of the retry delay. Must be at least 1.0.")
	flagSet.IntVar(&f.RetryMaxAttempts, "retry-max-attempts",
		models.DefaultServerRestoreRetryMaxAttempts,
		"Maximum number of attempts per record.")
	flagSet.BoolVar(&f.IgnoreRecordError, "ignore-record-error",
		models.DefaultServerRestoreIgnoreRecordError,
		"Continue the restore on errors of individual records.")

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
		"The namespace to check progress.")

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

	return flagSet
}

func (f *ServerRestoreAbort) GetServerRestoreAbort() *models.ServerRestoreAbort {
	return &f.ServerRestoreAbort
}
