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

// descSetListServerBackup differs from descSetListBackup: a server-integrated backup
// accepts a filter expression together with several sets.
const descSetListServerBackup = "The set(s) to be backed up. Accepts comma-separated values with no spaces: " +
	"'set1,set2,set3'\nIf empty, include all sets."

// descPathServerBackupWritten describes --path of the commands that read a backup back.
const descPathServerBackupWritten = "Key prefix in the bucket the backup was written under.\n" +
	"Must match the --path the backup was started with."

// ServerBackup holds flags for the server backup create command.
type ServerBackup struct {
	models.ServerBackup
}

func NewServerBackup() *ServerBackup {
	return &ServerBackup{}
}

func (f *ServerBackup) NewFlagSet() *pflag.FlagSet {
	flagSet := &pflag.FlagSet{}

	flagSet.StringVarP(&f.Namespace, "namespace", "n",
		models.DefaultCommonNamespace,
		"The namespace to be backed up.")
	flagSet.StringVar(&f.Path, "path",
		models.DefaultServerBackupPath,
		"Key prefix in the bucket to write the backup under.")
	flagSet.StringVarP(&f.ModifiedAfter, "modified-after", "a",
		models.DefaultBackupModifiedAfter,
		"<YYYY-MM-DD_HH:MM:SS>\n"+
			"Perform an incremental backup; only include records \n"+
			"that changed after the given date and time. The system's \n"+
			"local timezone applies. If only HH:MM:SS is specified, then\n"+
			"today's date is assumed as the date. If only YYYY-MM-DD is \n"+
			"specified, then 00:00:00 (midnight) is assumed as the time.\n"+
			"Must be after 2010-01-01 and not in the future.\n")

	flagSet.StringVarP(&f.ModifiedBefore, "modified-before", "b",
		models.DefaultBackupModifiedBefore,
		"<YYYY-MM-DD_HH:MM:SS>\n"+
			"Only include records that last changed before the given\n"+
			"date and time. May be combined with --modified-after to specify a range.\n"+
			"Must be after 2010-01-01.")
	flagSet.StringVarP(&f.SetList, "set-list", "s",
		models.DefaultCommonSetList,
		descSetListServerBackup)
	flagSet.StringVarP(&f.BinList, "bin-list", "B",
		models.DefaultCommonBinList,
		descBinListBackup)
	flagSet.StringVarP(&f.FilterExp, "filter-exp", "f",
		models.DefaultServerFilterExp,
		"Base64 encoded filter expression. Only live records are filtered,\n"+
			"tombstones are always backed up.")
	flagSet.BoolVarP(&f.NoIndexes, "no-indexes", "I",
		models.DefaultCommonNoIndexes,
		"Exclude indexes from the backup.")
	flagSet.BoolVarP(&f.NoUDFs, "no-udfs", "u",
		models.DefaultCommonNoUDFs,
		"Exclude user-defined functions from the backup.")
	flagSet.BoolVar(&f.NoConfig, "no-config",
		models.DefaultServerBackupNoConfig,
		"Do not upload the config file of each node with the backup.")
	flagSet.StringVar(&f.Policy, "policy",
		models.DefaultServerBackupPolicy,
		"Backup policy: strict, best-effort or fuzzy. strict aborts the backup\n"+
			"on any failure. best-effort commits the backup as fuzzy instead of\n"+
			"aborting it on a failure after the base scan is durable cluster-wide.\n"+
			"fuzzy runs the base scan only, without the change stream.\n"+
			"The value is case-insensitive. If empty, the server default is used.")
	flagSet.BoolVar(&f.Async, "async",
		models.DefaultServerBackupAsync,
		"Return as soon as the cluster accepts the backup, without\n"+
			"following its progress. Use the progress command to follow\n"+
			"the backup later.")
	flagSet.IntVarP(&f.RecordsPerSecond, "records-per-second", "L",
		models.DefaultServerBackupRecordsPerSecond,
		"Limit the records per second of each scan on each node.\n"+
			"A non-zero value must be at least 5 x --parallel-readers, or 20 when\n"+
			"--parallel-readers is 0 and the server uses its default of 4 threads.\n"+
			"If 0, the server default is used, which is no limit.")
	flagSet.IntVar(&f.ParallelReaders, "parallel-readers",
		models.DefaultServerBackupParallelReaders,
		"Number of scan threads per node, from 1 to 128. The threads read the\n"+
			"master partitions of the namespace and pack the records into segments\n"+
			"for upload. Raise it when the scan is limited by CPU or device reads.\n"+
			"It does not help when the upload is the bottleneck: the scan pauses\n"+
			"while the upload queue is full. More threads need more memory on each\n"+
			"node for segment buffers, which is not counted against namespace memory.\n"+
			"Scan and upload threads together must fit within the max-backup-threads\n"+
			"server config. If 0, the server default is used, which is 4.")
	flagSet.IntVar(&f.ParallelUploadThreads, "parallel-upload-threads",
		models.DefaultServerBackupParallelUploadThreads,
		"Number of upload threads per node, from 1 to 128. The threads take the\n"+
			"segments the scan threads fill and upload them to the object storage.\n"+
			"Raise it when the upload is the bottleneck. Upload threads add no\n"+
			"segment buffers. Scan and upload threads together must fit within the\n"+
			"max-backup-threads server config.\n"+
			"If 0, the server default is used, which is 8.")

	return flagSet
}

func (f *ServerBackup) GetServerBackup() *models.ServerBackup {
	return &f.ServerBackup
}

// ServerBackupList holds flags for the server backup list command.
type ServerBackupList struct {
	models.ServerBackupList
}

func NewServerBackupList() *ServerBackupList {
	return &ServerBackupList{}
}

func (f *ServerBackupList) NewFlagSet() *pflag.FlagSet {
	flagSet := &pflag.FlagSet{}

	flagSet.StringVar(&f.Path, "path", models.DefaultServerBackupPath, "Path to list backups from.")

	return flagSet
}

func (f *ServerBackupList) GetServerBackupList() *models.ServerBackupList {
	return &f.ServerBackupList
}

// ServerBackupValidate holds flags for the server backup validate command.
type ServerBackupValidate struct {
	models.ServerBackupValidate
}

func NewServerBackupValidate() *ServerBackupValidate {
	return &ServerBackupValidate{}
}

func (f *ServerBackupValidate) NewFlagSet() *pflag.FlagSet {
	flagSet := &pflag.FlagSet{}

	flagSet.IntVar(&f.SampleSize, "sample-size",
		models.DefaultServerBackupValidateSampleSize,
		"Number of segments for random validation. 0 means validate all segments.")
	flagSet.StringVar(&f.JobID, "backup-id",
		models.DefaultServerBackupJobID,
		"Backup id")
	flagSet.StringVar(&f.Path, "path",
		models.DefaultServerBackupPath,
		descPathServerBackupWritten)

	return flagSet
}

func (f *ServerBackupValidate) GetServerBackupValidate() *models.ServerBackupValidate {
	return &f.ServerBackupValidate
}

type ServerBackupProgress struct {
	models.ServerBackupProgress
}

// NewServerBackupProgress initializes and returns a new instance of ServerBackupProgress.
func NewServerBackupProgress() *ServerBackupProgress {
	return &ServerBackupProgress{}
}

func (f *ServerBackupProgress) NewFlagSet() *pflag.FlagSet {
	flagSet := &pflag.FlagSet{}

	flagSet.StringVar(&f.JobID, "backup-id",
		models.DefaultServerBackupJobID,
		"Backup id")
	flagSet.StringVar(&f.Path, "path",
		models.DefaultServerBackupPath,
		descPathServerBackupWritten)
	flagSet.BoolVar(&f.Watch, "watch",
		models.DefaultServerBackupProgressWatch,
		"Watch the progress of the backup.")

	return flagSet
}

func (f *ServerBackupProgress) GetServerBackupProgress() *models.ServerBackupProgress {
	return &f.ServerBackupProgress
}

type ServerBackupAbort struct {
	models.ServerBackupAbort
}

// NewServerBackupAbort initializes and returns a new instance of ServerBackupAbort.
func NewServerBackupAbort() *ServerBackupAbort {
	return &ServerBackupAbort{}
}

func (f *ServerBackupAbort) NewFlagSet() *pflag.FlagSet {
	flagSet := &pflag.FlagSet{}

	flagSet.StringVar(&f.JobID, "backup-id",
		models.DefaultServerBackupJobID,
		"Backup id")
	flagSet.BoolVar(&f.Async, "async",
		models.DefaultServerBackupAsync,
		"Return as soon as the cluster accepts the abort, without\n"+
			"waiting for the backup to stop.")

	return flagSet
}

func (f *ServerBackupAbort) GetServerBackupAbort() *models.ServerBackupAbort {
	return &f.ServerBackupAbort
}
