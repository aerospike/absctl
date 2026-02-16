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

package flags

import (
	"github.com/aerospike/absctl/internal/models"
	"github.com/spf13/pflag"
)

type Backup struct {
	models.Backup
}

func NewBackup() *Backup {
	return &Backup{}
}

func (f *Backup) NewFlagSet() *pflag.FlagSet {
	flagSet := &pflag.FlagSet{}

	flagSet.IntVar(&f.MaxRetries, "max-retries",
		models.DefaultBackupMaxRetries,
		"Maximum number of retries before aborting the current transaction.")

	flagSet.BoolVarP(&f.RemoveFiles, "remove-files", "r",
		models.DefaultBackupRemoveFiles,
		"Remove an existing backup file (-o) or entire directory (-d) and replace with the new backup.")

	flagSet.BoolVar(&f.RemoveArtifacts, "remove-artifacts",
		models.DefaultBackupRemoveArtifacts,
		"Remove existing backup file (-o) or files (-d) without performing a backup.")

	flagSet.StringVarP(&f.OutputFile, "output-file", "o",
		models.DefaultBackupOutputFile,
		"Backup to a single backup file. Use '-' for stdout. Required, unless -d or -e is used.\n"+
			"--file-limit will be ignored if this parameter is used.")
	flagSet.StringVarP(&f.OutputFilePrefix, "output-file-prefix", "q",
		"",
		"When using directory parameter, prepend a prefix to the names of the generated files.\n"+
			"Not applicable when --output-file is used. ")
	flagSet.Uint64VarP(&f.FileLimit, "file-limit", "F",
		models.DefaultBackupFileLimit,
		"Rotate backup files when their size crosses the given\n"+
			"value (in MiB). Only used when backing up to a directory.\n")

	flagSet.BoolVarP(&f.NoBins, "no-bins", "x",
		models.DefaultBackupNoBins,
		"Do not include bin data in the backup. Use this flag for data sampling or troubleshooting.\n"+
			"On restore, all records not containing bin data will be skipped.")

	flagSet.BoolVar(&f.NoTTLOnly, "no-ttl-only",
		models.DefaultBackupNoTTLOnly,
		"Only include records that have no TTL set (persistent records).")

	flagSet.StringVarP(&f.AfterDigest, "after-digest", "D",
		models.DefaultBackupAfterDigest,
		"Backup records after record digest in record's partition plus all succeeding\n"+
			"partitions. Used to resume backup with last record received from previous\n"+
			"incomplete backup.\n"+
			"This argument is mutually exclusive with partition-list.\n"+
			"Format: Base64 encoded string\n"+
			"Example: EjRWeJq83vEjRRI0VniavN7xI0U=\n")

	flagSet.StringVarP(&f.ModifiedAfter, "modified-after", "a",
		models.DefaultBackupModifiedAfter,
		"<YYYY-MM-DD_HH:MM:SS>\n"+
			"Perform an incremental backup; only include records \n"+
			"that changed after the given date and time. The system's \n"+
			"local timezone applies. If only HH:MM:SS is specified, then\n"+
			"today's date is assumed as the date. If only YYYY-MM-DD is \n"+
			"specified, then 00:00:00 (midnight) is assumed as the time.\n")

	flagSet.StringVarP(&f.ModifiedBefore, "modified-before", "b",
		models.DefaultBackupModifiedBefore,
		"<YYYY-MM-DD_HH:MM:SS>\n"+
			"Only include records that last changed before the given\n"+
			"date and time. May combined with --modified-after to specify a range.")

	flagSet.StringVarP(&f.FilterExpression, "filter-exp", "f",
		models.DefaultBackupFilterExpression,
		"Base64 encoded filter expression. Use the encoded filter expression in each scan call,\n"+
			"which can be used to do a partial backup. The expression to be used can be Base64 \n"+
			"encoded through any client. This argument is mutually exclusive with multi-set backup.\n")

	flagSet.StringVarP(&f.NodeList, "node-list", "l",
		models.DefaultBackupNodeList,
		"<addr 1>:<port 1>[,<addr 2>:<port 2>[,...]]\n"+
			"<node name 1>[,<node name 2>[,...]]\n"+
			"To get the correct node address, use the info command 'service-tls-std' if the database is configured to use TLS\n"+
			"or 'service-clear-std' if no TLS is configured.\n"+
			"To get the node name, use the 'node:' info command.\n"+
			"Back up the given cluster nodes only.\n"+
			"This argument is mutually exclusive with --partition-list, --after-digest, --rack-list, --prefer-racks"+
			" arguments.\n"+
			"Default: back up all nodes in the cluster")

	flagSet.StringVarP(&f.PartitionList, "partition-list", "X",
		models.DefaultBackupPartitionList,
		"List of partitions <filter[,<filter>[...]]> to back up. Partition filters can be ranges,\n"+
			"individual partitions, or records after a specific digest within a single partition.\n"+
			"To use this argument, --parallel must be set equal to or greater\n"+
			"than the number of elements in the partition list\n"+
			"This argument is mutually exclusive with after-digest.\n"+
			"Filter: <begin partition>[-<partition count>]|<digest>\n"+
			"begin partition: 0-4095\n"+
			"partition count: 1-4096 Default: 1\n"+
			"digest: Base64 encoded string\n"+
			"Examples: 0-1000, 1000-1000, 2222, EjRWeJq83vEjRRI0VniavN7xI0U=\n"+
			"Default: 0-4096 (all partitions)\n")

	flagSet.StringVar(&f.PreferRacks, "prefer-racks",
		models.DefaultBackupPreferRacks,
		"<rack id 1>[,<rack id 2>[,...]]\n"+
			"A list of Aerospike Database rack IDs to prefer when reading records for a backup.\n"+
			"This argument is mutually exclusive with --rack-list and --node-list.")

	flagSet.StringVar(&f.RackList, "rack-list",
		models.DefaultBackupRackList,
		"<rack id 1>[,<rack id 2>[,...]]\n"+
			"A list of Aerospike Database rack IDs to backup.\n"+
			"Unlike --prefer-racks, only specified racks will be backed up.\n"+
			"This argument is mutually exclusive with --prefer-racks and --node-list.")

	flagSet.Int64VarP(&f.MaxRecords, "max-records", "M",
		models.DefaultBackupMaxRecords,
		"The number of records approximately to back up. 0 - all records.\n"+
			"To use this argument, --parallel must be set to 1.")

	flagSet.IntVar(&f.SleepBetweenRetries, "sleep-between-retries",
		models.DefaultBackupSleepBetweenRetries,
		"The amount of milliseconds to sleep between retries after an error.\n"+
			"This field is ignored when --max-retries is zero.")

	flagSet.BoolVarP(&f.Compact, "compact", "C",
		models.DefaultBackupCompact,
		"If true, do not apply Base64 encoding to BLOBs and instead write raw binary data,\n"+
			"resulting in smaller backup files.")

	flagSet.BoolVarP(&f.Estimate, "estimate", "e",
		models.DefaultBackupEstimate,
		"Estimate the backed-up record size from a random sample of \n"+
			"10,000 (default) records at 99.9999% confidence to estimate the full backup size.\n"+
			"It ignores any filter:  --filter-exp, --node-list, --modified-after, --modified-before, --no-ttl-only,\n"+
			"--after-digest, --partition-list.")

	flagSet.Int64Var(&f.EstimateSamples, "estimate-samples",
		models.DefaultBackupEstimateSamples,
		"The number of samples to take when running a backup estimate.")

	flagSet.StringVar(&f.StateFileDst, "state-file-dst",
		models.DefaultBackupStateFileDst,
		"Name of a state file that will be saved in backup --directory.\n"+
			"Works only with --file-limit parameter. As --file-limit is reached and the file is closed,\n"+
			"the current state will be saved. Works only for default and/or partition backup.\n"+
			"Not work with --rack-list or --node--list.")

	flagSet.StringVarP(&f.Continue, "continue", "c",
		models.DefaultBackupContinue,
		"Resumes an interrupted/failed backup from where it was left off, given the .state file\n"+
			"that was generated from the interrupted/failed run.\n"+
			"--continue and --state-file-dst are mutually exclusive.")

	flagSet.Int64Var(&f.ScanPageSize, "scan-page-size",
		models.DefaultBackupScanPageSize,
		"Number of records will be read on one iteration for continuation backup.\n"+
			"Affects size if overlap on resuming backup after an error.\n"+
			"Used only with --state-file-dst or --continue.")

	return flagSet
}

func (f *Backup) GetBackup() *models.Backup {
	return &f.Backup
}
