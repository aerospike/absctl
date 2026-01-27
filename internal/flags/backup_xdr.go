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

type BackupXDR struct {
	models.BackupXDR
}

func NewBackupXDR() *BackupXDR {
	return &BackupXDR{}
}

func (f *BackupXDR) NewFlagSet() *pflag.FlagSet {
	flagSet := &pflag.FlagSet{}

	flagSet.StringVarP(&f.Namespace, "namespace", "n",
		models.DefaultBackupXDRNamespace,
		"The namespace to be backed up. Required.")

	flagSet.StringVarP(&f.Directory, "directory", "d",
		models.DefaultBackupXDRDirectory,
		"The directory that holds the backup files. Required.")

	flagSet.BoolVarP(&f.RemoveFiles, "remove-files", "r",
		models.DefaultBackupXDRRemoveFiles,
		"Remove an existing backup file (-o) or entire directory (-d) and replace with the new backup.")

	flagSet.Uint64VarP(&f.FileLimit, "file-limit", "F",
		models.DefaultBackupXDRFileLimit,
		"Rotate backup files when their size crosses the given\n"+
			"value (MiB). Only used when backing up to a directory.")

	flagSet.IntVar(&f.ParallelWrite, "parallel-write",
		models.DefaultBackupXDRParallelWrite,
		"Number of concurrent backup files writing.\n"+
			"If not set, the default value is automatically calculated and appears as the number of CPUs on your machine.")

	flagSet.StringVar(&f.DC, "dc",
		models.DefaultBackupXDRDC,
		"DC that will be created on source instance for xdr backup.\n"+
			"DC name can include only Latin lowercase and uppercase letters with no diacritical marks (a-z, A-Z),\n"+
			"digits 0-9, underscores (_), hyphens (-), and dollar signs ($). Max length is 31 bytes.")

	flagSet.BoolVar(&f.Forward, "forward",
		models.DefaultBackupXDRForward,
		"By default XDR writes that originated from another XDR are not forwarded to the specified\n"+
			"destination datacenters. Setting this parameter to true sends writes that originated from another XDR\n"+
			"to the specified destination datacenters.")

	flagSet.StringVar(&f.LocalAddress, "local-address",
		models.DefaultBackupXDRLocalAddress,
		"Local IP address that the XDR server listens on.")

	flagSet.IntVar(&f.LocalPort, "local-port",
		models.DefaultBackupXDRLocalPort,
		"Local port that the XDR server listens on.")

	flagSet.StringVar(&f.Rewind, "rewind",
		models.DefaultBackupXDRRewind,
		"Rewind is used to ship all existing records of a namespace.\n"+
			"When rewinding a namespace, XDR will scan through the index and ship\n"+
			"all the records for that namespace, partition by partition.\n"+
			"Can be the string \"all\" or an integer number of seconds.")

	flagSet.IntVar(&f.MaxThroughput, "max-throughput",
		models.DefaultBackupXDRMaxThroughput,
		"Number of records per second to ship using XDR.\n"+
			"The --max-throughput value should be in multiples of 100.\n"+
			"If 0, the default server value will be used.")

	flagSet.Int64Var(&f.ReadTimeoutMilliseconds, "read-timeout",
		models.DefaultBackupXDRReadTimeout,
		"Timeout (in ms) for TCP read operations. Used by TCP server for XDR.")

	flagSet.Int64Var(&f.WriteTimeoutMilliseconds, "write-timeout",
		models.DefaultBackupXDRWriteTimeout,
		"Timeout (in ms) for TCP write operations. Used by TCP server for XDR.")

	flagSet.IntVar(&f.ResultQueueSize, "results-queue-size",
		models.DefaultBackupXDRResultQueueSize,
		"Buffer for processing messages received from XDR.")

	flagSet.IntVar(&f.AckQueueSize, "ack-queue-size",
		models.DefaultBackupXDRAckQueueSize,
		"Buffer for processing acknowledge messages sent to XDR.")

	flagSet.IntVar(&f.MaxConnections, "max-connections",
		models.DefaultBackupXDRMaxConnections,
		"Maximum number of concurrent TCP connections.")

	flagSet.Int64Var(&f.InfoPolingPeriodMilliseconds, "info-poling-period",
		models.DefaultBackupXDRInfoPolingPeriod,
		"How often ((in ms)) a backup client sends info commands\n"+
			"to check Aerospike cluster statistics on recovery rate and lag.")

	flagSet.Int64Var(&f.InfoRetryIntervalMilliseconds, "info-retry-interval",
		models.DefaultBackupXDRInfoRetryInterval,
		"Set the initial interval for a retry (in ms) when info commands are sent.\n"+
			"This parameter is applied to stop-xdr and unblock-mrt requests.")

	flagSet.Float64Var(&f.InfoRetriesMultiplier, "info-retry-multiplier",
		models.DefaultBackupXDRInfoRetriesMultiplier,
		"Increases the delay between subsequent retry attempts.\n"+
			"The actual delay is calculated as: info-retry-interval * (info-retry-multiplier ^ attemptNumber)")

	flagSet.UintVar(&f.InfoMaxRetries, "info-max-retries",
		models.DefaultBackupXDRInfoMaxRetries,
		"How many times to retry sending info commands before failing.\n"+
			" This parameter is applied to stop-xdr and unblock-mrt requests.")

	flagSet.Int64Var(&f.StartTimeoutMilliseconds, "start-timeout",
		models.DefaultBackupXDRStartTimeout,
		"Timeout for starting TCP server for XDR.\n"+
			"If the TCP server for XDR does not receive any data within this timeout period, it will shut down.\n"+
			"This situation can occur if the --local-address and --local-port options are misconfigured.")

	flagSet.BoolVar(&f.StopXDR, "stop-xdr",
		models.DefaultBackupXDRStopXDR,
		"Stops XDR and removes XDR configuration from the database.\n"+
			"Used if previous XDR backup was interrupted or failed, but the database server still sends XDR events.\n"+
			"Use this functionality to stop XDR after an interrupted backup.")

	flagSet.BoolVar(&f.UnblockMRT, "unblock-mrt",
		models.DefaultBackupXDRUnblockMRT,
		"Unblock MRT writes on the database.\n"+
			"Use this functionality to unblock MRT writes after an interrupted backup.")

	flagSet.Int64VarP(&f.InfoTimeout, "info-timeout", "T",
		models.DefaultBackupXDRInfoTimeout,
		"Set the timeout (ms) for asinfo commands sent from backup tool to the database.\n"+
			"The info commands are to check version, get indexes, get udfs, count records, and check batch write support.")

	return flagSet
}

func (f *BackupXDR) GetBackupXDR() *models.BackupXDR {
	return &f.BackupXDR
}
