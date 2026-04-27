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

// ServiceConfigPolicy holds flags for the `service config policies add|update`
// commands. Each leaf field of DtoBackupPolicy is exposed as an individual
// flag; --file may be used as a base body that flags then override.
type ServiceConfigPolicy struct {
	models.ConfigPolicyFields
}

func NewServiceConfigPolicy() *ServiceConfigPolicy {
	return &ServiceConfigPolicy{}
}

func (f *ServiceConfigPolicy) NewFlagSet() *pflag.FlagSet {
	flagSet := &pflag.FlagSet{}

	flagSet.StringVar(&f.Name, "name", "",
		"Policy name (used as the URL path parameter).")

	flagSet.StringVar(&f.File, "file", "",
		"Path to a JSON file with a DtoBackupPolicy body. "+
			"Other flags override fields loaded from this file.")

	flagSet.IntVar(&f.Bandwidth, "bandwidth", 0,
		"Throttle backup write speed (MiB/s). 0 means no limit.")

	flagSet.IntVar(&f.FileLimit, "file-limit", 0,
		"Backup file size limit in MB before splitting.")

	flagSet.IntVar(&f.MaxConcurrentNodes, "max-concurrent-nodes", 0,
		"Maximum number of concurrent requests to server nodes.")

	flagSet.IntVar(&f.Parallel, "parallel", 0,
		"Maximum number of scan calls to run in parallel.")

	flagSet.IntVar(&f.ParallelWrite, "parallel-write", 0,
		"Maximum number of threads writing backup files.")

	flagSet.IntVar(&f.RecordsPerSecond, "records-per-second", 0,
		"Limit the number of records returned per second.")

	flagSet.IntVar(&f.SocketTimeout, "socket-timeout", 0,
		"Socket timeout in milliseconds.")

	flagSet.IntVar(&f.TotalTimeout, "total-timeout", 0,
		"Total socket timeout in milliseconds.")

	flagSet.BoolVar(&f.ConcurrentIncremental, "concurrent-incremental", false,
		"Allow incremental backups to run concurrently for the same routine.")

	flagSet.BoolVar(&f.NoIndexes, "no-indexes", false,
		"Do not back up secondary index definitions.")

	flagSet.BoolVar(&f.NoRecords, "no-records", false,
		"Do not back up record data (metadata or bins).")

	flagSet.BoolVar(&f.NoUdfs, "no-udfs", false,
		"Do not back up UDF modules.")

	flagSet.BoolVar(&f.Sealed, "sealed", false,
		"Only back up records last modified before the backup started.")

	flagSet.BoolVar(&f.UseScanCompression, "use-scan-compression", false,
		"Enable built-in scan compression (Aerospike Enterprise only).")

	flagSet.BoolVar(&f.WithClusterConfiguration, "with-cluster-configuration", false,
		"Back up the Aerospike cluster configuration alongside data.")

	flagSet.StringVar(&f.CompressionMode, "compression-mode", "",
		"Compression mode (NONE, ZSTD, ...).")

	flagSet.IntVar(&f.CompressionLevel, "compression-level", 0,
		"Compression level (algorithm specific; ignored when mode is NONE).")

	flagSet.StringVar(&f.EncryptionMode, "encryption-mode", "",
		"Encryption mode (NONE, AES128, AES256).")

	flagSet.StringVar(&f.EncryptionKeyEnv, "encryption-key-env", "",
		"Environment variable holding the encryption key.")

	flagSet.StringVar(&f.EncryptionKeyFile, "encryption-key-file", "",
		"Path to a file containing the encryption key.")

	flagSet.StringVar(&f.EncryptionKeySecret, "encryption-key-secret", "",
		"Secret keyword in Aerospike Secret Agent for the encryption key.")

	flagSet.IntVar(&f.RetentionFull, "retention-full", 0,
		"Number of full backups to retain (0 means unset).")

	flagSet.IntVar(&f.RetentionIncremental, "retention-incremental", 0,
		"Number of full backups to retain incrementals for (0 means unset).")

	flagSet.IntVar(&f.RetryBaseTimeout, "retry-base-timeout", 0,
		"Initial retry delay in milliseconds.")

	flagSet.IntVar(&f.RetryMaxRetries, "retry-max-retries", 0,
		"Maximum number of retry attempts.")

	flagSet.Float32Var(&f.RetryMultiplier, "retry-multiplier", 0,
		"Retry delay multiplier (BaseTimeout * Multiplier^attempt).")

	return flagSet
}
