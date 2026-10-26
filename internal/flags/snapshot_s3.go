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

const flagS3Throughput = "s3-throughput"

// descSnapshotS3Local is appended to the description of the flags that only the local
// S3 client uses: the server reaches the bucket with its own credentials and settings.
const descSnapshotS3Local = "\nUsed locally by absctl only, it is not sent to the server."

// SnapshotS3Mode selects the S3 flags a snapshot subcommand needs.
type SnapshotS3Mode int

const (
	// SnapshotS3ModeBackupStart is the "snapshot-backup start" set: the connection flags
	// and the target throughput of the server S3 client.
	SnapshotS3ModeBackupStart SnapshotS3Mode = iota + 1
	// SnapshotS3ModeRestoreStart is the "snapshot-restore start" set: the connection
	// flags only.
	SnapshotS3ModeRestoreStart
	// SnapshotS3ModeReader is the set of the subcommands that read the backup locally
	// ("snapshot-backup list", "snapshot-backup validate"): the connection flags and the
	// local S3 client settings.
	SnapshotS3ModeReader
	// SnapshotS3ModeProgress is the "snapshot-backup progress" set. It reads backup
	// metadata only, which has its own bounded request timeout, so --s3-request-timeout
	// is not part of it.
	SnapshotS3ModeProgress
)

// SnapshotS3 holds the S3 flags of the server-integrated snapshot subcommands.
type SnapshotS3 struct {
	mode SnapshotS3Mode
	models.SnapshotS3
}

// NewSnapshotS3 returns the S3 flags of a snapshot subcommand. The settings that mode
// does not expose keep their defaults.
func NewSnapshotS3(mode SnapshotS3Mode) *SnapshotS3 {
	return &SnapshotS3{
		mode:             mode,
		RetryMaxAttempts: models.DefaultS3RetryMaxAttempts,
		RetryMaxBackoff:  models.DefaultS3RetryMaxBackoff,
		MaxConnsPerHost:  models.DefaultCloudMaxConnsPerHost,
		RequestTimeout:   models.DefaultCloudRequestTimeout,
		Throughput:       models.DefaultSnapshotS3Throughput,
	}
}

// NewFlagSet returns the S3 flags of the mode the SnapshotS3 was created with.
func (f *SnapshotS3) NewFlagSet() *pflag.FlagSet {
	flagSet := &pflag.FlagSet{}

	flagSet.StringVar(&f.BucketName, flagS3BucketName,
		models.DefaultS3BucketName,
		"Existing S3 bucket name")

	flagSet.StringVar(&f.Region, flagS3Region,
		models.DefaultS3Region,
		"The S3 region that the bucket(s) exist in.")

	flagSet.StringVar(&f.Profile, flagS3Profile,
		models.DefaultS3Profile,
		"The S3 profile to use for credentials.")

	flagSet.StringVar(&f.AccessKeyID, flagS3AccessKeyID,
		models.DefaultS3AccessKeyID,
		"S3 access key ID. If not set, profile auth info will be used."+descSnapshotS3Local)

	flagSet.StringVar(&f.SecretAccessKey, flagS3SecretAccessKey,
		models.DefaultS3SecretAccessKey,
		"S3 secret access key. If not set, profile auth info will be used."+descSnapshotS3Local)

	flagSet.StringVar(&f.Endpoint, flagS3Endpoint,
		models.DefaultS3Endpoint,
		"An alternate URL endpoint to send S3 API calls to.")

	switch f.mode {
	case SnapshotS3ModeBackupStart:
		flagSet.IntVar(&f.Throughput, flagS3Throughput,
			models.DefaultSnapshotS3Throughput,
			"Target throughput of the S3 client per node, in gigabits per second\n"+
				"(Gbit/s), from 1 to 400. If 0, the server default is used.")
	case SnapshotS3ModeReader, SnapshotS3ModeProgress:
		f.addClientFlags(flagSet)
	case SnapshotS3ModeRestoreStart:
	}

	return flagSet
}

// addClientFlags adds the settings of the local S3 client to flagSet.
func (f *SnapshotS3) addClientFlags(flagSet *pflag.FlagSet) {
	flagSet.IntVar(&f.RetryMaxAttempts, flagS3RetryMaxAttempts,
		models.DefaultS3RetryMaxAttempts,
		"Maximum number of attempts that should be made in case of an error."+descSnapshotS3Local)

	flagSet.IntVar(&f.RetryMaxBackoff, flagS3RetryMaxBackoff,
		models.DefaultS3RetryMaxBackoff,
		"Max backoff duration (in ms) between retried attempts.\n"+
			"The delay increases exponentially with each retry up to the maximum specified by s3-retry-max-backoff."+
			descSnapshotS3Local)

	flagSet.IntVar(&f.MaxConnsPerHost, flagS3MaxConnsPerHost,
		models.DefaultCloudMaxConnsPerHost,
		"Max connections per host optionally limits the total number of connections per host,\n"+
			"including connections in the dialing, active, and idle states. On limit violation, dials will block.\n"+
			"0 means no limit."+descSnapshotS3Local)

	if f.mode == SnapshotS3ModeProgress {
		return
	}

	flagSet.IntVar(&f.RequestTimeout, flagS3RequestTimeout,
		models.DefaultCloudRequestTimeout,
		"Timeout (in ms) specifies a time limit for requests made by this Client.\n"+
			"The timeout includes connection time, any redirects, and reading the response body.\n"+
			"0 means no limit."+descSnapshotS3Local)
}

// GetSnapshotS3 returns the S3 settings the flags were parsed into.
func (f *SnapshotS3) GetSnapshotS3() *models.SnapshotS3 {
	return &f.SnapshotS3
}
