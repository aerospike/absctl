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

// ServiceRestoreJobID holds the --job-id flag used by cancel and status commands.
type ServiceRestoreJobID struct {
	models.RestoreJobID
}

func NewServiceRestoreJobID() *ServiceRestoreJobID {
	return &ServiceRestoreJobID{}
}

func (f *ServiceRestoreJobID) NewFlagSet() *pflag.FlagSet {
	flagSet := &pflag.FlagSet{}

	flagSet.Int64Var(&f.JobID, "job-id", 0,
		"Restore job ID.")

	return flagSet
}

// ServiceRestoreJobs holds flags for the jobs (list) command.
type ServiceRestoreJobs struct {
	models.RestoreJobsFilter
}

func NewServiceRestoreJobs() *ServiceRestoreJobs {
	return &ServiceRestoreJobs{}
}

func (f *ServiceRestoreJobs) NewFlagSet() *pflag.FlagSet {
	flagSet := &pflag.FlagSet{}

	flagSet.Int64Var(&f.From, "from", 0,
		"Lower bound timestamp filter (Unix milliseconds). 0 means no lower bound.")

	flagSet.Int64Var(&f.To, "to", 0,
		"Upper bound timestamp filter (Unix milliseconds). 0 means no upper bound.")

	flagSet.StringVar(&f.Status, "status", "",
		"Comma-separated status filter (Running,Done,Failed,Canceled). "+
			"Use ! prefix for exclude filter (e.g. !Failed,Canceled).")

	return flagSet
}

// ServiceRestoreRequest holds flags for the full and incremental restore commands.
type ServiceRestoreRequest struct {
	models.RestoreRequest
}

func NewServiceRestoreRequest() *ServiceRestoreRequest {
	return &ServiceRestoreRequest{}
}

func (f *ServiceRestoreRequest) NewFlagSet() *pflag.FlagSet {
	flagSet := &pflag.FlagSet{}

	flagSet.StringVar(&f.RequestFile, "request-file", "",
		"Path to a JSON file containing the full restore request body. "+
			"Other flags override fields loaded from this file.")

	flagSet.StringVar(&f.BackupDataPath, "backup-data-path", "",
		"Path to the backup data inside the storage root. "+
			"Required when --request-file is not provided.")

	flagSet.StringVar(&f.DestinationName, "destination-name", "",
		"Name of a preconfigured destination Aerospike cluster.")

	flagSet.StringVar(&f.SourceName, "source-name", "",
		"Name of a preconfigured storage source.")

	flagSet.StringVar(&f.SecretAgentName, "secret-agent-name", "",
		"Name of a preconfigured secret agent.")

	return flagSet
}

// ServiceRestoreTimestamp holds flags for the timestamp restore command.
type ServiceRestoreTimestamp struct {
	models.RestoreTimestampRequest
}

func NewServiceRestoreTimestamp() *ServiceRestoreTimestamp {
	return &ServiceRestoreTimestamp{}
}

func (f *ServiceRestoreTimestamp) NewFlagSet() *pflag.FlagSet {
	flagSet := &pflag.FlagSet{}

	flagSet.StringVar(&f.RequestFile, "request-file", "",
		"Path to a JSON file containing the full restore-by-timestamp request body. "+
			"Other flags override fields loaded from this file.")

	flagSet.StringVar(&f.Routine, "routine", "",
		"Backup routine name. Required when --request-file is not provided.")

	flagSet.Int64Var(&f.Time, "time", 0,
		"Epoch time in milliseconds for point-in-time recovery. "+
			"Required when --request-file is not provided.")

	flagSet.StringVar(&f.DestinationName, "destination-name", "",
		"Name of a preconfigured destination Aerospike cluster.")

	flagSet.StringVar(&f.SecretAgentName, "secret-agent-name", "",
		"Name of a preconfigured secret agent.")

	flagSet.BoolVar(&f.DisableReordering, "disable-reordering", false,
		"Disable reverse-order incremental backups optimisation.")

	return flagSet
}
