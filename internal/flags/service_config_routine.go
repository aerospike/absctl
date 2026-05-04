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

// ServiceConfigRoutine holds flags for the `service config routines add|update`
// commands. Each leaf field of DtoBackupRoutine is exposed as an individual
// flag;
type ServiceConfigRoutine struct {
	models.ConfigRoutineFields
}

func NewServiceConfigRoutine() *ServiceConfigRoutine {
	return &ServiceConfigRoutine{}
}

func (f *ServiceConfigRoutine) NewFlagSet() *pflag.FlagSet {
	flagSet := &pflag.FlagSet{}

	flagSet.StringVar(&f.Name, "name", "",
		"Routine name (used as the URL path parameter).")

	flagSet.StringVar(&f.BackupPolicy, "backup-policy", "",
		"Name of the backup policy to use.")

	flagSet.StringVar(&f.SourceCluster, "source-cluster", "",
		"Name of the source cluster (required for new routines).")

	flagSet.StringVar(&f.Storage, "storage", "",
		"Name of the storage provider configuration (required for new routines).")

	flagSet.StringVar(&f.SecretAgent, "secret-agent", "",
		"Name of a Secret Agent to read secrets from.")

	flagSet.StringVar(&f.IntervalCron, "interval-cron", "",
		"Cron expression for the full backup schedule (required for new routines).")

	flagSet.StringVar(&f.IncrIntervalCron, "incr-interval-cron", "",
		"Cron expression for the incremental backup schedule.")

	flagSet.StringVar(&f.PartitionList, "partition-list", "",
		"Comma-separated list of partitions or ranges (e.g. 0,100,200-50).")

	flagSet.BoolVar(&f.Disabled, "disabled", false,
		"Disable the routine (do not run it).")

	flagSet.StringSliceVar(&f.Namespaces, "namespaces", nil,
		"Comma-separated list of namespaces to back up.")

	flagSet.StringSliceVar(&f.BinList, "bin-list", nil,
		"Comma-separated list of bin names to back up.")

	flagSet.StringSliceVar(&f.SetList, "set-list", nil,
		"Comma-separated list of set names to back up.")

	flagSet.StringSliceVar(&f.NodeList, "node-list", nil,
		"Comma-separated list of Aerospike nodes to back up from.")

	flagSet.IntSliceVar(&f.RackList, "rack-list", nil,
		"Comma-separated list of Aerospike rack IDs to back up from.")

	return flagSet
}
