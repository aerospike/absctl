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

// ServiceBackupRoutine holds the --name flag used by cancel and status commands.
type ServiceBackupRoutine struct {
	models.BackupRoutine
}

func NewServiceBackupRoutine() *ServiceBackupRoutine {
	return &ServiceBackupRoutine{}
}

func (f *ServiceBackupRoutine) NewFlagSet() *pflag.FlagSet {
	flagSet := &pflag.FlagSet{}

	flagSet.StringVar(&f.Name, "name", "",
		"Backup routine name.")

	return flagSet
}

// ServiceBackupList holds flags for the full/incremental list commands.
type ServiceBackupList struct {
	models.BackupListFilter
}

func NewServiceBackupList() *ServiceBackupList {
	return &ServiceBackupList{}
}

func (f *ServiceBackupList) NewFlagSet() *pflag.FlagSet {
	flagSet := &pflag.FlagSet{}

	flagSet.StringVar(&f.Name, "name", "",
		"Backup routine name. If omitted, backups for all routines are returned.")

	flagSet.Int64Var(&f.From, "from", 0,
		"Lower bound timestamp filter (Unix milliseconds). 0 means no lower bound.")

	flagSet.Int64Var(&f.To, "to", 0,
		"Upper bound timestamp filter (Unix milliseconds). 0 means no upper bound.")

	return flagSet
}

// ServiceBackupTrigger holds flags for the full/incremental start commands.
type ServiceBackupTrigger struct {
	models.BackupTrigger
}

func NewServiceBackupTrigger() *ServiceBackupTrigger {
	return &ServiceBackupTrigger{}
}

func (f *ServiceBackupTrigger) NewFlagSet() *pflag.FlagSet {
	flagSet := &pflag.FlagSet{}

	flagSet.StringVar(&f.Name, "name", "",
		"Backup routine name.")

	flagSet.IntVar(&f.Delay, "delay", 0,
		"Delay interval in milliseconds before the backup starts. 0 means no delay.")

	return flagSet
}
