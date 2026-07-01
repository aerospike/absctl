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

// ServerBackup holds flags for the server backup create command.
type ServerBackup struct {
	models.ServerBackup
}

func NewServerBackup() *ServerBackup {
	return &ServerBackup{}
}

func (f *ServerBackup) NewFlagSet() *pflag.FlagSet {
	flagSet := &pflag.FlagSet{}

	flagSet.StringVar(&f.Namespace, "namespace", "", "The namespace to be backed up.")
	flagSet.StringVar(&f.StorageType, "object-storage-type", "", "Type of object storage. "+
		"Example: aws-s3")
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
	flagSet.StringVarP(&f.SetList, "set-list", "s",
		models.DefaultCommonSetList,
		descSetListBackup)
	flagSet.BoolVarP(&f.NoIndexes, "no-indexes", "i",
		models.DefaultCommonNoIndexes,
		"Exclude indexes from the backup.")
	flagSet.BoolVarP(&f.NoUDFs, "no-udfs", "u",
		models.DefaultCommonNoUDFs,
		"Exclude user-defined functions from the backup.")

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

	flagSet.StringVar(&f.ListPath, "path", "/", "Path to list backups from.")

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

	flagSet.IntVar(&f.SampleSize, "sample-size", 10000, "Number of segments for random validation.")
	flagSet.StringVar(&f.JobID, "backup-id", "", "Backup id used for validation.")

	return flagSet
}

func (f *ServerBackupValidate) GetServerBackupValidate() *models.ServerBackupValidate {
	return &f.ServerBackupValidate
}
