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

type ServerSideBackup struct {
	models.IntegratedBackup
}

func NewServerSideBackup() *ServerSideBackup {
	return &ServerSideBackup{}
}

func (f *ServerSideBackup) NewBackupCreateFlagSet() *pflag.FlagSet {
	flagSet := &pflag.FlagSet{}

	flagSet.StringVar(&f.Namespace, "namespace", "", "The namespace to be backed up.")
	flagSet.StringVar(&f.StorageType, "object-storage-type", "", "Type of object storage. "+
		"Example: aws-s3")
	flagSet.Int64Var(&f.JobID, "job-id", 0, "Job id used for restore.")

	return flagSet
}

func (f *ServerSideBackup) NewBackupListFlagSet() *pflag.FlagSet {
	flagSet := &pflag.FlagSet{}

	flagSet.StringVar(&f.ListPath, "path", "", "Path to list backups from.")

	return flagSet
}

func (f *ServerSideBackup) NewRestoreStartFlagSet() *pflag.FlagSet {
	flagSet := &pflag.FlagSet{}

	flagSet.StringVar(&f.Namespace, "namespace", "", "The namespace to restore.")
	flagSet.StringVar(&f.StorageType, "object-storage-type", "", "Type of object storage. "+
		"Example: aws-s3")
	flagSet.Int64Var(&f.JobID, "job-id", 0, "Job id used for restore.")

	return flagSet
}

func (f *ServerSideBackup) GetServerSideBackup() *models.IntegratedBackup {
	return &f.IntegratedBackup
}
