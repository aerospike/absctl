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

// ServerRestore holds flags for the server restore start command.
type ServerRestore struct {
	models.ServerRestore
}

func NewServerRestore() *ServerRestore {
	return &ServerRestore{}
}

func (f *ServerRestore) NewFlagSet() *pflag.FlagSet {
	flagSet := &pflag.FlagSet{}

	flagSet.StringVar(&f.Namespace, "namespace", "", "The namespace to restore.")
	flagSet.StringVar(&f.StorageType, "object-storage-type", "", "Type of object storage. "+
		"Example: aws-s3")
	flagSet.StringVar(&f.JobID, "backup-id", "", "Job id used for restore.")
	flagSet.StringVar(&f.Path, "path", "", "Path to restore from.")
	flagSet.BoolVar(&f.FuzzyRestore, "fuzzy-restore", false, "Fuzzy restore.")

	return flagSet
}

func (f *ServerRestore) GetServerRestore() *models.ServerRestore {
	return &f.ServerRestore
}

// ServerRestorePrepare holds flags for the server restore prepare command.
type ServerRestorePrepare struct {
	models.ServerRestorePrepare
}

func NewServerRestorePrepare() *ServerRestorePrepare {
	return &ServerRestorePrepare{}
}

func (f *ServerRestorePrepare) NewFlagSet() *pflag.FlagSet {
	flagSet := &pflag.FlagSet{}

	flagSet.StringVar(&f.Namespace, "namespace", "", "The namespace to restore.")
	flagSet.StringVar(&f.JobID, "backup-id", "", "Job id used for restore.")

	return flagSet
}

func (f *ServerRestorePrepare) GetServerRestorePrepare() *models.ServerRestorePrepare {
	return &f.ServerRestorePrepare
}

type ServerRestoreProgress struct {
	models.ServerRestoreProgress
}

func NewServerRestoreProgress() *ServerRestoreProgress {
	return &ServerRestoreProgress{}
}

func (f *ServerRestoreProgress) NewFlagSet() *pflag.FlagSet {
	flagSet := &pflag.FlagSet{}

	flagSet.StringVar(&f.Namespace, "namespace", "", "The namespace to check progress.")

	return flagSet
}

func (f *ServerRestoreProgress) GetServerRestoreProgress() *models.ServerRestoreProgress {
	return &f.ServerRestoreProgress
}
