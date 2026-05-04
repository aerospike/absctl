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

// ServiceConfigName holds the --name flag for show/delete/enable/disable commands.
type ServiceConfigName struct {
	models.ConfigResourceName
}

func NewServiceConfigName() *ServiceConfigName {
	return &ServiceConfigName{}
}

func (f *ServiceConfigName) NewFlagSet() *pflag.FlagSet {
	flagSet := &pflag.FlagSet{}

	flagSet.StringVar(&f.Name, "name", "",
		"Name of the configuration resource.")

	return flagSet
}

// ServiceConfigFile holds the --file flag for replacing the full service configuration.
type ServiceConfigFile struct {
	models.ConfigUpdate
}

func NewServiceConfigFile() *ServiceConfigFile {
	return &ServiceConfigFile{}
}

func (f *ServiceConfigFile) NewFlagSet() *pflag.FlagSet {
	flagSet := &pflag.FlagSet{}

	flagSet.StringVar(&f.File, "file", "",
		"Path to a YAML file with the full service configuration body (DtoConfig).")

	return flagSet
}
