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
	"context"
	"fmt"

	"github.com/aerospike/absctl/internal/models"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

type App struct {
	models.App
}

func NewApp() *App {
	return &App{}
}

func (f *App) NewFlagSet() *pflag.FlagSet {
	flagSet := &pflag.FlagSet{}

	flagSet.BoolP("help", "Z", models.DefaultAppHelp, "Display help information.")
	flagSet.BoolVarP(&f.Verbose, "verbose", "v",
		models.DefaultAppVerbose,
		"Enable more detailed logging.")
	flagSet.StringVar(&f.LogLevel, "log-level",
		models.DefaultAppLogLevel,
		"Determine log level for --verbose output. Log levels are: debug, info, warn, error.")
	flagSet.BoolVar(&f.LogJSON, "log-json",
		models.DefaultAppLogJSON,
		"Set output in JSON format for parsing by external tools.")
	flagSet.StringVar(&f.LogFile, "log-file",
		models.DefaultAppLogFile,
		"Path to log file. If empty logs will be printed to stderr.")
	flagSet.StringVar(&f.ConfigFilePath, "config",
		models.DefaultAppConfigFilePath,
		"Path to YAML configuration file.")

	return flagSet
}

// GetApp returns the App struct.
func (f *App) GetApp() *models.App {
	return &f.App
}

// PreRun contains logic that is executed right after flag parsing.
// Is used in backup/restore to preload secrets from SecretAgent for external libs.
func (f *App) PreRun(cmd *cobra.Command, sa *models.SecretAgent) error {
	flagsToPreload := []string{
		"host", "port", "user",
		"password", "tls-name", "tls-cafile",
		"tls-capath", "tls-certfile", "tls-keyfile",
		"tls-keyfile-password", "tls-protocols",
	}

	fs := cmd.Flags()

	for _, flag := range flagsToPreload {
		if err := parseValueWithSecretAgent(cmd.Context(), fs, sa, flag); err != nil {
			return err
		}
	}

	return nil
}

func parseValueWithSecretAgent(ctx context.Context, fs *pflag.FlagSet, sa *models.SecretAgent, name string) error {
	flag := fs.Lookup(name)

	val, err := sa.GetSecret(ctx, flag.Value.String())
	if err != nil {
		return fmt.Errorf("failed to get secret for %s: %w", name, err)
	}

	if err := flag.Value.Set(val); err != nil {
		return fmt.Errorf("failed to set secret value of %s: %w", name, err)
	}

	return nil
}
