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
	"context"
	"fmt"
	"strings"

	"github.com/aerospike/absctl/internal/models"
	"github.com/aerospike/backup-go"
	"github.com/aerospike/tools-common-go/secretagent"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// Flag names referenced outside the flag set that registers them.
const (
	// FlagConfig is the name of the flag that sets the YAML configuration file.
	FlagConfig = "config"
	// FlagVersion is the name of the flag that prints the application version.
	FlagVersion = "version"
	// FlagHelp is the name of the flag that prints help.
	FlagHelp = "help"
)

// ConfigCompatibleFlags returns the names of the flags that may be combined with
// --config. The configuration file is the single source of truth, so any other flag
// would be silently overridden by it and is rejected instead.
func ConfigCompatibleFlags() []string {
	return []string{FlagConfig, FlagVersion, FlagHelp}
}

// TextConfigExclusive returns the rule for combining --config with other flags, as
// plain text for help output.
func TextConfigExclusive() string {
	return FormatConfigExclusive(func(name string) string { return "--" + name })
}

// FormatConfigExclusive returns the rule for combining --config with other flags, with
// every flag name rendered by flagName, for example as a markdown code span.
func FormatConfigExclusive(flagName func(name string) string) string {
	allowed := make([]string, 0, len(ConfigCompatibleFlags()))

	for _, name := range ConfigCompatibleFlags() {
		if name != FlagConfig {
			allowed = append(allowed, flagName(name))
		}
	}

	return "The file is the single source of truth: no other flags are allowed with " + flagName(FlagConfig) +
		", except " + strings.Join(allowed, ", ") + "."
}

type App struct {
	models.App
}

func NewApp() *App {
	return &App{}
}

func (f *App) NewFlagSet() *pflag.FlagSet {
	flagSet := &pflag.FlagSet{}

	flagSet.BoolP(FlagHelp, "Z", models.DefaultAppHelp, "Display help information.")
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
		"Path to log file. If empty, logs will be printed to stderr.")
	flagSet.StringVar(&f.ConfigFilePath, FlagConfig,
		models.DefaultAppConfigFilePath,
		"Path to YAML configuration file. "+TextConfigExclusive())

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
		// Aerospike connection flags.
		flagHost, flagPort, flagUser,
		flagPassword, flagTLSName, flagTLSCaFile,
		flagTLSCapath, flagTLSCertFile, flagTLSKeyFile,
		flagTLSKeyFilePassword, flagTLSProtocols,
		// Encryption flags.
		flagEncryptKeyFile, flagEncryptKeyEnv,
		// AWS Flags
		flagS3BucketName, flagS3Region, flagS3Profile,
		flagS3Endpoint, flagS3AccessKeyID, flagS3SecretAccessKey,
		flagS3StorageClass, flagS3AccessTier,
		// Azure Flags
		flagAzureAccountName, flagAzureAccountKey, flagAzureTenantID,
		flagAzureClientID, flagAzureClientSecret, flagAzureEndpoint,
		flagAzureContainerName, flagAzureAccessTier,
		// GCP Flags
		flagGcpKeyPath, flagGcpBucketName, flagGcpEndpoint,
	}

	fs := cmd.Flags()
	// Preload secret agent config, not to load it every time.
	saCfg := sa.Config()

	for _, flag := range flagsToPreload {
		if err := parseValueWithSecretAgent(cmd.Context(), fs, saCfg, flag); err != nil {
			return err
		}
	}

	return nil
}

func parseValueWithSecretAgent(ctx context.Context, fs *pflag.FlagSet, saCfg *backup.SecretAgentConfig, name string,
) error {
	flag := fs.Lookup(name)
	// Skip unset flags.
	if flag == nil {
		return nil
	}
	// clean.
	curVal := strings.TrimSpace(flag.Value.String())
	if curVal == "" {
		return nil
	}
	// Check if we need to parse secret.
	if !secretagent.IsSecret(curVal) {
		return nil
	}

	val, err := backup.ParseSecret(ctx, saCfg, curVal)
	if err != nil {
		return fmt.Errorf("failed to get secret for %s: %w", name, err)
	}

	if err = flag.Value.Set(val); err != nil {
		return fmt.Errorf("failed to set secret value of %s: %w", name, err)
	}

	return nil
}
