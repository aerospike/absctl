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

package subcmd

import (
	"context"
	"fmt"
	"log"
	"log/slog"

	"github.com/aerospike/absctl/internal/flags"
	"github.com/aerospike/absctl/internal/logging"
	"github.com/aerospike/absctl/internal/models"
	asFlags "github.com/aerospike/tools-common-go/flags"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// ServiceConfig must be implemented by every subcommand's config type
// so the builder can extract App settings for logger initialization.
type ServiceConfig interface {
	GetApp() *models.App
	Validate() error
}

// SharedFlags holds all flag objects shared across subcommands.
type SharedFlags struct {
	Root         *flags.Root
	App          *flags.App
	Aerospike    *asFlags.AerospikeFlags
	ClientPolicy *flags.ClientPolicy
	Compression  *flags.Compression
	Encryption   *flags.Encryption
	SecretAgent  *flags.SecretAgent
	Aws          *flags.AwsS3
	Gcp          *flags.GcpStorage
	Azure        *flags.AzureBlob
}

// SharedFlagSets holds pflag.FlagSet objects created from SharedFlags.
// Subcommands use these to build their custom help functions.
type SharedFlagSets struct {
	App          *pflag.FlagSet
	Aerospike    *pflag.FlagSet
	ClientPolicy *pflag.FlagSet
	Compression  *pflag.FlagSet
	Encryption   *pflag.FlagSet
	SecretAgent  *pflag.FlagSet
	Aws          *pflag.FlagSet
	Gcp          *pflag.FlagSet
	Azure        *pflag.FlagSet
}

// Runner defines the contract each subcommand must implement.
type Runner interface {
	// FlagSets returns flag sets specific to this subcommand.
	// They are registered as regular Flags (not PersistentFlags).
	FlagSets() []*pflag.FlagSet

	// PostRegistration is called after all flags are registered on the cobra command.
	PostRegistration(cmd *cobra.Command)

	// SetHelpUsage sets custom help and usage functions on the command.
	SetHelpUsage(cmd *cobra.Command, shared SharedFlagSets)

	// NewServiceConfig creates the service configuration from parsed flags.
	NewServiceConfig(ctx context.Context, shared *SharedFlags) (ServiceConfig, error)

	// RunService creates and runs the domain service.
	RunService(ctx context.Context, cfg ServiceConfig, logger *slog.Logger) error
}

// BuildCommand constructs a cobra.Command with shared flag registration and
// a standard run lifecycle: version check -> logger init -> service execution.
func BuildCommand(
	name, short, long string,
	flagsRoot *flags.Root,
	appVersion, commitHash, buildTime string,
	op int,
	runner Runner,
) *cobra.Command {
	shared := &SharedFlags{
		Root:         flagsRoot,
		App:          flags.NewApp(),
		Aerospike:    asFlags.NewDefaultAerospikeFlags(),
		ClientPolicy: flags.NewClientPolicy(),
		Compression:  flags.NewCompression(op),
		Encryption:   flags.NewEncryption(op),
		SecretAgent:  flags.NewSecretAgent(),
		Aws:          flags.NewAwsS3(op),
		Gcp:          flags.NewGcpStorage(op),
		Azure:        flags.NewAzureBlob(op),
	}

	cmd := &cobra.Command{
		Use:   name,
		Short: short,
		Long:  long,
	}

	cmd.PersistentFlags().SortFlags = false
	cmd.SilenceUsage = true

	sharedSets := newSharedFlagSets(shared)

	// Register shared flags as PersistentFlags.
	cmd.PersistentFlags().AddFlagSet(sharedSets.App)
	cmd.PersistentFlags().AddFlagSet(sharedSets.Aerospike)
	cmd.PersistentFlags().AddFlagSet(sharedSets.ClientPolicy)
	cmd.PersistentFlags().AddFlagSet(sharedSets.Compression)
	cmd.PersistentFlags().AddFlagSet(sharedSets.Encryption)
	cmd.PersistentFlags().AddFlagSet(sharedSets.SecretAgent)
	cmd.PersistentFlags().AddFlagSet(sharedSets.Aws)
	cmd.PersistentFlags().AddFlagSet(sharedSets.Gcp)
	cmd.PersistentFlags().AddFlagSet(sharedSets.Azure)

	// Register subcommand-specific flag sets.
	for _, fs := range runner.FlagSets() {
		cmd.Flags().AddFlagSet(fs)
	}

	runner.PostRegistration(cmd)
	runner.SetHelpUsage(cmd, sharedSets)

	cmd.RunE = func(cmd *cobra.Command, _ []string) error {
		return runCommand(cmd, runner, shared, flagsRoot, appVersion, commitHash, buildTime)
	}

	cmd.PersistentPreRunE = func(cmd *cobra.Command, _ []string) error {
		sa := shared.SecretAgent.GetSecretAgent()
		return shared.App.PreRun(cmd, sa)
	}

	return cmd
}

func newSharedFlagSets(shared *SharedFlags) SharedFlagSets {
	aerospikeFlagSet := shared.Aerospike.NewFlagSet(asFlags.DefaultWrapHelpString)
	flags.WrapCertFlagsForSecrets(aerospikeFlagSet)

	return SharedFlagSets{
		App:          shared.App.NewFlagSet(),
		Aerospike:    aerospikeFlagSet,
		ClientPolicy: shared.ClientPolicy.NewFlagSet(),
		Compression:  shared.Compression.NewFlagSet(),
		Encryption:   shared.Encryption.NewFlagSet(),
		SecretAgent:  shared.SecretAgent.NewFlagSet(),
		Aws:          shared.Aws.NewFlagSet(),
		Gcp:          shared.Gcp.NewFlagSet(),
		Azure:        shared.Azure.NewFlagSet(),
	}
}

func runCommand(
	cmd *cobra.Command,
	runner Runner,
	shared *SharedFlags,
	flagsRoot *flags.Root,
	appVersion, commitHash, buildTime string,
) error {
	if flagsRoot.Version {
		fmt.Printf("version: %s (%s) %s \n", appVersion, commitHash, buildTime)
		return nil
	}

	if cmd.Flags().NFlag() == 0 {
		if err := cmd.Help(); err != nil {
			return fmt.Errorf("failed to load help: %w", err)
		}

		return nil
	}

	cfg, err := runner.NewServiceConfig(cmd.Context(), shared)
	if err != nil {
		return fmt.Errorf("failed to initialize app: %w", err)
	}

	if err = cfg.Validate(); err != nil {
		return fmt.Errorf("failed to validate config: %w", err)
	}

	app := cfg.GetApp()
	loggerConf := logging.NewConfig(app.Verbose, app.LogJSON, app.LogLevel, app.LogFile)

	logger, loggerClose, err := logging.NewLogger(loggerConf)
	if err != nil {
		return fmt.Errorf("failed to initialize logger: %w", err)
	}

	defer func() {
		if err := loggerClose(); err != nil {
			log.Printf("failed to close logger: %v", err)
		}
	}()

	return runner.RunService(cmd.Context(), cfg, logger)
}
