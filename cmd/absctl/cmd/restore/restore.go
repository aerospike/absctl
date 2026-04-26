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

package restore

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"strings"

	"github.com/aerospike/absctl/internal/config"
	"github.com/aerospike/absctl/internal/flags"
	"github.com/aerospike/absctl/internal/models"
	"github.com/aerospike/absctl/internal/restore"
	"github.com/aerospike/absctl/internal/subcmd"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

const (
	welcomeMessage      = "Welcome to the Aerospike restore CLI tool!"
	welcomeMessageShort = "Aerospike restore CLI tool"
)

type runner struct {
	flagsRestore *flags.Restore
	flagsCommon  *flags.Common

	commonFlagSet  *pflag.FlagSet
	restoreFlagSet *pflag.FlagSet
}

func NewCmd(flagsRoot *flags.Root, appVersion, commitHash, buildTime string) *cobra.Command {
	r := &runner{
		flagsRestore: flags.NewRestore(),
	}
	r.flagsCommon = flags.NewCommon(&r.flagsRestore.Common, flags.OperationRestore)

	return subcmd.BuildCommand(
		"restore", welcomeMessageShort, welcomeMessage,
		flagsRoot, appVersion, commitHash, buildTime,
		flags.OperationRestore, r,
	)
}

func (r *runner) FlagSets() []*pflag.FlagSet {
	r.commonFlagSet = r.flagsCommon.NewFlagSet()
	r.restoreFlagSet = r.flagsRestore.NewFlagSet()

	return []*pflag.FlagSet{
		r.commonFlagSet,
		r.restoreFlagSet,
	}
}

func (r *runner) PostRegistration(cmd *cobra.Command) {
	if err := cmd.Flags().MarkDeprecated("nice", "use --bandwidth instead"); err != nil {
		log.Fatal(err)
	}

	cmd.Flags().Lookup("nice").Hidden = false
}

func (r *runner) SetHelpUsage(cmd *cobra.Command, shared *subcmd.SharedFlagSets) {
	helpFunc := newHelpFunction(
		shared.App,
		shared.Aerospike,
		shared.ClientPolicy,
		r.commonFlagSet,
		r.restoreFlagSet,
		shared.Compression,
		shared.Encryption,
		shared.SecretAgent,
		shared.Aws,
		shared.Gcp,
		shared.Azure,
	)

	cmd.SetUsageFunc(func(_ *cobra.Command) error {
		helpFunc()
		return nil
	})
	cmd.SetHelpFunc(func(_ *cobra.Command, _ []string) {
		helpFunc()
	})
}

func (r *runner) NewServiceConfig(ctx context.Context, shared *subcmd.SharedFlags) (subcmd.ServiceConfig, error) {
	var (
		cfg *config.RestoreServiceConfig
		err error
	)

	app := shared.App.GetApp()
	if app != nil && app.ConfigFilePath != "" {
		cfg, err = config.DecodeRestoreServiceConfig(ctx, app.ConfigFilePath)
		if err != nil {
			return nil, fmt.Errorf("failed to load config file %s: %w", app.ConfigFilePath, err)
		}
	} else {
		cfg, err = config.NewRestoreServiceConfig(
			shared.App.GetApp(),
			shared.Aerospike.NewAerospikeConfig(),
			shared.ClientPolicy.GetClientPolicy(),
			r.flagsRestore.GetRestore(),
			shared.Compression.GetCompression(),
			shared.Encryption.GetEncryption(),
			shared.SecretAgent.GetSecretAgent(),
			shared.Aws.GetAwsS3(),
			shared.Gcp.GetGcpStorage(),
			shared.Azure.GetAzureBlob(),
		)
		if err != nil {
			return nil, err
		}
	}

	// Set default restore mode to asb.
	// This should be removed once asbx is released.
	cfg.Restore.Mode = models.RestoreModeASB

	return cfg, nil
}

func (r *runner) RunService(ctx context.Context, cfg subcmd.ServiceConfig, logger *slog.Logger) error {
	restoreCfg := cfg.(*config.RestoreServiceConfig)

	logMsg := "restore"
	if restoreCfg.Restore.ValidateOnly {
		logMsg = "validation"
	}

	asr, err := restore.NewService(ctx, restoreCfg, logger)
	if err != nil {
		return fmt.Errorf("%s initialization failed: %w", logMsg, err)
	}

	if err = asr.Run(ctx); err != nil {
		return fmt.Errorf("%s failed: %w", logMsg, err)
	}

	return nil
}

func newHelpFunction(
	appFlagSet,
	aerospikeFlagSet,
	clientPolicyFlagSet,
	commonFlagSet,
	restoreFlagSet,
	compressionFlagSet,
	encryptionFlagSet,
	secretAgentFlagSet,
	awsFlagSet,
	gcpFlagSet,
	azureFlagSet *pflag.FlagSet,
) func() {
	return func() {
		fmt.Println(welcomeMessage)
		fmt.Println(strings.Repeat("-", len(welcomeMessage)))
		fmt.Println(flags.SectionTextUsageRestore)

		// Print section: App Flags
		fmt.Println(flags.SectionTextGeneral)
		appFlagSet.PrintDefaults()

		// Print section: Common Flags
		fmt.Println(flags.SectionTextAerospike)
		aerospikeFlagSet.PrintDefaults()
		clientPolicyFlagSet.PrintDefaults()

		// Print section: Restore Flags
		fmt.Println(flags.SectionTextRestore)
		commonFlagSet.PrintDefaults()
		restoreFlagSet.PrintDefaults()

		// Print section: Compression Flags
		fmt.Println(flags.SectionTextCompression)
		compressionFlagSet.PrintDefaults()

		// Print section: Encryption Flags
		fmt.Println(flags.SectionTextEncryption)
		encryptionFlagSet.PrintDefaults()

		// Print section: Secret Agent Flags
		fmt.Println(flags.SectionTextSecretAgentRestore)
		secretAgentFlagSet.PrintDefaults()

		// Print section: AWS Flags
		fmt.Println(flags.SectionTextAWS)
		awsFlagSet.PrintDefaults()

		// Print section: GCP Flags
		fmt.Println(flags.SectionTextGCP)
		gcpFlagSet.PrintDefaults()

		// Print section: Azure Flags
		fmt.Println(flags.SectionTextAzure)
		azureFlagSet.PrintDefaults()
	}
}
