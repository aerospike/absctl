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

package backup

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"strings"

	"github.com/aerospike/absctl/internal/backup"
	"github.com/aerospike/absctl/internal/config"
	"github.com/aerospike/absctl/internal/flags"
	"github.com/aerospike/absctl/internal/subcmd"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

const (
	welcomeMessage      = "Welcome to the Aerospike backup CLI tool!"
	welcomeMessageShort = "Aerospike backup CLI tool"
)

type runner struct {
	flagsBackup *flags.Backup
	flagsCommon *flags.Common
	flagsLocal  *flags.Local

	commonFlagSet *pflag.FlagSet
	backupFlagSet *pflag.FlagSet
	localFlagSet  *pflag.FlagSet
}

func NewCmd(flagsRoot *flags.Root, appVersion, commitHash, buildTime string) (*cobra.Command, *subcmd.SharedFlags) {
	r := &runner{
		flagsBackup: flags.NewBackup(),
		flagsLocal:  flags.NewLocal(flags.OperationBackup),
	}
	r.flagsCommon = flags.NewCommon(&r.flagsBackup.Common, flags.OperationBackup)

	return subcmd.BuildCommand(
		"backup", welcomeMessageShort, welcomeMessage,
		flagsRoot, appVersion, commitHash, buildTime,
		flags.OperationBackup, r,
	)
}

func (r *runner) FlagSets() []*pflag.FlagSet {
	r.commonFlagSet = r.flagsCommon.NewFlagSet()
	r.backupFlagSet = r.flagsBackup.NewFlagSet()
	r.localFlagSet = r.flagsLocal.NewFlagSet()

	return []*pflag.FlagSet{
		r.commonFlagSet,
		r.backupFlagSet,
		r.localFlagSet,
	}
}

func (r *runner) PostRegistration(cmd *cobra.Command) {
	if err := cmd.Flags().MarkDeprecated("nice", "use --bandwidth instead"); err != nil {
		log.Fatal(err)
	}

	cmd.Flags().Lookup("nice").Hidden = false
}

func (r *runner) SetHelpUsage(cmd *cobra.Command, shared subcmd.SharedFlagSets) {
	helpFunc := newHelpFunction(
		shared.App,
		shared.Aerospike,
		shared.ClientPolicy,
		r.commonFlagSet,
		r.backupFlagSet,
		shared.Compression,
		shared.Encryption,
		shared.SecretAgent,
		shared.Aws,
		shared.Gcp,
		shared.Azure,
		r.localFlagSet,
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
	app := shared.App.GetApp()
	if app != nil && app.ConfigFilePath != "" {
		cfg, err := config.DecodeBackupServiceConfig(ctx, app.ConfigFilePath)
		if err != nil {
			return nil, fmt.Errorf("failed to load config file %s: %w", app.ConfigFilePath, err)
		}

		return cfg, nil
	}

	cfg, err := config.NewBackupServiceConfig(
		shared.App.GetApp(),
		shared.Aerospike.NewAerospikeConfig(),
		shared.ClientPolicy.GetClientPolicy(),
		r.flagsBackup.GetBackup(),
		nil,
		shared.Compression.GetCompression(),
		shared.Encryption.GetEncryption(),
		shared.SecretAgent.GetSecretAgent(),
		shared.Aws.GetAwsS3(),
		shared.Gcp.GetGcpStorage(),
		shared.Azure.GetAzureBlob(),
		r.flagsLocal.GetLocal(),
	)
	if err != nil {
		return nil, err
	}

	return cfg, nil
}

func (r *runner) RunService(ctx context.Context, cfg subcmd.ServiceConfig, logger *slog.Logger) error {
	backupCfg := cfg.(*config.BackupServiceConfig)

	asb, err := backup.NewService(ctx, backupCfg, logger)
	if err != nil {
		return fmt.Errorf("backup initialization failed: %w", err)
	}

	if err = asb.Run(ctx); err != nil {
		return fmt.Errorf("backup failed: %w", err)
	}

	return nil
}

func newHelpFunction(
	appFlagSet,
	aerospikeFlagSet,
	clientPolicyFlagSet,
	commonFlagSet,
	backupFlagSet,
	compressionFlagSet,
	encryptionFlagSet,
	secretAgentFlagSet,
	awsFlagSet,
	gcpFlagSet,
	azureFlagSet,
	localFlagSet *pflag.FlagSet,
) func() {
	return func() {
		fmt.Println(welcomeMessage)
		fmt.Println(strings.Repeat("-", len(welcomeMessage)))
		fmt.Println(flags.SectionTextUsageBackup)

		// Print section: App Flags
		fmt.Println(flags.SectionTextGeneral)
		appFlagSet.PrintDefaults()

		// Print section: Common Flags
		fmt.Println(flags.SectionTextAerospike)
		aerospikeFlagSet.PrintDefaults()
		clientPolicyFlagSet.PrintDefaults()

		// Print section: Backup Flags
		fmt.Println(flags.SectionTextBackup)
		commonFlagSet.PrintDefaults()
		backupFlagSet.PrintDefaults()

		// Print section: Compression Flags
		fmt.Println(flags.SectionTextCompression)
		compressionFlagSet.PrintDefaults()

		// Print section: Encryption Flags
		fmt.Println(flags.SectionTextEncryption)
		encryptionFlagSet.PrintDefaults()

		// Print section: Secret Agent Flags
		fmt.Println(flags.SectionTextSecretAgentBackup)
		secretAgentFlagSet.PrintDefaults()

		// Print section: Local Flags
		fmt.Println(flags.SectionTextLocal)
		localFlagSet.PrintDefaults()

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
