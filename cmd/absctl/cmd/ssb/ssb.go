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

package ssb

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/aerospike/absctl/internal/config"
	"github.com/aerospike/absctl/internal/flags"
	"github.com/aerospike/absctl/internal/ssb"
	"github.com/aerospike/absctl/internal/subcmd"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

const (
	welcomeMessage      = "Welcome to the Aerospike server dside backup tool!"
	welcomeMessageShort = "Aerospike  server dside backup tool"
)

type runner struct {
	flagsSSB *flags.ServerSideBackup

	ssbFlagSet *pflag.FlagSet
}

func NewCmd(flagsRoot *flags.Root, appVersion, commitHash, buildTime string) *cobra.Command {
	r := &runner{
		flagsSSB: flags.NewServerSideBackup(),
	}

	return subcmd.BuildCommand(
		"ssb", welcomeMessageShort, welcomeMessage,
		flagsRoot, appVersion, commitHash, buildTime,
		flags.OperationRestore, r,
	)
}

func (r *runner) FlagSets() []*pflag.FlagSet {
	r.ssbFlagSet = r.flagsSSB.NewFlagSet()

	return []*pflag.FlagSet{
		r.ssbFlagSet,
	}
}

func (r *runner) PostRegistration(_ *cobra.Command) {}

func (r *runner) SetHelpUsage(cmd *cobra.Command, shared subcmd.SharedFlagSets) {
	helpFunc := newHelpFunction(
		shared.App,
		shared.Aerospike,
		shared.ClientPolicy,
		shared.Compression,
		shared.Encryption,
		shared.SecretAgent,
		shared.Aws,
		shared.Gcp,
		shared.Azure,
		r.ssbFlagSet,
	)

	cmd.SetUsageFunc(func(_ *cobra.Command) error {
		helpFunc()
		return nil
	})
	cmd.SetHelpFunc(func(_ *cobra.Command, _ []string) {
		helpFunc()
	})
}

func (r *runner) NewServiceConfig(_ context.Context, shared *subcmd.SharedFlags) (subcmd.ServiceConfig, error) {
	cfg := config.NewSSBServiceConfig(
		r.flagsSSB.GetServerSideBackup(),
		shared.App.GetApp(),
		shared.Aerospike.NewAerospikeConfig(),
		shared.ClientPolicy.GetClientPolicy(),
		shared.Compression.GetCompression(),
		shared.Encryption.GetEncryption(),
		shared.SecretAgent.GetSecretAgent(),
		shared.Aws.GetAwsS3(),
		shared.Gcp.GetGcpStorage(),
		shared.Azure.GetAzureBlob(),
		nil,
	)

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

func (r *runner) RunService(ctx context.Context, cfg subcmd.ServiceConfig, logger *slog.Logger) error {
	ssbCfg := cfg.(*config.SSBServiceConfig)

	svc, err := ssb.NewService(ctx, ssbCfg, logger)
	if err != nil {
		return fmt.Errorf("server side backup initialization failed: %w", err)
	}

	if err = svc.Run(ctx); err != nil {
		return fmt.Errorf("server side backup failed: %w", err)
	}

	return nil
}

func newHelpFunction(
	appFlagSet,
	aerospikeFlagSet,
	clientPolicyFlagSet,
	compressionFlagSet,
	encryptionFlagSet,
	secretAgentFlagSet,
	awsFlagSet,
	gcpFlagSet,
	azureFlagSet,
	ssbFlagSet *pflag.FlagSet,
) func() {
	return func() {
		fmt.Println(welcomeMessage)
		fmt.Println(strings.Repeat("-", len(welcomeMessage)))
		fmt.Println(flags.SectionTextUsageServerSideBackup)

		// Print section: App Flags
		fmt.Println(flags.SectionTextGeneral)
		appFlagSet.PrintDefaults()

		// Print section: Common Flags
		fmt.Println(flags.SectionTextAerospike)
		aerospikeFlagSet.PrintDefaults()
		clientPolicyFlagSet.PrintDefaults()

		// Print section: SSB Flags
		fmt.Println(flags.SectionTextSSB)
		ssbFlagSet.PrintDefaults()

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
