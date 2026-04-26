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

package create

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"strings"

	"github.com/aerospike/absctl/internal/config"
	"github.com/aerospike/absctl/internal/flags"
	"github.com/aerospike/absctl/internal/integrated"
	"github.com/aerospike/absctl/internal/logging"
	"github.com/aerospike/absctl/internal/subcmd"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

const (
	welcomeMessage = "Create a server-integrated backup on the Aerospike cluster."
	shortMessage   = "Create a server-integrated backup"
)

func NewCmd(shared *subcmd.SharedFlags, flagsRoot *flags.Root, appVersion, commitHash, buildTime string,
) *cobra.Command {
	ssbFlags := flags.NewIntegratedBackup()
	ssbFlagSet := ssbFlags.NewBackupCreateFlagSet()

	cmd := &cobra.Command{
		Use:   "create",
		Short: shortMessage,
		Long:  welcomeMessage,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return run(cmd, shared, ssbFlags, flagsRoot, appVersion, commitHash, buildTime)
		},
	}

	awsFlagSet := subcmd.NewSharedFlagSets(shared).Aws

	cmd.Flags().AddFlagSet(ssbFlagSet)
	cmd.SetHelpFunc(func(_ *cobra.Command, _ []string) {
		newHelpFunction(flags.SectionTextUsageBackupCreate, ssbFlagSet, awsFlagSet)
	})
	cmd.SetUsageFunc(func(_ *cobra.Command) error {
		newHelpFunction(flags.SectionTextUsageBackupCreate, ssbFlagSet, awsFlagSet)
		return nil
	})

	return cmd
}

func run(
	cmd *cobra.Command,
	shared *subcmd.SharedFlags,
	ssbFlags *flags.IntegratedBackup,
	flagsRoot *flags.Root,
	appVersion, commitHash, buildTime string,
) error {
	if flagsRoot.Version {
		fmt.Printf("version: %s (%s) %s \n", appVersion, commitHash, buildTime)
		return nil
	}

	cfg, logger, loggerClose, err := initService(cmd.Context(), shared, ssbFlags)
	if err != nil {
		return err
	}

	defer func() {
		if err := loggerClose(); err != nil {
			log.Printf("failed to close logger: %v", err)
		}
	}()

	svc, err := integrated.NewService(cmd.Context(), cfg, logger)
	if err != nil {
		return fmt.Errorf("server side backup initialization failed: %w", err)
	}

	if err := svc.StartBackup(cmd.Context()); err != nil {
		return fmt.Errorf("server side backup failed: %w", err)
	}

	return nil
}

func initService(
	_ context.Context,
	shared *subcmd.SharedFlags,
	ssbFlags *flags.IntegratedBackup,
) (*config.IntegratedServiceConfig, *slog.Logger, func() error, error) {
	cfg := config.NewIntegratedServiceConfig(
		ssbFlags.GetIntegratedBackup(),
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

	if err := cfg.Validate(true); err != nil {
		return nil, nil, nil, fmt.Errorf("failed to validate config: %w", err)
	}

	app := cfg.GetApp()
	loggerConf := logging.NewConfig(app.Verbose, app.LogJSON, app.LogLevel, app.LogFile)

	logger, loggerClose, err := logging.NewLogger(loggerConf)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to initialize logger: %w", err)
	}

	return cfg, logger, loggerClose, nil
}

func newHelpFunction(usageText string, ssbFlagSet, awsFlagSet *pflag.FlagSet) {
	fmt.Println(welcomeMessage)
	fmt.Println(strings.Repeat("-", len(welcomeMessage)))

	fmt.Println(usageText)

	fmt.Println(flags.SectionTextSSB)
	ssbFlagSet.PrintDefaults()

	// Print section: AWS Flags
	fmt.Println(flags.SectionTextAWS)
	awsFlagSet.PrintDefaults()

	fmt.Println("\nUse \"absctl backup --help\" for the full list of inherited flags.")
}
