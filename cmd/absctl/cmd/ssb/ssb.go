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
	"log"
	"log/slog"
	"strings"

	"github.com/aerospike/absctl/internal/config"
	"github.com/aerospike/absctl/internal/flags"
	"github.com/aerospike/absctl/internal/logging"
	"github.com/aerospike/absctl/internal/ssb"
	asFlags "github.com/aerospike/tools-common-go/flags"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

const (
	welcomeMessage      = "Welcome to the Aerospike server dside backup tool!"
	welcomeMessageShort = "Aerospike  server dside backup tool"
	useCommand          = "ssb"
)

type Cmd struct {
	// Version params.
	appVersion string
	commitHash string
	buildTime  string

	// Root flags
	flagsRoot         *flags.Root
	flagsApp          *flags.App
	flagsAerospike    *asFlags.AerospikeFlags
	flagsClientPolicy *flags.ClientPolicy
	flagsCompression  *flags.Compression
	flagsEncryption   *flags.Encryption
	flagsSecretAgent  *flags.SecretAgent
	flagsAws          *flags.AwsS3
	flagsGcp          *flags.GcpStorage
	flagsAzure        *flags.AzureBlob

	// Server side backup flags.
	flagsSSB *flags.ServerSideBackup

	Logger *slog.Logger
}

func NewCmd(flagsRoot *flags.Root, appVersion, commitHash, buildTime string) *cobra.Command {
	c := &Cmd{
		appVersion:        appVersion,
		commitHash:        commitHash,
		buildTime:         buildTime,
		flagsRoot:         flagsRoot,
		flagsApp:          flags.NewApp(),
		flagsAerospike:    asFlags.NewDefaultAerospikeFlags(),
		flagsClientPolicy: flags.NewClientPolicy(),
		flagsCompression:  flags.NewCompression(flags.OperationRestore),
		flagsEncryption:   flags.NewEncryption(flags.OperationRestore),
		flagsSecretAgent:  flags.NewSecretAgent(),
		flagsAws:          flags.NewAwsS3(flags.OperationRestore),
		flagsGcp:          flags.NewGcpStorage(flags.OperationRestore),
		flagsAzure:        flags.NewAzureBlob(flags.OperationRestore),
		// First init default logger.
		Logger: logging.NewDefaultLogger(),

		flagsSSB: flags.NewServerSideBackup(),
	}

	ssbCmd := &cobra.Command{
		Use:               useCommand,
		Short:             welcomeMessageShort,
		Long:              welcomeMessage,
		RunE:              c.run,
		PersistentPreRunE: c.preRun,
	}

	// Disable sorting
	ssbCmd.PersistentFlags().SortFlags = false
	ssbCmd.SilenceUsage = true

	appFlagSet := c.flagsApp.NewFlagSet()
	aerospikeFlagSet := c.flagsAerospike.NewFlagSet(asFlags.DefaultWrapHelpString)
	flags.WrapCertFlagsForSecrets(aerospikeFlagSet)
	clientPolicyFlagSet := c.flagsClientPolicy.NewFlagSet()
	compressionFlagSet := c.flagsCompression.NewFlagSet()
	encryptionFlagSet := c.flagsEncryption.NewFlagSet()
	secretAgentFlagSet := c.flagsSecretAgent.NewFlagSet()
	awsFlagSet := c.flagsAws.NewFlagSet()
	gcpFlagSet := c.flagsGcp.NewFlagSet()
	azureFlagSet := c.flagsAzure.NewFlagSet()

	ssbFlagSet := c.flagsSSB.NewFlagSet()

	// App flags.
	ssbCmd.PersistentFlags().AddFlagSet(appFlagSet)
	ssbCmd.PersistentFlags().AddFlagSet(aerospikeFlagSet)
	ssbCmd.PersistentFlags().AddFlagSet(clientPolicyFlagSet)

	ssbCmd.Flags().AddFlagSet(ssbFlagSet)

	ssbCmd.PersistentFlags().AddFlagSet(compressionFlagSet)
	ssbCmd.PersistentFlags().AddFlagSet(encryptionFlagSet)
	ssbCmd.PersistentFlags().AddFlagSet(secretAgentFlagSet)
	ssbCmd.PersistentFlags().AddFlagSet(awsFlagSet)
	ssbCmd.PersistentFlags().AddFlagSet(gcpFlagSet)
	ssbCmd.PersistentFlags().AddFlagSet(azureFlagSet)

	// Beautify help and usage.
	helpFunc := newHelpFunction(
		appFlagSet,
		aerospikeFlagSet,
		clientPolicyFlagSet,
		compressionFlagSet,
		encryptionFlagSet,
		secretAgentFlagSet,
		awsFlagSet,
		gcpFlagSet,
		azureFlagSet,

		ssbFlagSet,
	)

	ssbCmd.SetUsageFunc(func(_ *cobra.Command) error {
		helpFunc()
		return nil
	})
	ssbCmd.SetHelpFunc(func(_ *cobra.Command, _ []string) {
		helpFunc()
	})

	return ssbCmd
}

func (c *Cmd) run(cmd *cobra.Command, _ []string) error {
	if c.flagsRoot.Version {
		c.printVersion()

		return nil
	}

	// If no flags were passed, show help.
	if cmd.Flags().NFlag() == 0 {
		if err := cmd.Help(); err != nil {
			return fmt.Errorf("failed to load help: %w", err)
		}

		return nil
	}

	// Init app.
	serviceConfig, err := c.newServiceConfig(cmd.Context())
	if err != nil {
		return fmt.Errorf("failed to initialize app: %w", err)
	}

	// Init logger.
	// Init logger.
	loggerConf := logging.NewConfig(
		serviceConfig.App.Verbose,
		serviceConfig.App.LogJSON,
		serviceConfig.App.LogLevel,
		serviceConfig.App.LogFile,
	)

	logger, loggerClose, err := logging.NewLogger(loggerConf)
	if err != nil {
		return fmt.Errorf("failed to initialize logger: %w", err)
	}

	defer func() {
		if err := loggerClose(); err != nil {
			log.Printf("failed to close logger: %v", err)
		}
	}()
	// After initialization replace logger.
	c.Logger = logger

	asr, err := ssb.NewService(cmd.Context(), serviceConfig, logger)
	if err != nil {
		return fmt.Errorf("server side backup initialization failed: %w", err)
	}

	if err = asr.Run(cmd.Context()); err != nil {
		return fmt.Errorf("server side backup failed: %w", err)
	}

	return nil
}

func (c *Cmd) preRun(cmd *cobra.Command, _ []string) error {
	sa := c.flagsSecretAgent.GetSecretAgent()

	return c.flagsApp.PreRun(cmd, sa)
}

// newServiceConfig returns a new *config.RestoreServiceConfig based on the flags or config file.
func (c *Cmd) newServiceConfig(_ context.Context) (*config.ServiceConfigCommon, error) {
	serviceConfig := config.NewServiceConfigCommon(
		c.flagsApp.GetApp(),
		c.flagsAerospike.NewAerospikeConfig(),
		c.flagsClientPolicy.GetClientPolicy(),
		c.flagsCompression.GetCompression(),
		c.flagsEncryption.GetEncryption(),
		c.flagsSecretAgent.GetSecretAgent(),
		c.flagsAws.GetAwsS3(),
		c.flagsGcp.GetGcpStorage(),
		c.flagsAzure.GetAzureBlob(),
		nil,
	)

	return serviceConfig, nil
}

func (c *Cmd) printVersion() {
	fmt.Printf("version: %s (%s) %s \n", c.appVersion, c.commitHash, c.buildTime)
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

		// Commented until XDR will be released.
		// fmt.Println("The restore tool automatically identifies and " +
		// 	"restores ASB and ASBX backup files found in the specified folder.")
		// fmt.Println("You can set restore mode manually with --mode flag. " +
		// 	"Flags that are incompatible with restore mode,")
		// fmt.Println("are also incompatible in automatic mode (when mode is not set).")
		fmt.Println(flags.SectionTextUsageRestore)

		// Print section: App Flags
		fmt.Println(flags.SectionTextGeneral)
		appFlagSet.PrintDefaults()

		// Print section: Common Flags
		fmt.Println(flags.SectionTextAerospike)
		aerospikeFlagSet.PrintDefaults()
		clientPolicyFlagSet.PrintDefaults()

		// Print section: Restore Flags
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
