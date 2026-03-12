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
	"github.com/aerospike/absctl/internal/logging"
	"github.com/aerospike/absctl/internal/restore"
	asFlags "github.com/aerospike/tools-common-go/flags"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

const (
	welcomeMessage      = "Welcome to the Aerospike restore CLI tool!"
	welcomeMessageShort = "Aerospike restore CLI tool"
	useCommand          = "restore"
)

// Text for usage pretty-print.

// Cmd represents the base command when called without any subcommands
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

	// Restore flags.
	flagsRestore *flags.Restore
	flagsCommon  *flags.Common

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
		flagsRestore:      flags.NewRestore(),
		flagsCompression:  flags.NewCompression(flags.OperationRestore),
		flagsEncryption:   flags.NewEncryption(flags.OperationRestore),
		flagsSecretAgent:  flags.NewSecretAgent(),
		flagsAws:          flags.NewAwsS3(flags.OperationRestore),
		flagsGcp:          flags.NewGcpStorage(flags.OperationRestore),
		flagsAzure:        flags.NewAzureBlob(flags.OperationRestore),
		// First init default logger.
		Logger: logging.NewDefaultLogger(),
	}

	c.flagsCommon = flags.NewCommon(&c.flagsRestore.Common, flags.OperationRestore)

	restoreCmd := &cobra.Command{
		Use:               useCommand,
		Short:             welcomeMessageShort,
		Long:              welcomeMessage,
		RunE:              c.run,
		PersistentPreRunE: c.preRun,
	}

	// Disable sorting
	restoreCmd.PersistentFlags().SortFlags = false
	restoreCmd.SilenceUsage = true

	appFlagSet := c.flagsApp.NewFlagSet()
	aerospikeFlagSet := c.flagsAerospike.NewFlagSet(asFlags.DefaultWrapHelpString)
	flags.WrapCertFlagsForSecrets(aerospikeFlagSet)
	clientPolicyFlagSet := c.flagsClientPolicy.NewFlagSet()
	commonFlagSet := c.flagsCommon.NewFlagSet()
	restoreFlagSet := c.flagsRestore.NewFlagSet()
	compressionFlagSet := c.flagsCompression.NewFlagSet()
	encryptionFlagSet := c.flagsEncryption.NewFlagSet()
	secretAgentFlagSet := c.flagsSecretAgent.NewFlagSet()
	awsFlagSet := c.flagsAws.NewFlagSet()
	gcpFlagSet := c.flagsGcp.NewFlagSet()
	azureFlagSet := c.flagsAzure.NewFlagSet()

	// App flags.
	restoreCmd.PersistentFlags().AddFlagSet(appFlagSet)
	restoreCmd.PersistentFlags().AddFlagSet(aerospikeFlagSet)
	restoreCmd.PersistentFlags().AddFlagSet(clientPolicyFlagSet)

	restoreCmd.Flags().AddFlagSet(commonFlagSet)
	restoreCmd.Flags().AddFlagSet(restoreFlagSet)

	restoreCmd.PersistentFlags().AddFlagSet(compressionFlagSet)
	restoreCmd.PersistentFlags().AddFlagSet(encryptionFlagSet)
	restoreCmd.PersistentFlags().AddFlagSet(secretAgentFlagSet)
	restoreCmd.PersistentFlags().AddFlagSet(awsFlagSet)
	restoreCmd.PersistentFlags().AddFlagSet(gcpFlagSet)
	restoreCmd.PersistentFlags().AddFlagSet(azureFlagSet)

	// Deprecated fields.
	if err := restoreCmd.Flags().MarkDeprecated("nice", "use --bandwidth instead"); err != nil {
		log.Fatal(err)
	}

	restoreCmd.Flags().Lookup("nice").Hidden = false

	// Beautify help and usage.
	helpFunc := newHelpFunction(
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
		azureFlagSet,
	)

	restoreCmd.SetUsageFunc(func(_ *cobra.Command) error {
		helpFunc()
		return nil
	})
	restoreCmd.SetHelpFunc(func(_ *cobra.Command, _ []string) {
		helpFunc()
	})

	return restoreCmd
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

	logMsg := "restore"
	if serviceConfig.Restore.ValidateOnly {
		logMsg = "validation"
	}

	asr, err := restore.NewService(cmd.Context(), serviceConfig, logger)
	if err != nil {
		return fmt.Errorf("%s initialization failed: %w", logMsg, err)
	}

	if err = asr.Run(cmd.Context()); err != nil {
		return fmt.Errorf("%s failed: %w", logMsg, err)
	}

	return nil
}

func (c *Cmd) preRun(cmd *cobra.Command, _ []string) error {
	sa := c.flagsSecretAgent.GetSecretAgent()

	return c.flagsApp.PreRun(cmd, sa)
}

// newServiceConfig returns a new *config.RestoreServiceConfig based on the flags or config file.
func (c *Cmd) newServiceConfig(ctx context.Context) (*config.RestoreServiceConfig, error) {
	app := c.flagsApp.GetApp()
	// If we have a config file, load serviceConfig from it.
	if app != nil && app.ConfigFilePath != "" {
		serviceConfig, err := config.DecodeRestoreServiceConfig(ctx, app.ConfigFilePath)
		if err != nil {
			return nil, fmt.Errorf("failed to load config file %s: %w", app.ConfigFilePath, err)
		}

		return serviceConfig, nil
	}

	serviceConfig, err := config.NewRestoreServiceConfig(
		c.flagsApp.GetApp(),
		c.flagsAerospike.NewAerospikeConfig(),
		c.flagsClientPolicy.GetClientPolicy(),
		c.flagsRestore.GetRestore(),
		c.flagsCompression.GetCompression(),
		c.flagsEncryption.GetEncryption(),
		c.flagsSecretAgent.GetSecretAgent(),
		c.flagsAws.GetAwsS3(),
		c.flagsGcp.GetGcpStorage(),
		c.flagsAzure.GetAzureBlob(),
	)
	if err != nil {
		return nil, err
	}

	return serviceConfig, nil
}

func (c *Cmd) printVersion() {
	fmt.Printf("version: %s (%s) %s \n", c.appVersion, c.commitHash, c.buildTime)
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
