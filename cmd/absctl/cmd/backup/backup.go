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
	"fmt"
	"log"
	"log/slog"
	"strings"

	"github.com/aerospike/absctl/internal/backup"
	"github.com/aerospike/absctl/internal/config"
	"github.com/aerospike/absctl/internal/flags"
	"github.com/aerospike/absctl/internal/logging"
	asFlags "github.com/aerospike/tools-common-go/flags"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

const (
	welcomeMessage      = "Welcome to the Aerospike backup CLI tool!"
	welcomeMessageShort = "Aerospike backup CLI tool"
	useCommand          = "backup"
)

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
	flagsLocal        *flags.Local

	// backup flags.
	flagsBackup *flags.Backup
	flagsCommon *flags.Common

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
		flagsBackup:       flags.NewBackup(),
		flagsCompression:  flags.NewCompression(flags.OperationBackup),
		flagsEncryption:   flags.NewEncryption(flags.OperationBackup),
		flagsSecretAgent:  flags.NewSecretAgent(),
		flagsAws:          flags.NewAwsS3(flags.OperationBackup),
		flagsGcp:          flags.NewGcpStorage(flags.OperationBackup),
		flagsAzure:        flags.NewAzureBlob(flags.OperationBackup),
		flagsLocal:        flags.NewLocal(flags.OperationBackup),
		// First init default logger.
		Logger: logging.NewDefaultLogger(),
	}
	c.flagsCommon = flags.NewCommon(&c.flagsBackup.Common, flags.OperationBackup)

	backupCmd := &cobra.Command{
		Use:               useCommand,
		Short:             welcomeMessageShort,
		Long:              welcomeMessage,
		RunE:              c.run,
		PersistentPreRunE: c.preRun,
	}

	// Disable sorting
	backupCmd.PersistentFlags().SortFlags = false
	backupCmd.SilenceUsage = true

	// Add sub command
	// xdrCmd := xdr.NewCmd(
	// 	c.flagsApp,
	// 	c.flagsAerospike,
	// 	c.flagsClientPolicy,
	// 	c.flagsCompression,
	// 	c.flagsEncryption,
	// 	c.flagsSecretAgent,
	// 	c.flagsAws,
	// 	c.flagsGcp,
	// 	c.flagsAzure,
	// )
	// backupCmd.AddCommand(xdrCmd)

	appFlagSet := c.flagsApp.NewFlagSet()
	aerospikeFlagSet := c.flagsAerospike.NewFlagSet(asFlags.DefaultWrapHelpString)
	flags.WrapCertFlagsForSecrets(aerospikeFlagSet)
	clientPolicyFlagSet := c.flagsClientPolicy.NewFlagSet()
	commonFlagSet := c.flagsCommon.NewFlagSet()
	backupFlagSet := c.flagsBackup.NewFlagSet()
	compressionFlagSet := c.flagsCompression.NewFlagSet()
	encryptionFlagSet := c.flagsEncryption.NewFlagSet()
	secretAgentFlagSet := c.flagsSecretAgent.NewFlagSet()
	awsFlagSet := c.flagsAws.NewFlagSet()
	gcpFlagSet := c.flagsGcp.NewFlagSet()
	azureFlagSet := c.flagsAzure.NewFlagSet()
	localFlagSet := c.flagsLocal.NewFlagSet()

	// App flags.
	backupCmd.PersistentFlags().AddFlagSet(appFlagSet)
	backupCmd.PersistentFlags().AddFlagSet(aerospikeFlagSet)
	backupCmd.PersistentFlags().AddFlagSet(clientPolicyFlagSet)

	backupCmd.Flags().AddFlagSet(commonFlagSet)
	backupCmd.Flags().AddFlagSet(backupFlagSet)

	backupCmd.PersistentFlags().AddFlagSet(compressionFlagSet)
	backupCmd.PersistentFlags().AddFlagSet(encryptionFlagSet)
	backupCmd.PersistentFlags().AddFlagSet(secretAgentFlagSet)
	backupCmd.PersistentFlags().AddFlagSet(awsFlagSet)
	backupCmd.PersistentFlags().AddFlagSet(gcpFlagSet)
	backupCmd.PersistentFlags().AddFlagSet(azureFlagSet)
	backupCmd.PersistentFlags().AddFlagSet(localFlagSet)

	// Deprecated fields.
	if err := backupCmd.Flags().MarkDeprecated("nice", "use --bandwidth instead"); err != nil {
		log.Fatal(err)
	}

	backupCmd.Flags().Lookup("nice").Hidden = false

	// Beautify help and usage.
	helpFunc := newHelpFunction(
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
		localFlagSet,
	)

	backupCmd.SetUsageFunc(func(_ *cobra.Command) error {
		helpFunc()
		return nil
	})
	backupCmd.SetHelpFunc(func(_ *cobra.Command, _ []string) {
		helpFunc()
	})

	return backupCmd
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
	serviceConfig, err := c.newServiceConfig()
	if err != nil {
		return fmt.Errorf("failed to initialize app: %w", err)
	}

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

	asb, err := backup.NewService(cmd.Context(), serviceConfig, logger)
	if err != nil {
		return fmt.Errorf("backup initialization failed: %w", err)
	}

	if err = asb.Run(cmd.Context()); err != nil {
		return fmt.Errorf("backup failed: %w", err)
	}

	return nil
}

func (c *Cmd) preRun(cmd *cobra.Command, _ []string) error {
	sa := c.flagsSecretAgent.GetSecretAgent()

	return c.flagsApp.PreRun(cmd, sa)
}

// newServiceConfig returns a new *config.BackupServiceConfig based on the flags or config file.
func (c *Cmd) newServiceConfig() (*config.BackupServiceConfig, error) {
	app := c.flagsApp.GetApp()
	// If we have a config file, load serviceConfig from it.
	if app != nil && app.ConfigFilePath != "" {
		serviceConfig, err := config.DecodeBackupServiceConfig(app.ConfigFilePath)
		if err != nil {
			return nil, fmt.Errorf("failed to load config file %s: %w", app.ConfigFilePath, err)
		}

		return serviceConfig, nil
	}

	serviceConfig, err := config.NewBackupServiceConfig(
		c.flagsApp.GetApp(),
		c.flagsAerospike.NewAerospikeConfig(),
		c.flagsClientPolicy.GetClientPolicy(),
		c.flagsBackup.GetBackup(),
		nil,
		c.flagsCompression.GetCompression(),
		c.flagsEncryption.GetEncryption(),
		c.flagsSecretAgent.GetSecretAgent(),
		c.flagsAws.GetAwsS3(),
		c.flagsGcp.GetGcpStorage(),
		c.flagsAzure.GetAzureBlob(),
		c.flagsLocal.GetLocal(),
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
		fmt.Println("\nUsage:")
		fmt.Println("  absctl backup [flags]")

		// Print section: App Flags
		fmt.Println("\nGeneral Flags:")
		appFlagSet.PrintDefaults()

		// Print section: Common Flags
		fmt.Println("\nAerospike Client Flags:")
		aerospikeFlagSet.PrintDefaults()
		clientPolicyFlagSet.PrintDefaults()

		// Print section: Backup Flags
		fmt.Println("\nBackup Flags:")
		commonFlagSet.PrintDefaults()
		backupFlagSet.PrintDefaults()

		// Print section: Compression Flags
		fmt.Println("\nCompression Flags:")
		compressionFlagSet.PrintDefaults()

		// Print section: Encryption Flags
		fmt.Println("\nEncryption Flags:")
		encryptionFlagSet.PrintDefaults()

		// Print section: Secret Agent Flags
		fmt.Println("\nSecret Agent Flags:\n" +
			"Options pertaining to the Aerospike Secret Agent.\n" +
			"See documentation here: https://aerospike.com/docs/tools/secret-agent.\n" +
			"Both backup and restore commands support getting all the cloud configuration parameters\n" +
			"from the Aerospike Secret Agent.\n" +
			"To use a secret as an option, use this format: 'secrets:<resource_name>:<secret_name>' \n" +
			"Example: absctl backup --azure-account-name secret:resource1:azaccount")
		secretAgentFlagSet.PrintDefaults()

		// Print section: Local Flags
		fmt.Println("\nLocal Storage Flags:")
		localFlagSet.PrintDefaults()

		// Print section: AWS Flags
		fmt.Println("\nAWS Storage Flags:\n" +
			"For S3, the storage bucket name must be set with the --s3-bucket-name flag.\n" +
			"--directory path will only contain the folder name.\n" +
			"--s3-endpoint-override is used for MinIO storage instead of AWS.\n" +
			"Any AWS parameter can be retrieved from Secret Agent.")
		awsFlagSet.PrintDefaults()

		// Print section: GCP Flags
		fmt.Println("\nGCP Storage Flags:\n" +
			"For GCP storage, the bucket name must be set with --gcp-bucket-name flag.\n" +
			"--directory path will only contain the folder name.\n" +
			"The flag --gcp-endpoint-override  is optional, and is used for tests or any other GCP emulator.\n" +
			"Any GCP parameter can be retrieved from Secret Agent.")
		gcpFlagSet.PrintDefaults()

		// Print section: Azure Flags
		fmt.Println("\nAzure Storage Flags:\n" +
			"For Azure storage, the container name must be set with --azure-container-name flag.\n" +
			"--directory path will only contain folder name.\n" +
			"The flag --azure-endpoint is also mandatory, as each storage account has different service address.\n" +
			"For authentication, use --azure-account-name and --azure-account-key, or \n" +
			"--azure-tenant-id, --azure-client-id and --azure-client-secret.\n" +
			"Any Azure parameter can be retrieved from Secret Agent.")
		azureFlagSet.PrintDefaults()
	}
}
