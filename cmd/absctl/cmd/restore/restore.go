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
		Use:   useCommand,
		Short: welcomeMessageShort,
		Long:  welcomeMessage,
		RunE:  c.run,
	}

	// Disable sorting
	restoreCmd.PersistentFlags().SortFlags = false
	restoreCmd.SilenceUsage = true

	appFlagSet := c.flagsApp.NewFlagSet()
	aerospikeFlagSet := c.flagsAerospike.NewFlagSet(asFlags.DefaultWrapHelpString)
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
	serviceConfig, err := c.newServiceConfig()
	if err != nil {
		return fmt.Errorf("failed to initialize app: %w", err)
	}

	// Init logger.
	logger, err := logging.NewLogger(serviceConfig.App.LogLevel, serviceConfig.App.Verbose, serviceConfig.App.LogJSON)
	if err != nil {
		return fmt.Errorf("failed to initialize logger: %w", err)
	}
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

// newServiceConfig returns a new *config.RestoreServiceConfig based on the flags or config file.
func (c *Cmd) newServiceConfig() (*config.RestoreServiceConfig, error) {
	app := c.flagsApp.GetApp()
	// If we have a config file, load serviceConfig from it.
	if app != nil && app.ConfigFilePath != "" {
		serviceConfig, err := config.DecodeRestoreServiceConfig(app.ConfigFilePath)
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

		fmt.Println("\nUsage:")
		fmt.Println("  absctl restore [flags]")

		// Print section: App Flags
		fmt.Println("\nGeneral Flags:")
		appFlagSet.PrintDefaults()

		// Print section: Common Flags
		fmt.Println("\nAerospike Client Flags:")
		aerospikeFlagSet.PrintDefaults()
		clientPolicyFlagSet.PrintDefaults()

		// Print section: Restore Flags
		fmt.Println("\nRestore Flags:")
		commonFlagSet.PrintDefaults()
		restoreFlagSet.PrintDefaults()

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
			"Example: absctl restore --azure-account-name secret:resource1:azaccount")
		secretAgentFlagSet.PrintDefaults()

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
