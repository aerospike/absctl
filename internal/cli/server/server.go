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

package server

import (
	"fmt"
	"log/slog"

	"github.com/aerospike/absctl/internal/flags"
	"github.com/aerospike/absctl/internal/logging"
	asFlags "github.com/aerospike/tools-common-go/flags"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

const (
	serverShort = "Manage server-integrated backups and restores"
	serverLong  = "Commands for triggering and inspecting backups and restores that run inside the Aerospike server."

	useBackup   = "backup"
	useRestore  = "restore"
	useStart    = "start"
	useList     = "list"
	useProgress = "progress"
	usePrepare  = "prepare"
)

// runCtx is passed to all server subcommand constructors so they share the
// flag objects collected on the parent commands and the lazily-initialized logger.
type runCtx struct {
	flagsRoot    *flags.Root
	app          *flags.App
	aerospike    *asFlags.AerospikeFlags
	clientPolicy *flags.ClientPolicy
	secretAgent  *flags.SecretAgent

	logger *slog.Logger

	appVersion string
	commitHash string
	buildTime  string
}

// NewCmd creates the root "server" command with the flags shared by all
// server-integrated backup and restore subcommands.
func NewCmd(flagsRoot *flags.Root, appVersion, commitHash, buildTime string) *cobra.Command {
	rc := &runCtx{
		flagsRoot:    flagsRoot,
		app:          flags.NewApp(),
		aerospike:    asFlags.NewDefaultAerospikeFlags(),
		clientPolicy: flags.NewClientPolicy(),
		secretAgent:  flags.NewSecretAgent(),
		logger:       logging.NewDefaultLogger(),
		appVersion:   appVersion,
		commitHash:   commitHash,
		buildTime:    buildTime,
	}

	cmd := &cobra.Command{
		Use:   "server",
		Short: serverShort,
		Long:  serverLong,
	}

	cmd.PersistentFlags().SortFlags = false
	cmd.SilenceUsage = true

	appFlagSet := rc.app.NewFlagSet()
	aerospikeFlagSet := rc.aerospike.NewFlagSet(asFlags.DefaultWrapHelpString)
	flags.WrapFlagsForSecrets(aerospikeFlagSet)
	clientPolicyFlagSet := rc.clientPolicy.NewFlagSet()
	secretAgentFlagSet := rc.secretAgent.NewFlagSet()

	cmd.PersistentFlags().AddFlagSet(appFlagSet)
	cmd.PersistentFlags().AddFlagSet(aerospikeFlagSet)
	cmd.PersistentFlags().AddFlagSet(clientPolicyFlagSet)
	cmd.PersistentFlags().AddFlagSet(secretAgentFlagSet)

	var loggerClose func() error

	cmd.PersistentPreRunE = func(c *cobra.Command, _ []string) error {
		if err := rc.app.PreRun(c, rc.secretAgent.GetSecretAgent()); err != nil {
			return err
		}

		app := rc.app.GetApp()
		cfg := logging.NewConfig(app.Verbose, app.LogJSON, app.LogLevel, app.LogFile)

		logger, closeFn, err := logging.NewLogger(cfg)
		if err != nil {
			return fmt.Errorf("failed to initialize logger: %w", err)
		}

		rc.logger = logger
		loggerClose = closeFn

		return nil
	}

	cmd.PersistentPostRunE = func(c *cobra.Command, _ []string) error {
		if loggerClose == nil {
			return nil
		}

		if err := loggerClose(); err != nil {
			fmt.Fprintf(c.ErrOrStderr(), "failed to close logger: %v\n", err)
		}

		return nil
	}

	cmd.AddCommand(newBackupCmd(rc))
	cmd.AddCommand(newRestoreCmd(rc))

	setParentHelp(cmd, appFlagSet, aerospikeFlagSet, clientPolicyFlagSet, secretAgentFlagSet)

	return cmd
}

// setParentHelp overrides the root-inherited help for commands that have subcommands.
func setParentHelp(cmd *cobra.Command, flagSets ...*pflag.FlagSet) {
	cmd.SetHelpFunc(func(c *cobra.Command, _ []string) {
		fmt.Println(c.Short)
		fmt.Printf("\nUsage:\n  %s [command]\n", c.CommandPath())
		fmt.Println("\nAvailable Commands:")

		for _, sub := range c.Commands() {
			if !sub.IsAvailableCommand() {
				continue
			}

			fmt.Printf("  %-25s %s\n", sub.Name(), sub.Short)
		}

		if len(flagSets) > 0 {
			fmt.Println("\nFlags:")

			for _, fs := range flagSets {
				fmt.Print(fs.FlagUsages())
			}
		}

		fmt.Printf("\nUse \"%s [command] --help\" for more information about a command.\n", c.CommandPath())
	})

	cmd.SetUsageFunc(func(c *cobra.Command) error {
		c.HelpFunc()(c, nil)
		return nil
	})
}

// setLeafHelp overrides the root-inherited help for leaf commands.
func setLeafHelp(cmd *cobra.Command) {
	cmd.SetHelpFunc(func(c *cobra.Command, _ []string) {
		fmt.Println(c.Short)
		fmt.Printf("\nUsage:\n  %s [flags]\n", c.CommandPath())

		local := c.LocalFlags()
		if local.HasFlags() {
			fmt.Println("\nFlags:")
			fmt.Print(local.FlagUsages())
		}

		inherited := c.InheritedFlags()
		if inherited.HasFlags() {
			fmt.Println("\nGlobal Flags:")
			fmt.Print(inherited.FlagUsages())
		}
	})

	cmd.SetUsageFunc(func(c *cobra.Command) error {
		c.HelpFunc()(c, nil)
		return nil
	})
}
