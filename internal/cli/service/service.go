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

package service

import (
	"fmt"
	"log/slog"

	"github.com/aerospike/absctl/internal/flags"
	"github.com/aerospike/absctl/internal/logging"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

const (
	serviceShort = "Interact with Aerospike Backup Service REST API"
	serviceLong  = "Commands for managing backups, restores, and configuration via the Aerospike Backup Service."
)

// runCtx is passed to all service subcommand constructors so they share the
// connection flags and the logger configured from --verbose / --log-* flags.
type runCtx struct {
	conn   *flags.ServiceConnection
	app    *flags.App
	logger *slog.Logger
}

// NewCmd creates the root "service" command with shared connection and logger flags.
func NewCmd() *cobra.Command {
	rc := &runCtx{
		conn:   flags.NewServiceConnection(),
		app:    flags.NewApp(),
		logger: logging.NewDefaultLogger(),
	}

	cmd := &cobra.Command{
		Use:   "service",
		Short: serviceShort,
		Long:  serviceLong,
	}

	cmd.PersistentFlags().SortFlags = false
	cmd.SilenceUsage = true

	connFlagSet := rc.conn.NewFlagSet()
	appFlagSet := rc.app.NewFlagSet()

	cmd.PersistentFlags().AddFlagSet(connFlagSet)
	cmd.PersistentFlags().AddFlagSet(appFlagSet)

	var loggerClose func() error

	cmd.PersistentPreRunE = func(_ *cobra.Command, _ []string) error {
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

	setParentHelp(cmd, connFlagSet, appFlagSet)

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
