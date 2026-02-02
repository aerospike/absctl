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

package cmd

import (
	"fmt"
	"log/slog"
	"strings"

	"github.com/aerospike/absctl/cmd/absctl/cmd/backup"
	"github.com/aerospike/absctl/cmd/absctl/cmd/restore"
	"github.com/aerospike/absctl/internal/flags"
	"github.com/aerospike/absctl/internal/logging"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

const (
	VersionDev     = "dev"
	welcomeMessage = "Welcome to the Aerospike Backup CLI tool!"
	useMessage     = "absctl"
	shortMessage   = "Aerospike Backup CLI tool"
)

// Cmd represents the base command when called without any subcommands
type Cmd struct {
	// Version params.
	appVersion string
	commitHash string
	buildTime  string

	flagsRoot *flags.Root

	Logger *slog.Logger
}

func NewCmd(appVersion, commitHash, buildTime string) (*cobra.Command, *Cmd) {
	c := &Cmd{
		appVersion: appVersion,
		commitHash: commitHash,
		buildTime:  buildTime,

		flagsRoot: flags.NewRoot(),

		// First init default logger.
		Logger: logging.NewDefaultLogger(),
	}

	rootCmd := &cobra.Command{
		Use:   useMessage,
		Short: shortMessage,
		Long:  welcomeMessage,
		RunE:  c.run,
	}

	// Disable sorting
	rootCmd.PersistentFlags().SortFlags = false
	rootCmd.SilenceUsage = true

	rootFlagSet := c.flagsRoot.NewFlagSet()
	// App flags.
	rootCmd.PersistentFlags().AddFlagSet(rootFlagSet)

	// Add subcommands - they will initialize their own operation-specific flags
	backupCmd := backup.NewCmd(c.flagsRoot, appVersion, commitHash, buildTime)
	restoreCmd := restore.NewCmd(c.flagsRoot, appVersion, commitHash, buildTime)

	rootCmd.AddCommand(backupCmd)
	rootCmd.AddCommand(restoreCmd)

	helpFunc := newHelpFunction(rootFlagSet)

	rootCmd.SetUsageFunc(func(_ *cobra.Command) error {
		helpFunc()
		return nil
	})
	rootCmd.SetHelpFunc(func(_ *cobra.Command, _ []string) {
		helpFunc()
	})

	// Set cobra output to logger.
	logWriter := logging.NewCobraLogger(c.Logger)
	rootCmd.SetOut(logWriter)
	rootCmd.SetErr(logWriter)

	return rootCmd, c
}

func (c *Cmd) run(cmd *cobra.Command, _ []string) error {
	// Show version.
	if c.flagsRoot.Version {
		c.printVersion()

		return nil
	}

	return cmd.Help()
}

func (c *Cmd) printVersion() {
	fmt.Printf("version: %s (%s) %s \n", c.appVersion, c.commitHash, c.buildTime)
}

func newHelpFunction(flagSet *pflag.FlagSet) func() {
	return func() {
		fmt.Println(welcomeMessage)
		fmt.Println(strings.Repeat("-", len(welcomeMessage)))
		fmt.Println("\nUsage:")
		fmt.Println("  absctl [command] [flags]")
		fmt.Println("\nAvailable Commands:")
		fmt.Println("  backup    Aerospike backup command")
		fmt.Println("  restore   Aerospike restore command")
		fmt.Println("\nFlags:")
		flagSet.PrintDefaults()
		fmt.Println("\nUse \"absctl [command] --help\" for more information about a command.")
	}
}
