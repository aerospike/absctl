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
	"io"
	"log/slog"
	"strings"

	"github.com/aerospike/absctl/internal/config"
	"github.com/aerospike/absctl/internal/flags"
	"github.com/aerospike/absctl/internal/logging"
	"github.com/aerospike/absctl/internal/server"
	asFlags "github.com/aerospike/tools-common-go/flags"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// runCtx is shared by every server subcommand constructor. It owns the flag
// holders collected on the parent command and the lazily-initialized logger.
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

// newRunCtx constructs a runCtx with freshly-allocated flag holders and a
// default logger. Each top-level command (backup, restore) gets its own
// runCtx; the logger is replaced by applyRootPersistent during PreRun.
func newRunCtx(flagsRoot *flags.Root, appVersion, commitHash, buildTime string) *runCtx {
	return &runCtx{
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
}

// commonFlagSets holds the connection-related flag sets in help-display order.
// Returning a named struct instead of a []*pflag.FlagSet keeps the help
// builders readable (common.aerospike vs common[1]).
type commonFlagSets struct {
	app          *pflag.FlagSet
	aerospike    *pflag.FlagSet
	clientPolicy *pflag.FlagSet
	secretAgent  *pflag.FlagSet
}

// applyRootPersistent attaches the application-wide persistent flags and the
// logger lifecycle hooks to a top-level command (backup, restore).
//
// The logger is needed by every subcommand, so the hooks live on the parent
// and are inherited by all children. This fixes the previous behavior where
// subcommands that did not call applyCommon (list, validate) ran with the
// default logger and without SilenceUsage.
func applyRootPersistent(cmd *cobra.Command, rc *runCtx) {
	cmd.SilenceUsage = true
	cmd.PersistentFlags().SortFlags = false
	cmd.PersistentFlags().AddFlagSet(rc.app.NewFlagSet())

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
}

// applyCommon binds the Aerospike connection flag sets (aerospike, client
// policy, secret agent) to a data-plane subcommand and returns all common
// flag sets for help rendering. Unlike the previous version it no longer
// installs lifecycle hooks — those belong on the parent (applyRootPersistent).
func applyCommon(cmd *cobra.Command, rc *runCtx) commonFlagSets {
	fs := commonFlagSets{
		app:          rc.app.NewFlagSet(),
		aerospike:    rc.aerospike.NewFlagSet(asFlags.DefaultWrapHelpString),
		clientPolicy: rc.clientPolicy.NewFlagSet(),
		secretAgent:  rc.secretAgent.NewFlagSet(),
	}
	flags.WrapFlagsForSecrets(fs.aerospike)

	cmd.Flags().SortFlags = false
	cmd.Flags().AddFlagSet(fs.aerospike)
	cmd.Flags().AddFlagSet(fs.clientPolicy)
	cmd.Flags().AddFlagSet(fs.secretAgent)

	return fs
}

// printHelpHeader writes the welcome banner, a matching underline and the
// usage section for a command.
func printHelpHeader(w io.Writer, usage string) {
	fmt.Fprintln(w, textWelcomeMessage)
	fmt.Fprintln(w, strings.Repeat("-", len(textWelcomeMessage)))
	fmt.Fprintln(w, usage)
}

// printSection writes a section title followed by the defaults of one or more
// flag sets, all routed to w so help output is testable and consistent.
func printSection(w io.Writer, title string, sets ...*pflag.FlagSet) {
	fmt.Fprintln(w, title)

	for _, fs := range sets {
		fs.SetOutput(w)
		fs.PrintDefaults()
	}
}

// printCommands lists the available subcommands of c in two-column form.
func printCommands(w io.Writer, c *cobra.Command) {
	fmt.Fprintln(w, flags.SectionAvailableCommands)

	for _, sub := range c.Commands() {
		if !sub.IsAvailableCommand() {
			continue
		}

		fmt.Fprintf(w, "  %-25s %s\n", sub.Name(), sub.Short)
	}
}

// usageFromHelp wires SetUsageFunc to delegate to the command's help func so a
// usage error and "--help" render identically.
func usageFromHelp(cmd *cobra.Command) {
	cmd.SetUsageFunc(func(c *cobra.Command) error {
		c.HelpFunc()(c, nil)
		return nil
	})
}

// newService validates cfg and constructs the server-side service, wrapping an
// initialization failure with the supplied prefix (errInitBackup /
// errInitRestore).
func newService(
	rc *runCtx,
	backupCfg *config.ServerBackupServiceConfig,
	restoreCfg *config.ServerRestoreServiceConfig,
) (*server.Service, error) {
	if backupCfg != nil {
		if err := backupCfg.Validate(false); err != nil {
			return nil, fmt.Errorf("failed to validate backup config: %w", err)
		}
	}

	if restoreCfg != nil {
		if err := restoreCfg.Validate(false); err != nil {
			return nil, fmt.Errorf("failed to validate restore config: %w", err)
		}
	}

	svc, err := server.NewService(backupCfg, restoreCfg, rc.logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create service: %w", err)
	}

	return svc, nil
}
