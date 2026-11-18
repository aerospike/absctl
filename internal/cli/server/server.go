// Copyright 2026 Aerospike, Inc.
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
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/aerospike/absctl/internal/config"
	"github.com/aerospike/absctl/internal/flags"
	"github.com/aerospike/absctl/internal/logging"
	"github.com/aerospike/absctl/internal/models"
	"github.com/aerospike/absctl/internal/server"
	"github.com/aerospike/absctl/internal/subcmd"
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

// configLoader decodes the configuration file for the subcommand leaf, keeps the
// decoded service config for the RunE of leaf and returns its app section.
type configLoader func(ctx context.Context, leaf *cobra.Command, path string) (*models.App, error)

// applyRootPersistent attaches the application-wide persistent flags and the
// logger lifecycle hooks to a top-level command (backup, restore).
//
// The logger is needed by every subcommand, so the hooks live on the parent
// and are inherited by all children. This fixes the previous behavior where
// subcommands that did not call applyCommon (list, validate) ran with the
// default logger and without SilenceUsage.
//
// Flags combined with --config are rejected before load decodes the file or the
// Secret Agent is contacted for flag values, so ignored input never triggers either.
func applyRootPersistent(cmd *cobra.Command, rc *runCtx, load configLoader) {
	cmd.SilenceUsage = true
	cmd.PersistentFlags().SortFlags = false
	cmd.PersistentFlags().AddFlagSet(rc.app.NewFlagSet())

	var loggerClose func() error

	cmd.PersistentPreRunE = func(c *cobra.Command, _ []string) error {
		// The group itself runs no service and --version runs none either, so neither
		// needs secrets or a logger.
		if c == cmd || rc.flagsRoot.Version {
			return nil
		}

		if err := subcmd.RejectFlagsWithConfig(c); err != nil {
			return err
		}

		app := rc.app.GetApp()

		// A config file supersedes the flags entirely, logging settings included,
		// and resolves its own secrets.
		if app.ConfigFilePath != "" {
			fileApp, err := load(c.Context(), c, app.ConfigFilePath)
			if err != nil {
				return err
			}

			app = fileApp
		} else if err := rc.app.PreRun(c, rc.secretAgent.GetSecretAgent()); err != nil {
			return err
		}

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

// subcommandSection pairs a subcommand with the config file section it reads.
type subcommandSection[S any] struct {
	cmd     *cobra.Command
	section S
}

// addSubcommands adds every subcommand to group and returns the config file section of
// each one, keyed by the subcommand. Registering both from one list keeps every
// subcommand of the group bound to a section.
func addSubcommands[S any](group *cobra.Command, subs []subcommandSection[S]) map[*cobra.Command]S {
	sections := make(map[*cobra.Command]S, len(subs))

	for _, sub := range subs {
		group.AddCommand(sub.cmd)
		sections[sub.cmd] = sub.section
	}

	return sections
}

// runOrVersion returns a cobra RunE that prints the version when --version is set and
// calls run otherwise.
func runOrVersion(rc *runCtx, run func(cmd *cobra.Command) error) func(*cobra.Command, []string) error {
	return func(cmd *cobra.Command, _ []string) error {
		if rc.flagsRoot.Version {
			subcmd.PrintVersion(rc.appVersion, rc.commitHash, rc.buildTime)

			return nil
		}

		return run(cmd)
	}
}

// argHelp is the only positional argument a command group accepts.
const argHelp = "help"

// errConfigNotLoaded is returned when --config is set but the configuration file was
// not decoded for the subcommand being run.
var errConfigNotLoaded = errors.New("config file was not loaded")

// ErrFlagsWithoutSubcommand is returned when a command group is given flags but no
// subcommand that would act on them.
var ErrFlagsWithoutSubcommand = errors.New("flags require a subcommand")

// groupRunE returns the RunE of a command group: it prints the version or the help.
// Without a RunE cobra prints help and exits with code 0 before validating the
// arguments, so a mistyped subcommand would pass unnoticed.
func groupRunE(rc *runCtx) func(*cobra.Command, []string) error {
	return runOrVersion(rc, func(cmd *cobra.Command) error {
		if err := subcmd.RejectEmptyConfig(cmd); err != nil {
			return err
		}

		if err := rejectFlagsWithoutSubcommand(cmd); err != nil {
			return err
		}

		return cmd.Help()
	})
}

// groupArgs is the positional argument validator of a command group. It accepts a lone
// "help" and rejects anything else, so a mistyped subcommand fails instead of printing
// help with exit code 0. Leaf commands use cobra.NoArgs instead.
func groupArgs(cmd *cobra.Command, args []string) error {
	if len(args) == 0 || args[0] != argHelp {
		return cobra.NoArgs(cmd, args)
	}

	if len(args) > 1 {
		return fmt.Errorf("unknown command %q for %q, use %q",
			strings.Join(args, " "), cmd.CommandPath(),
			cmd.CommandPath()+" "+strings.Join(args[1:], " ")+" --help")
	}

	return nil
}

// rejectFlagsWithoutSubcommand fails when a command group is run with flags but no
// subcommand, because the group would print help and silently ignore them. --help and
// --version never reach it: cobra handles the first, runOrVersion the second.
func rejectFlagsWithoutSubcommand(cmd *cobra.Command) error {
	var names []string

	cmd.Flags().Visit(func(f *pflag.Flag) {
		names = append(names, "--"+f.Name)
	})

	if len(names) > 0 {
		return fmt.Errorf("%w: %s", ErrFlagsWithoutSubcommand, strings.Join(names, ", "))
	}

	return nil
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
	flags.PrepareAerospikeFlags(fs.aerospike)

	cmd.Flags().SortFlags = false
	cmd.Flags().AddFlagSet(fs.aerospike)
	cmd.Flags().AddFlagSet(fs.clientPolicy)
	cmd.Flags().AddFlagSet(fs.secretAgent)

	return fs
}

// printSubcommandHelp writes the usage line and flag sections for a subcommand.
//
//nolint:gocritic // THis is doc gen function we don't need to optimize it and use pointer instead.
func printSubcommandHelp(doc SubcommandDoc) {
	printHelpHeader(doc.Usage)

	for _, sec := range doc.Sections {
		printSection(sec.Title, sec.FlagSets...)
	}
}

// printHelpHeader writes the welcome banner, a matching underline and the
// usage section for a command.
func printHelpHeader(usage string) {
	fmt.Println(textWelcomeMessage)
	fmt.Println(strings.Repeat("-", len(textWelcomeMessage)))
	fmt.Println(usage)
}

// printSection writes a section title followed by the defaults of one or more
// flag sets, all routed to w so help output is testable and consistent.
func printSection(title string, sets ...*pflag.FlagSet) {
	fmt.Println(title)

	for _, fs := range sets {
		fs.PrintDefaults()
	}
}

// printCommands lists the available subcommands of c in two-column form.
func printCommands(c *cobra.Command) {
	fmt.Println(flags.SectionAvailableCommands)

	for _, sub := range c.Commands() {
		if !sub.IsAvailableCommand() {
			continue
		}

		fmt.Printf("  %-25s %s\n", sub.Name(), sub.Short)
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
