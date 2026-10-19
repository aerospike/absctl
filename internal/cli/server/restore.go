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
	"fmt"

	"github.com/aerospike/absctl/internal/config"
	"github.com/aerospike/absctl/internal/flags"
	"github.com/aerospike/absctl/internal/models"
	"github.com/aerospike/absctl/internal/server"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// restoreCtx groups the storage flag holders shared by the "server restore"
// subcommands.
type restoreCtx struct {
	start    *flags.ServerRestore
	prepare  *flags.ServerRestorePrepare
	progress *flags.ServerRestoreProgress
	abort    *flags.ServerRestoreAbort
	open     *flags.ServerRestoreOpen
	clear    *flags.ServerRestoreClearStorage
	// objectStorageS3 is the restore read source (start, prepare).
	objectStorageS3 *flags.ObjectStorageS3
}

func newRestoreCtx() *restoreCtx {
	return &restoreCtx{
		start:           flags.NewServerRestore(),
		prepare:         flags.NewServerRestorePrepare(),
		progress:        flags.NewServerRestoreProgress(),
		abort:           flags.NewServerRestoreAbort(),
		open:            flags.NewServerRestoreOpen(),
		clear:           flags.NewServerRestoreClearStorage(),
		objectStorageS3: flags.NewObjectStorageS3(),
	}
}

// restoreServiceConfig builds the service config for one snapshot-restore
// subcommand. When --config is set, the YAML file is the single source of
// truth and every other flag is ignored, matching the scan commands; only the
// section belonging to command is populated, so the unrelated ones are skipped
// during validation. Otherwise fromFlags supplies the flag-based config.
func restoreServiceConfig(
	ctx context.Context,
	rc *runCtx,
	command config.ServerRestoreCommand,
	fromFlags func() *config.ServerRestoreServiceConfig,
) (*config.ServerRestoreServiceConfig, error) {
	path := rc.app.GetApp().ConfigFilePath
	if path == "" {
		return fromFlags(), nil
	}

	cfg, err := config.DecodeServerRestoreServiceConfig(ctx, path, command)
	if err != nil {
		return nil, fmt.Errorf("failed to load config file %s: %w", path, err)
	}

	return cfg, nil
}

// newRestoreServiceConfig builds the flag-based service config of a snapshot-restore
// subcommand, whose own configuration is the only one set in commands. awsS3 is nil for
// the subcommands served by the cluster alone.
func newRestoreServiceConfig(
	rc *runCtx,
	commands config.ServerRestoreCommands,
	awsS3 *models.AwsS3,
) *config.ServerRestoreServiceConfig {
	return config.NewServerRestoreServiceConfig(
		commands,
		rc.app.GetApp(),
		rc.aerospike.NewAerospikeConfig(),
		rc.clientPolicy.GetClientPolicy(),
		rc.secretAgent.GetSecretAgent(),
		awsS3,
	)
}

// NewRestoreCmd builds the top-level "restore" command for server-integrated
// restores.
func NewRestoreCmd(flagsRoot *flags.Root, appVersion, commitHash, buildTime string) *cobra.Command {
	rc := newRunCtx(flagsRoot, appVersion, commitHash, buildTime)

	rf := newRestoreCtx()

	cmd := &cobra.Command{
		Use:   UseSnapshotRestore,
		Short: ShortRestore,
	}

	cmd.AddCommand(
		newRestoreStartCmd(rc, rf),
		newRestorePrepareCmd(rc, rf),
		newRestoreProgressCmd(rc, rf),
		newRestoreAbortCmd(rc, rf),
		newRestoreOpenCmd(rc, rf),
		newRestoreClearStorageCmd(rc, rf),
	)

	applyRootPersistent(cmd, rc)
	setHelpRestore(cmd)

	return cmd
}

func setHelpRestore(cmd *cobra.Command) {
	cmd.SetHelpFunc(func(c *cobra.Command, _ []string) {
		printHelpHeader(flags.SectionTextUsageRestoreServer)
		printCommands(c)
	})

	usageFromHelp(cmd)
}

func newRestoreStartCmd(rc *runCtx, rf *restoreCtx) *cobra.Command {
	startFlags := rf.start.NewFlagSet()
	objectStoreFlagSet := rf.objectStorageS3.NewFlagSet()

	cmd := &cobra.Command{
		Use:   UseStart,
		Short: ShortRestoreStart,
		Long:  LongRestoreStart,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := restoreServiceConfig(cmd.Context(), rc, config.ServerRestoreCommandStart,
				func() *config.ServerRestoreServiceConfig {
					return newRestoreServiceConfig(rc,
						config.ServerRestoreCommands{Start: rf.start.GetServerRestore()},
						rf.objectStorageS3.ToAwsS3())
				})
			if err != nil {
				return err
			}

			svc, err := newService(rc, nil, cfg)
			if err != nil {
				return fmt.Errorf("failed to initialize server integrated restore: %w", err)
			}

			if err := svc.StartRestore(cmd.Context()); err != nil {
				return fmt.Errorf("failed to start server integrated restore: %w", err)
			}

			return nil
		},
	}

	common := applyCommon(cmd, rc)
	cmd.Flags().AddFlagSet(startFlags)
	cmd.Flags().AddFlagSet(objectStoreFlagSet)

	setHelpRestoreStart(cmd, startFlags, objectStoreFlagSet, common)

	return cmd
}

func setHelpRestoreStart(
	cmd *cobra.Command,
	startFS, objectStoreFS *pflag.FlagSet,
	common commonFlagSets,
) {
	doc := SubcommandDoc{
		Usage:    flags.SectionTextUsageRestoreStart,
		Sections: restoreStartHelpSections(startFS, objectStoreFS, common),
	}

	cmd.SetHelpFunc(func(_ *cobra.Command, _ []string) {
		printSubcommandHelp(doc)
	})

	usageFromHelp(cmd)
}

// restoreSubcommand describes a snapshot-restore subcommand that is configured by a
// single flag set of its own.
type restoreSubcommand struct {
	use   string
	short string
	long  string
	// usage is the line the help of the subcommand opens with.
	usage   string
	command config.ServerRestoreCommand
	flagSet *pflag.FlagSet
	// helpSections lays out the help of the subcommand.
	helpSections func(fs *pflag.FlagSet, common commonFlagSets) []HelpSection
	// commands returns the configuration of the subcommand, parsed from its flags.
	commands func() config.ServerRestoreCommands
	// awsS3 returns the object storage settings, or nil for a subcommand served by the
	// cluster alone.
	awsS3 func() *models.AwsS3
	// initErrMsg prefixes a failure to build the service. When it is empty, the failure
	// is returned as it is.
	initErrMsg string
	// run runs the subcommand with the service built from its configuration. The
	// services of abort, open and clear-storage already report what went wrong and for
	// which job, so their errors are passed through instead of being prefixed again.
	run func(ctx context.Context, svc *server.Service) error
}

// newRestoreSubcommandCmd builds the cobra command of sub.
func newRestoreSubcommandCmd(rc *runCtx, sub *restoreSubcommand) *cobra.Command {
	cmd := &cobra.Command{
		Use:   sub.use,
		Short: sub.short,
		Long:  sub.long,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := restoreServiceConfig(cmd.Context(), rc, sub.command,
				func() *config.ServerRestoreServiceConfig {
					var awsS3 *models.AwsS3
					if sub.awsS3 != nil {
						awsS3 = sub.awsS3()
					}

					return newRestoreServiceConfig(rc, sub.commands(), awsS3)
				})
			if err != nil {
				return err
			}

			svc, err := newService(rc, nil, cfg)
			if err != nil {
				if sub.initErrMsg == "" {
					return err
				}

				return fmt.Errorf("%s: %w", sub.initErrMsg, err)
			}

			return sub.run(cmd.Context(), svc)
		},
	}

	common := applyCommon(cmd, rc)
	cmd.Flags().AddFlagSet(sub.flagSet)

	doc := SubcommandDoc{
		Usage:    sub.usage,
		Sections: sub.helpSections(sub.flagSet, common),
	}

	cmd.SetHelpFunc(func(_ *cobra.Command, _ []string) {
		printSubcommandHelp(doc)
	})

	usageFromHelp(cmd)

	return cmd
}

func newRestorePrepareCmd(rc *runCtx, rf *restoreCtx) *cobra.Command {
	return newRestoreSubcommandCmd(rc, &restoreSubcommand{
		use:          UsePrepare,
		short:        ShortRestorePrepare,
		long:         LongRestorePrepare,
		usage:        flags.SectionTextUsageRestorePrepare,
		command:      config.ServerRestoreCommandPrepare,
		flagSet:      rf.prepare.NewFlagSet(),
		helpSections: restorePrepareHelpSections,
		commands: func() config.ServerRestoreCommands {
			return config.ServerRestoreCommands{Prepare: rf.prepare.GetServerRestorePrepare()}
		},
		awsS3: rf.objectStorageS3.ToAwsS3,
		run: func(ctx context.Context, svc *server.Service) error {
			if err := svc.PrepareRestore(ctx); err != nil {
				return fmt.Errorf("server side restore preparation failed: %w", err)
			}

			return nil
		},
	})
}

func newRestoreProgressCmd(rc *runCtx, rf *restoreCtx) *cobra.Command {
	return newRestoreSubcommandCmd(rc, &restoreSubcommand{
		use:          UseProgress,
		short:        ShortRestoreProgress,
		long:         LongRestoreProgress,
		usage:        flags.SectionTextUsageRestoreProgress,
		command:      config.ServerRestoreCommandProgress,
		flagSet:      rf.progress.NewFlagSet(),
		helpSections: restoreProgressHelpSections,
		commands: func() config.ServerRestoreCommands {
			return config.ServerRestoreCommands{Progress: rf.progress.GetServerRestoreProgress()}
		},
		awsS3: rf.objectStorageS3.ToAwsS3,
		run: func(ctx context.Context, svc *server.Service) error {
			if err := svc.RestoreProgress(ctx); err != nil {
				return fmt.Errorf("failed to get server side restore progress: %w", err)
			}

			return nil
		},
	})
}

func newRestoreAbortCmd(rc *runCtx, rf *restoreCtx) *cobra.Command {
	return newRestoreSubcommandCmd(rc, &restoreSubcommand{
		use:          UseAbort,
		short:        ShortRestoreAbort,
		long:         LongRestoreAbort,
		usage:        flags.SectionTextUsageRestoreAbort,
		command:      config.ServerRestoreCommandAbort,
		flagSet:      rf.abort.NewFlagSet(),
		helpSections: restoreAbortHelpSections,
		commands: func() config.ServerRestoreCommands {
			return config.ServerRestoreCommands{Abort: rf.abort.GetServerRestoreAbort()}
		},
		initErrMsg: "failed to initialize restore abort",
		run: func(ctx context.Context, svc *server.Service) error {
			return svc.AbortRestore(ctx)
		},
	})
}

func newRestoreOpenCmd(rc *runCtx, rf *restoreCtx) *cobra.Command {
	return newRestoreSubcommandCmd(rc, &restoreSubcommand{
		use:          UseOpen,
		short:        ShortRestoreOpen,
		long:         LongRestoreOpen,
		usage:        flags.SectionTextUsageRestoreOpen,
		command:      config.ServerRestoreCommandOpen,
		flagSet:      rf.open.NewFlagSet(),
		helpSections: restoreOpenHelpSections,
		commands: func() config.ServerRestoreCommands {
			return config.ServerRestoreCommands{Open: rf.open.GetServerRestoreOpen()}
		},
		initErrMsg: "failed to initialize restore open",
		run: func(ctx context.Context, svc *server.Service) error {
			return svc.OpenRestore(ctx)
		},
	})
}

func newRestoreClearStorageCmd(rc *runCtx, rf *restoreCtx) *cobra.Command {
	return newRestoreSubcommandCmd(rc, &restoreSubcommand{
		use:          UseClearStorage,
		short:        ShortRestoreClear,
		long:         LongRestoreClear,
		usage:        flags.SectionTextUsageRestoreClear,
		command:      config.ServerRestoreCommandClearStorage,
		flagSet:      rf.clear.NewFlagSet(),
		helpSections: restoreClearStorageHelpSections,
		commands: func() config.ServerRestoreCommands {
			return config.ServerRestoreCommands{ClearStorage: rf.clear.GetServerRestoreClearStorage()}
		},
		initErrMsg: "failed to initialize restore clear storage",
		run: func(ctx context.Context, svc *server.Service) error {
			return svc.ClearRestoreStorage(ctx)
		},
	})
}
