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

	"github.com/aerospike/absctl/internal/config"
	"github.com/aerospike/absctl/internal/flags"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// restoreCtx groups the storage flag holders shared by the "server restore"
// subcommands.
type restoreCtx struct {
	start    *flags.ServerRestore
	prepare  *flags.ServerRestorePrepare
	progress *flags.ServerRestoreProgress
	// objectStorageS3 is the restore read source (start, prepare).
	objectStorageS3 *flags.ObjectStorageS3
}

func newRestoreCtx() *restoreCtx {
	return &restoreCtx{
		start:           flags.NewServerRestore(),
		prepare:         flags.NewServerRestorePrepare(),
		progress:        flags.NewServerRestoreProgress(),
		objectStorageS3: flags.NewObjectStorageS3(),
	}
}

// NewRestoreCmd builds the top-level "restore" command for server-integrated
// restores.
func NewRestoreCmd(flagsRoot *flags.Root, appVersion, commitHash, buildTime string) *cobra.Command {
	rc := newRunCtx(flagsRoot, appVersion, commitHash, buildTime)

	rf := newRestoreCtx()

	cmd := &cobra.Command{
		Use:   UseRestore,
		Short: ShortRestore,
	}

	cmd.AddCommand(
		newRestoreStartCmd(rc, rf),
		newRestorePrepareCmd(rc, rf),
		newRestoreProgressCmd(rc, rf),
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

//nolint:dupl // Commands are all look the same.
func newRestoreStartCmd(rc *runCtx, rf *restoreCtx) *cobra.Command {
	startFlags := rf.start.NewFlagSet()
	objectStoreFlagSet := rf.objectStorageS3.NewFlagSet()

	cmd := &cobra.Command{
		Use:   UseStart,
		Short: ShortRestoreStart,
		Long:  LongRestoreStart,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg := config.NewServerRestoreServiceConfig(
				rf.start.GetServerRestore(),
				nil,
				nil,
				rc.app.GetApp(),
				rc.aerospike.NewAerospikeConfig(),
				rc.clientPolicy.GetClientPolicy(),
				rc.secretAgent.GetSecretAgent(),
				rf.objectStorageS3.ToAwsS3(),
			)

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

func newRestorePrepareCmd(rc *runCtx, rf *restoreCtx) *cobra.Command {
	prepareFlags := rf.prepare.NewFlagSet()

	cmd := &cobra.Command{
		Use:   UsePrepare,
		Short: ShortRestorePrepare,
		Long:  LongRestorePrepare,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg := config.NewServerRestoreServiceConfig(
				nil,
				rf.prepare.GetServerRestorePrepare(),
				nil,
				rc.app.GetApp(),
				rc.aerospike.NewAerospikeConfig(),
				rc.clientPolicy.GetClientPolicy(),
				rc.secretAgent.GetSecretAgent(),
				rf.objectStorageS3.ToAwsS3(),
			)

			svc, err := newService(rc, nil, cfg)
			if err != nil {
				return err
			}

			if err := svc.PrepareRestore(cmd.Context()); err != nil {
				return fmt.Errorf("server side restore preparation failed: %w", err)
			}

			return nil
		},
	}

	common := applyCommon(cmd, rc)
	cmd.Flags().AddFlagSet(prepareFlags)

	setHelpRestorePrepare(cmd, prepareFlags, common)

	return cmd
}

func setHelpRestorePrepare(cmd *cobra.Command, prepareFS *pflag.FlagSet, common commonFlagSets) {
	doc := SubcommandDoc{
		Usage:    flags.SectionTextUsageRestorePrepare,
		Sections: restorePrepareHelpSections(prepareFS, common),
	}

	cmd.SetHelpFunc(func(_ *cobra.Command, _ []string) {
		printSubcommandHelp(doc)
	})

	usageFromHelp(cmd)
}

func newRestoreProgressCmd(rc *runCtx, rf *restoreCtx) *cobra.Command {
	progressFlags := rf.progress.NewFlagSet()

	cmd := &cobra.Command{
		Use:   UseProgress,
		Short: ShortRestoreProgress,
		Long:  LongRestoreProgress,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg := config.NewServerRestoreServiceConfig(
				nil,
				nil,
				rf.progress.GetServerRestoreProgress(),
				rc.app.GetApp(),
				rc.aerospike.NewAerospikeConfig(),
				rc.clientPolicy.GetClientPolicy(),
				rc.secretAgent.GetSecretAgent(),
				rf.objectStorageS3.ToAwsS3(),
			)

			svc, err := newService(rc, nil, cfg)
			if err != nil {
				return err
			}

			if err := svc.RestoreProgress(cmd.Context()); err != nil {
				return fmt.Errorf("server side restore preparation failed: %w", err)
			}

			return nil
		},
	}

	common := applyCommon(cmd, rc)
	cmd.Flags().AddFlagSet(progressFlags)

	setHelpRestoreProgress(cmd, progressFlags, common)

	return cmd
}

func setHelpRestoreProgress(cmd *cobra.Command, prepareFS *pflag.FlagSet, common commonFlagSets) {
	doc := SubcommandDoc{
		Usage:    flags.SectionTextUsageRestoreProgress,
		Sections: restoreProgressHelpSections(prepareFS, common),
	}

	cmd.SetHelpFunc(func(_ *cobra.Command, _ []string) {
		printSubcommandHelp(doc)
	})

	usageFromHelp(cmd)
}
