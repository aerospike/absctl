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
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// backupCtx groups the storage flag holders shared by the "server backup"
// subcommands.
type backupCtx struct {
	start    *flags.ServerBackup
	list     *flags.ServerBackupList
	validate *flags.ServerBackupValidate
	progress *flags.ServerBackupProgress

	// aws is the listing/validation source (list, validate, progress).
	aws *flags.AwsS3
	// objectStorageS3 is the backup write target (start).
	objectStorageS3 *flags.ObjectStorageS3
}

func newBackupCtx() *backupCtx {
	return &backupCtx{
		start:           flags.NewServerBackup(),
		list:            flags.NewServerBackupList(),
		validate:        flags.NewServerBackupValidate(),
		progress:        flags.NewServerBackupProgress(),
		aws:             flags.NewAwsS3(flags.OperationRestore),
		objectStorageS3: flags.NewObjectStorageS3(),
	}
}

// backupServiceConfig builds the service config for one snapshot-backup
// subcommand. When --config is set, the YAML file is the single source of
// truth and every other flag is ignored, matching the scan commands; only the
// section belonging to command is populated, so the unrelated ones are skipped
// during validation. Otherwise fromFlags supplies the flag-based config.
func backupServiceConfig(
	ctx context.Context,
	rc *runCtx,
	command config.ServerBackupCommand,
	fromFlags func() *config.ServerBackupServiceConfig,
) (*config.ServerBackupServiceConfig, error) {
	path := rc.app.GetApp().ConfigFilePath
	if path == "" {
		return fromFlags(), nil
	}

	cfg, err := config.DecodeServerBackupServiceConfig(ctx, path, command)
	if err != nil {
		return nil, fmt.Errorf("failed to load config file %s: %w", path, err)
	}

	return cfg, nil
}

// NewBackupCmd builds the top-level "backup" command for server-integrated
// backups.
func NewBackupCmd(flagsRoot *flags.Root, appVersion, commitHash, buildTime string) *cobra.Command {
	rc := newRunCtx(flagsRoot, appVersion, commitHash, buildTime)
	bc := newBackupCtx()

	cmd := &cobra.Command{
		Use:   UseSnapshotBackup,
		Short: ShortBackup,
	}

	cmd.AddCommand(
		newBackupStartCmd(rc, bc),
		newBackupListCmd(rc, bc),
		newBackupProgressCmd(rc, bc),
		newBackupValidateCmd(rc, bc),
	)

	applyRootPersistent(cmd, rc)
	setHelpBackup(cmd)

	return cmd
}

func setHelpBackup(cmd *cobra.Command) {
	cmd.SetHelpFunc(func(c *cobra.Command, _ []string) {
		printHelpHeader(flags.SectionTextUsageBackupServer)
		printCommands(c)
	})

	usageFromHelp(cmd)
}

func newBackupStartCmd(rc *runCtx, bf *backupCtx) *cobra.Command {
	startFlagSet := bf.start.NewFlagSet()
	objectStoreFlagSet := bf.objectStorageS3.NewFlagSet()

	cmd := &cobra.Command{
		Use:   UseStart,
		Short: ShortBackupStart,
		Long:  LongBackupStart,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := backupServiceConfig(cmd.Context(), rc, config.ServerBackupCommandStart,
				func() *config.ServerBackupServiceConfig {
					return config.NewServerBackupServiceConfig(
						bf.start.GetServerBackup(),
						nil, nil, nil,
						rc.app.GetApp(),
						rc.aerospike.NewAerospikeConfig(),
						rc.clientPolicy.GetClientPolicy(),
						rc.secretAgent.GetSecretAgent(),
						bf.objectStorageS3.ToAwsS3(),
					)
				})
			if err != nil {
				return err
			}

			svc, err := newService(rc, cfg, nil)
			if err != nil {
				return fmt.Errorf("failed to initialize server integrated backup: %w", err)
			}

			if err := svc.StartBackup(cmd.Context()); err != nil {
				return fmt.Errorf("failed to start server integrated backup: %w", err)
			}

			return nil
		},
	}

	common := applyCommon(cmd, rc)
	cmd.Flags().AddFlagSet(startFlagSet)
	cmd.Flags().AddFlagSet(objectStoreFlagSet)

	setHelpBackupStart(cmd, startFlagSet, objectStoreFlagSet, common)

	return cmd
}

func setHelpBackupStart(
	cmd *cobra.Command,
	startFS, objectStoreFS *pflag.FlagSet,
	common commonFlagSets,
) {
	doc := SubcommandDoc{
		Usage:    flags.SectionTextUsageBackupStart,
		Sections: backupStartHelpSections(startFS, objectStoreFS, common),
	}

	cmd.SetHelpFunc(func(_ *cobra.Command, _ []string) {
		printSubcommandHelp(doc)
	})

	usageFromHelp(cmd)
}

func newBackupListCmd(rc *runCtx, bf *backupCtx) *cobra.Command {
	awsFlagSet := bf.aws.NewFlagSet()
	listFlagSet := bf.list.NewFlagSet()

	cmd := &cobra.Command{
		Use:   UseList,
		Short: ShortBackupList,
		Long:  LongBackupList,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := backupServiceConfig(cmd.Context(), rc, config.ServerBackupCommandList,
				func() *config.ServerBackupServiceConfig {
					return config.NewServerBackupServiceConfig(
						nil,
						bf.list.GetServerBackupList(),
						nil, nil,
						rc.app.GetApp(),
						rc.aerospike.NewAerospikeConfig(),
						rc.clientPolicy.GetClientPolicy(),
						rc.secretAgent.GetSecretAgent(),
						bf.aws.GetAwsS3(),
					)
				})
			if err != nil {
				return err
			}

			svc, err := newService(rc, cfg, nil)
			if err != nil {
				return fmt.Errorf("failed to initialize listing backups: %w", err)
			}

			if err := svc.ListBackups(cmd.Context()); err != nil {
				return fmt.Errorf("failed to list backups: %w", err)
			}

			return nil
		},
	}

	cmd.Flags().AddFlagSet(listFlagSet)
	cmd.Flags().AddFlagSet(awsFlagSet)

	setHelpBackupList(cmd, rc.app.NewFlagSet(), listFlagSet, awsFlagSet)

	return cmd
}

func setHelpBackupList(cmd *cobra.Command, appFS, listFS, awsFS *pflag.FlagSet) {
	doc := SubcommandDoc{
		Usage:    flags.SectionTextUsageBackupList,
		Sections: backupListHelpSections(appFS, listFS, awsFS),
	}

	cmd.SetHelpFunc(func(_ *cobra.Command, _ []string) {
		printSubcommandHelp(doc)
	})

	usageFromHelp(cmd)
}

func newBackupProgressCmd(rc *runCtx, bf *backupCtx) *cobra.Command {
	awsFlagSet := bf.aws.NewFlagSet()
	progressFlagSet := bf.progress.NewFlagSet()

	cmd := &cobra.Command{
		Use:   UseProgress,
		Short: ShortBackupProgress,
		Long:  LongBackupProgress,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := backupServiceConfig(cmd.Context(), rc, config.ServerBackupCommandProgress,
				func() *config.ServerBackupServiceConfig {
					return config.NewServerBackupServiceConfig(
						nil, nil, nil,
						bf.progress.GetServerBackupProgress(),
						rc.app.GetApp(),
						rc.aerospike.NewAerospikeConfig(),
						rc.clientPolicy.GetClientPolicy(),
						rc.secretAgent.GetSecretAgent(),
						bf.aws.GetAwsS3(),
					)
				})
			if err != nil {
				return err
			}

			svc, err := newService(rc, cfg, nil)
			if err != nil {
				return fmt.Errorf("failed to initialize backup progress: %w", err)
			}

			if err := svc.BackupProgress(cmd.Context()); err != nil {
				return fmt.Errorf("failed to get backup progress: %w", err)
			}

			return nil
		},
	}

	cmd.Flags().AddFlagSet(progressFlagSet)
	cmd.Flags().AddFlagSet(awsFlagSet)

	common := applyCommon(cmd, rc)

	setHelpBackupProgress(cmd, common, progressFlagSet, awsFlagSet)

	return cmd
}

func setHelpBackupProgress(cmd *cobra.Command, common commonFlagSets, progressFs, awsFs *pflag.FlagSet) {
	doc := SubcommandDoc{
		Usage:    flags.SectionTextUsageBackupProgress,
		Sections: backupProgressHelpSections(common, progressFs, awsFs),
	}

	cmd.SetHelpFunc(func(_ *cobra.Command, _ []string) {
		printSubcommandHelp(doc)
	})

	usageFromHelp(cmd)
}

func newBackupValidateCmd(rc *runCtx, bf *backupCtx) *cobra.Command {
	validationFlagSet := bf.validate.NewFlagSet()
	awsFlagSet := bf.aws.NewFlagSet()

	cmd := &cobra.Command{
		Use:   UseValidate,
		Short: ShortBackupValidate,
		Long:  LongBackupValidate,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := backupServiceConfig(cmd.Context(), rc, config.ServerBackupCommandValidate,
				func() *config.ServerBackupServiceConfig {
					return config.NewServerBackupServiceConfig(
						nil, nil,
						bf.validate.GetServerBackupValidate(),
						nil,
						rc.app.GetApp(),
						rc.aerospike.NewAerospikeConfig(),
						rc.clientPolicy.GetClientPolicy(),
						rc.secretAgent.GetSecretAgent(),
						bf.aws.GetAwsS3(),
					)
				})
			if err != nil {
				return err
			}

			svc, err := newService(rc, cfg, nil)
			if err != nil {
				return fmt.Errorf("failed to initialize backup validation: %w", err)
			}

			if err := svc.BackupValidate(cmd.Context()); err != nil {
				return fmt.Errorf("failed to validate backup: %w", err)
			}

			return nil
		},
	}

	cmd.Flags().AddFlagSet(validationFlagSet)
	cmd.Flags().AddFlagSet(awsFlagSet)

	setHelpBackupValidate(cmd, rc.app.NewFlagSet(), validationFlagSet, awsFlagSet)

	return cmd
}

func setHelpBackupValidate(cmd *cobra.Command, appFS, validationFS, awsFS *pflag.FlagSet) {
	doc := SubcommandDoc{
		Usage:    flags.SectionTextUsageValidate,
		Sections: backupValidateHelpSections(appFS, validationFS, awsFS),
	}

	cmd.SetHelpFunc(func(_ *cobra.Command, _ []string) {
		printSubcommandHelp(doc)
	})

	usageFromHelp(cmd)
}
