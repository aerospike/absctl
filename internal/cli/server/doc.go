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
	"github.com/aerospike/absctl/internal/flags"
	asFlags "github.com/aerospike/tools-common-go/flags"
	"github.com/spf13/pflag"
)

// HelpSection groups a section title with one or more flag sets rendered under it.
type HelpSection struct {
	Title    string
	FlagSets []*pflag.FlagSet
}

// SubcommandDoc describes one server subcommand for CLI help and generated docs.
type SubcommandDoc struct {
	Name     string
	Short    string
	Long     string
	Usage    string
	Sections []HelpSection
}

func newCommonFlagSets(rc *runCtx) commonFlagSets {
	fs := commonFlagSets{
		app:          rc.app.NewFlagSet(),
		aerospike:    rc.aerospike.NewFlagSet(asFlags.DefaultWrapHelpString),
		clientPolicy: rc.clientPolicy.NewFlagSet(),
		secretAgent:  rc.secretAgent.NewFlagSet(),
	}
	flags.WrapFlagsForSecrets(fs.aerospike)

	return fs
}

func backupStartHelpSections(startFS, objectStoreFS *pflag.FlagSet, common commonFlagSets) []HelpSection {
	return []HelpSection{
		{Title: flags.SectionTextGeneral, FlagSets: []*pflag.FlagSet{common.app}},
		{Title: flags.SectionTextAerospike, FlagSets: []*pflag.FlagSet{common.aerospike, common.clientPolicy}},
		{Title: flags.SectionTextBackup, FlagSets: []*pflag.FlagSet{startFS}},
		{Title: flags.SectionTextAWS, FlagSets: []*pflag.FlagSet{objectStoreFS}},
		{Title: flags.SectionTextSecretAgentBackup, FlagSets: []*pflag.FlagSet{common.secretAgent}},
	}
}

func backupListHelpSections(listFS, awsFS *pflag.FlagSet) []HelpSection {
	return []HelpSection{
		{Title: flags.SectionTextBackup, FlagSets: []*pflag.FlagSet{listFS}},
		{Title: flags.SectionTextAWS, FlagSets: []*pflag.FlagSet{awsFS}},
	}
}

func backupProgressHelpSections(common commonFlagSets, progressFs, awsFS *pflag.FlagSet) []HelpSection {
	return []HelpSection{
		{Title: flags.SectionTextGeneral, FlagSets: []*pflag.FlagSet{common.app}},
		{Title: flags.SectionTextAerospike, FlagSets: []*pflag.FlagSet{common.aerospike, common.clientPolicy}},
		{Title: flags.SectionTextBackup, FlagSets: []*pflag.FlagSet{progressFs}},
		{Title: flags.SectionTextAWS, FlagSets: []*pflag.FlagSet{awsFS}},
		{Title: flags.SectionTextSecretAgentBackup, FlagSets: []*pflag.FlagSet{common.secretAgent}},
	}
}

func backupValidateHelpSections(validationFS, awsFS *pflag.FlagSet) []HelpSection {
	return []HelpSection{
		{Title: flags.SectionTextBackup, FlagSets: []*pflag.FlagSet{validationFS}},
		{Title: flags.SectionTextAWS, FlagSets: []*pflag.FlagSet{awsFS}},
	}
}

func restoreStartHelpSections(startFS, objectStoreFS *pflag.FlagSet, common commonFlagSets) []HelpSection {
	return []HelpSection{
		{Title: flags.SectionTextGeneral, FlagSets: []*pflag.FlagSet{common.app}},
		{Title: flags.SectionTextAerospike, FlagSets: []*pflag.FlagSet{common.aerospike, common.clientPolicy}},
		{Title: flags.SectionTextRestore, FlagSets: []*pflag.FlagSet{startFS}},
		{Title: flags.SectionTextAWS, FlagSets: []*pflag.FlagSet{objectStoreFS}},
		{Title: flags.SectionTextSecretAgentBackup, FlagSets: []*pflag.FlagSet{common.secretAgent}},
	}
}

func restorePrepareHelpSections(prepareFS *pflag.FlagSet, common commonFlagSets) []HelpSection {
	return []HelpSection{
		{Title: flags.SectionTextGeneral, FlagSets: []*pflag.FlagSet{common.app}},
		{Title: flags.SectionTextAerospike, FlagSets: []*pflag.FlagSet{common.aerospike, common.clientPolicy}},
		{Title: flags.SectionTextRestore, FlagSets: []*pflag.FlagSet{prepareFS}},
		{Title: flags.SectionTextSecretAgentBackup, FlagSets: []*pflag.FlagSet{common.secretAgent}},
	}
}

func restoreProgressHelpSections(prepareFS *pflag.FlagSet, common commonFlagSets) []HelpSection {
	return []HelpSection{
		{Title: flags.SectionTextGeneral, FlagSets: []*pflag.FlagSet{common.app}},
		{Title: flags.SectionTextAerospike, FlagSets: []*pflag.FlagSet{common.aerospike, common.clientPolicy}},
		{Title: flags.SectionTextRestore, FlagSets: []*pflag.FlagSet{prepareFS}},
		{Title: flags.SectionTextSecretAgentBackup, FlagSets: []*pflag.FlagSet{common.secretAgent}},
	}
}

// BuildBackupSubcommandDocs returns documentation for every server backup subcommand.
func BuildBackupSubcommandDocs() []SubcommandDoc {
	rc := newRunCtx(flags.NewRoot(), "", "", "")
	bc := newBackupCtx()
	common := newCommonFlagSets(rc)

	startFS := bc.start.NewFlagSet()
	objectStoreFS := bc.objectStorageS3.NewFlagSet()
	listFS := bc.list.NewFlagSet()
	awsFS := bc.aws.NewFlagSet()
	validationFS := bc.validate.NewFlagSet()
	progressFS := bc.progress.NewFlagSet()

	return []SubcommandDoc{
		{
			Name:     UseStart,
			Short:    ShortBackupStart,
			Long:     LongBackupStart,
			Usage:    flags.SectionTextUsageBackupStart,
			Sections: backupStartHelpSections(startFS, objectStoreFS, common),
		},
		{
			Name:     UseList,
			Short:    ShortBackupList,
			Long:     LongBackupList,
			Usage:    flags.SectionTextUsageBackupList,
			Sections: backupListHelpSections(listFS, awsFS),
		},
		{
			Name:     UseProgress,
			Short:    ShortBackupProgress,
			Long:     LongBackupProgress,
			Usage:    flags.SectionTextUsageBackupProgress,
			Sections: backupProgressHelpSections(common, progressFS, awsFS),
		},
		{
			Name:     UseValidate,
			Short:    ShortBackupValidate,
			Long:     LongBackupValidate,
			Usage:    flags.SectionTextUsageValidate,
			Sections: backupValidateHelpSections(validationFS, awsFS),
		},
	}
}

// BuildRestoreSubcommandDocs returns documentation for every server restore subcommand.
func BuildRestoreSubcommandDocs() []SubcommandDoc {
	rc := newRunCtx(flags.NewRoot(), "", "", "")
	rf := newRestoreCtx()
	common := newCommonFlagSets(rc)

	startFS := rf.start.NewFlagSet()
	objectStoreFS := rf.objectStorageS3.NewFlagSet()
	prepareFS := rf.prepare.NewFlagSet()
	progressFS := rf.progress.NewFlagSet()

	return []SubcommandDoc{
		{
			Name:     UseStart,
			Short:    ShortRestoreStart,
			Long:     LongRestoreStart,
			Usage:    flags.SectionTextUsageRestoreStart,
			Sections: restoreStartHelpSections(startFS, objectStoreFS, common),
		},
		{
			Name:     UsePrepare,
			Short:    ShortRestorePrepare,
			Long:     LongRestorePrepare,
			Usage:    flags.SectionTextUsageRestorePrepare,
			Sections: restorePrepareHelpSections(prepareFS, common),
		},
		{
			Name:     UseProgress,
			Short:    ShortRestoreProgress,
			Long:     LongRestoreProgress,
			Usage:    flags.SectionTextUsageRestoreProgress,
			Sections: restoreProgressHelpSections(progressFS, common),
		},
	}
}
