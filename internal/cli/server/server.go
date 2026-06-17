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
	backupWelcomeMessage = "Welcome to the Aerospike backup CLI tool!"

	useBackup   = "backup"
	useRestore  = "restore"
	useStart    = "start"
	useList     = "list"
	useProgress = "progress"
	usePrepare  = "prepare"
	useValidate = "validate"
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

// newRunCtx constructs a runCtx with freshly-allocated flag holders and a
// default logger. Each top-level command (backup, restore) gets its own runCtx.
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

// applyCommon attaches the persistent flag sets and pre/post-run lifecycle
// hooks shared by the server-integrated backup and restore commands. The
// returned flag sets are passed to setParentHelp so they appear in the help
// output.
func applyCommon(cmd *cobra.Command, rc *runCtx) []*pflag.FlagSet {
	cmd.PersistentFlags().SortFlags = false
	cmd.SilenceUsage = true

	appFlagSet := rc.app.NewFlagSet()
	aerospikeFlagSet := rc.aerospike.NewFlagSet(asFlags.DefaultWrapHelpString)
	flags.WrapFlagsForSecrets(aerospikeFlagSet)
	clientPolicyFlagSet := rc.clientPolicy.NewFlagSet()
	secretAgentFlagSet := rc.secretAgent.NewFlagSet()

	cmd.Flags().AddFlagSet(aerospikeFlagSet)
	cmd.Flags().AddFlagSet(clientPolicyFlagSet)
	cmd.Flags().AddFlagSet(secretAgentFlagSet)

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

	return []*pflag.FlagSet{appFlagSet, aerospikeFlagSet, clientPolicyFlagSet, secretAgentFlagSet}
}
