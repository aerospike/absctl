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

package subcmd

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/aerospike/absctl/internal/flags"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

var (
	// ErrFlagsWithConfig is returned when --config is combined with flags that the
	// configuration file would silently override.
	ErrFlagsWithConfig = errors.New("flags cannot be combined with --config")
	// ErrEmptyConfigPath is returned when --config is given with an empty value.
	ErrEmptyConfigPath = errors.New("config path is empty")
)

// RejectFlagsWithConfig fails when --config is given with an empty value, or is
// combined with any flag outside flags.ConfigCompatibleFlags. Without this check such
// flags would be silently ignored, for example a backup requested with --encrypt would
// be written in plaintext.
func RejectFlagsWithConfig(cmd *cobra.Command) error {
	if !cmd.Flags().Changed(flags.FlagConfig) {
		return nil
	}

	if err := RejectEmptyConfig(cmd); err != nil {
		return err
	}

	allowed := flags.ConfigCompatibleFlags()

	conflicts := changedFlagNames(cmd.Flags(), func(name string) bool {
		return slices.Contains(allowed, name)
	})
	if len(conflicts) > 0 {
		return fmt.Errorf("%w: %s", ErrFlagsWithConfig, strings.Join(conflicts, ", "))
	}

	return nil
}

// RejectEmptyConfig fails when --config is given with an empty value: no file would be
// read, although the user asked for one.
func RejectEmptyConfig(cmd *cobra.Command) error {
	if !cmd.Flags().Changed(flags.FlagConfig) {
		return nil
	}

	path, err := cmd.Flags().GetString(flags.FlagConfig)
	if err != nil {
		return fmt.Errorf("failed to read --%s: %w", flags.FlagConfig, err)
	}

	if path == "" {
		return ErrEmptyConfigPath
	}

	return nil
}

// changedFlagNames returns the names of the flags set on the command line, prefixed
// with "--", except those for which skip returns true.
func changedFlagNames(fs *pflag.FlagSet, skip func(name string) bool) []string {
	var names []string

	fs.Visit(func(f *pflag.Flag) {
		if !skip(f.Name) {
			names = append(names, "--"+f.Name)
		}
	})

	return names
}
