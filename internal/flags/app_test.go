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

package flags

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestApp_NewFlagSet(t *testing.T) {
	t.Parallel()

	app := NewApp()

	flagSet := app.NewFlagSet()

	args := []string{
		"--verbose",
		"--log-level", "error",
		"--log-json",
		"--log-file", "log.txt",
		"--config", "config.yaml",
	}

	err := flagSet.Parse(args)
	require.NoError(t, err)

	assert.False(t, app.Help, "Help flag should default to false")
	assert.True(t, app.Verbose, "Verbose flag should be true when set")
	assert.Equal(t, "error", app.LogLevel, "Log level flag should be error")
	assert.True(t, app.LogJSON, "Log JSON flag should be true when set")
	assert.Equal(t, "config.yaml", app.ConfigFilePath, "Config flag should be config.yaml")
	assert.Equal(t, "log.txt", app.LogFile, "Log file flag should be config.yaml")
}

func TestApp_NewFlagSet_DefaultValues(t *testing.T) {
	t.Parallel()

	app := NewApp()

	flagSet := app.NewFlagSet()

	err := flagSet.Parse([]string{})
	require.NoError(t, err)

	assert.False(t, app.Help, "Help flag should default to false")
	assert.False(t, app.Verbose, "Verbose flag should default to false")
	assert.Equal(t, "debug", app.LogLevel, "Log level flag should default be debug")
	assert.False(t, app.LogJSON, "Log JSON flag should default to false")
	assert.Empty(t, app.ConfigFilePath, "Config flag should default be empty string")
	assert.Empty(t, app.LogFile, "Log file flag should default be empty string")
}
