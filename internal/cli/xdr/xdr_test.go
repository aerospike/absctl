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

package xdr

import (
	"io"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/aerospike/absctl/internal/flags"
	asFlags "github.com/aerospike/tools-common-go/flags"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var captureStdoutMu sync.Mutex

func captureStdout(t *testing.T, fn func()) string {
	t.Helper()

	captureStdoutMu.Lock()
	defer captureStdoutMu.Unlock()

	r, w, err := os.Pipe()
	require.NoError(t, err)

	orig := os.Stdout
	os.Stdout = w

	fn()

	require.NoError(t, w.Close())
	os.Stdout = orig

	out, err := io.ReadAll(r)
	require.NoError(t, err)

	return string(out)
}

func newTestCmd() *Cmd {
	c := &Cmd{
		flagsApp:          flags.NewApp(),
		flagsAerospike:    asFlags.NewDefaultAerospikeFlags(),
		flagsClientPolicy: flags.NewClientPolicy(),
		flagsCompression:  flags.NewCompression(flags.OperationBackup),
		flagsEncryption:   flags.NewEncryption(flags.OperationBackup),
		flagsSecretAgent:  flags.NewSecretAgent(),
		flagsAws:          flags.NewAwsS3(flags.OperationBackup),
		flagsGcp:          flags.NewGcpStorage(flags.OperationBackup),
		flagsAzure:        flags.NewAzureBlob(flags.OperationBackup),
		flagsLocal:        flags.NewLocal(flags.OperationBackup),
	}

	return c
}

func TestNewCmd_Structure(t *testing.T) {
	t.Parallel()

	c := newTestCmd()
	cmd := NewCmd(
		c.flagsApp,
		c.flagsAerospike,
		c.flagsClientPolicy,
		c.flagsCompression,
		c.flagsEncryption,
		c.flagsSecretAgent,
		c.flagsAws,
		c.flagsGcp,
		c.flagsAzure,
		c.flagsLocal,
	)

	require.NotNil(t, cmd)
	assert.Equal(t, "xdr", cmd.Use)
	assert.Equal(t, "Aerospike XDR backup CLI tool", cmd.Short)
	assert.Equal(t, welcomeMessage, cmd.Long)
	assert.NotNil(t, cmd.RunE)
}

func TestNewCmd_HasXDRFlags(t *testing.T) {
	t.Parallel()

	c := newTestCmd()
	cmd := NewCmd(
		c.flagsApp,
		c.flagsAerospike,
		c.flagsClientPolicy,
		c.flagsCompression,
		c.flagsEncryption,
		c.flagsSecretAgent,
		c.flagsAws,
		c.flagsGcp,
		c.flagsAzure,
		c.flagsLocal,
	)

	expected := []string{
		"namespace",
		"directory",
		"dc",
		"local-address",
		"local-port",
		"rewind",
	}

	for _, name := range expected {
		assert.NotNilf(t, cmd.Flag(name), "expected xdr command to define --%s", name)
	}
}

// TestRun_NoFlagsShowsHelp covers the "no flags passed" branch in Cmd.run that
// invokes cmd.Help() and returns nil.
func TestRun_NoFlagsShowsHelp(t *testing.T) {
	t.Parallel()

	c := newTestCmd()
	cmd := NewCmd(
		c.flagsApp,
		c.flagsAerospike,
		c.flagsClientPolicy,
		c.flagsCompression,
		c.flagsEncryption,
		c.flagsSecretAgent,
		c.flagsAws,
		c.flagsGcp,
		c.flagsAzure,
		c.flagsLocal,
	)

	// Locate the Cmd struct that backs this cobra.Command via Closure: the
	// RunE is bound to a *Cmd whose run method we want to exercise.
	// We can call run() directly by re-binding: easier to call cmd.RunE.
	out := captureStdout(t, func() {
		require.NoError(t, cmd.RunE(cmd, nil))
	})

	assert.Contains(t, out, welcomeMessage)
}

func TestNewHelpFunction_Output(t *testing.T) {
	t.Parallel()

	c := newTestCmd()
	xdrFS := c.flagsBackupXDR // nil at this point: set when NewCmd is called.
	if xdrFS == nil {
		c.flagsBackupXDR = flags.NewBackupXDR()
	}

	helpFn := newHelpFunction(c.flagsBackupXDR.NewFlagSet())

	out := captureStdout(t, helpFn)
	assert.Contains(t, out, welcomeMessage)
	assert.Contains(t, out, strings.Repeat("-", len(welcomeMessage)))
	assert.Contains(t, out, "XDR Backup Flags:")
}
