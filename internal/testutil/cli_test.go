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

package testutil

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/aerospike/absctl/internal/logging"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testUse       = "tool"
	testStdoutMsg = "written to stdout"
	testStderrMsg = "written to stderr"
	testCobraOut  = "cobra out"
	testCobraErr  = "cobra err"
	testBuildMsg  = "written while building"
	testNoArgs    = "args=0"
	testFlagFail  = "fail"
	testArgFail   = "--" + testFlagFail

	// envFailureHelper tells TestCaptureOutput_FailureHelper how to end.
	envFailureHelper = "ABSCTL_TEST_FAILURE_HELPER"
	// envRaceOptions holds the options of the race detector.
	envRaceOptions   = "GORACE"
	helperPass       = "pass"
	helperFail       = "fail"
	helperFailEmpty  = "fail-empty"
	helperFailRunCLI = "fail-runcli"
	helperFailNow    = "failnow"
	helperPanic      = "panic"
	helperSkip       = "skip"
	helperGoexit     = "goexit"
	helperStopMsg    = "stopped by " + envFailureHelper

	wantStderr = testBuildMsg + "\n" + testStderrMsg + "\n" + testCobraErr + "\n"
)

var errTestRun = errors.New("run failed")

// newTestCmd builds a command that reports its arguments on stdout, writes to both
// standard streams directly and through cobra, and fails when asked to. Building it
// writes to stderr, as the creation of a logger may.
func newTestCmd() *cobra.Command {
	var fail bool

	fmt.Fprintln(os.Stderr, testBuildMsg)

	cmd := &cobra.Command{
		Use:           testUse,
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Println(testStdoutMsg)
			fmt.Fprintln(os.Stderr, testStderrMsg)
			fmt.Fprintln(cmd.OutOrStdout(), testCobraOut)
			fmt.Fprintln(cmd.ErrOrStderr(), testCobraErr)
			fmt.Printf("args=%d\n", len(args))

			if fail {
				return errTestRun
			}

			return nil
		},
	}
	cmd.Flags().BoolVar(&fail, testFlagFail, false, "fail the run")

	return cmd
}

// The tests of this file that capture the standard streams are not parallel:
// CaptureOutput does not allow it. That includes TestCaptureOutput_FailureHelper, which
// captures only when it runs in a child process. Two tests never capture in their own
// process and are parallel: one checks that a parallel test is refused, the other starts
// the child processes.

func TestRunCLI(t *testing.T) {
	tests := []struct {
		name       string
		giveArgs   []string
		wantStdout string
		wantErr    error
	}{
		{
			name:       "no arguments",
			wantStdout: testNoArgs,
		},
		{
			name:       "positional arguments",
			giveArgs:   []string{"one", "two"},
			wantStdout: "args=2",
		},
		{
			name:       "command error is returned with the output",
			giveArgs:   []string{testArgFail},
			wantStdout: testNoArgs,
			wantErr:    errTestRun,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stdout, stderr, err := RunCLI(t, newTestCmd, tt.giveArgs...)

			require.ErrorIs(t, err, tt.wantErr)
			assert.Equal(t, testStdoutMsg+"\n"+testCobraOut+"\n"+tt.wantStdout+"\n", stdout)
			assert.Equal(t, wantStderr, stderr)
		})
	}
}

// TestRunCLI_NoArgs makes sure a run without arguments does not take the arguments of
// the test binary, as cobra does for a command whose arguments are not set.
func TestRunCLI_NoArgs(t *testing.T) {
	origArgs := os.Args
	t.Cleanup(func() { os.Args = origArgs })

	os.Args = []string{origArgs[0], "leaked"}

	stdout, _, err := RunCLI(t, newTestCmd)

	require.NoError(t, err)
	assert.Contains(t, stdout, testNoArgs)
}

// TestRunCLI_Context makes sure the command runs with the context of the test, so it is
// canceled when the test ends.
func TestRunCLI_Context(t *testing.T) {
	var got context.Context

	_, _, err := RunCLI(t, func() *cobra.Command {
		return &cobra.Command{
			Use: testUse,
			Run: func(cmd *cobra.Command, _ []string) { got = cmd.Context() },
		}
	})

	require.NoError(t, err)
	assert.Same(t, t.Context(), got)
}

func TestRunCLI_ParseError(t *testing.T) {
	stdout, stderr, err := RunCLI(t, newTestCmd, "--bogus")

	require.ErrorContains(t, err, "unknown flag: --bogus")
	assert.Empty(t, stdout)
	assert.Equal(t, testBuildMsg+"\n", stderr)
}

// TestCaptureOutput_LargeOutput makes sure output larger than a pipe buffer does not
// block the writer.
func TestCaptureOutput_LargeOutput(t *testing.T) {
	const size = 1 << 20

	give := strings.Repeat("x", size)

	stdout, stderr := CaptureOutput(t, func() {
		fmt.Print(give)
		fmt.Fprint(os.Stderr, give)
	})

	assert.Len(t, stdout, size)
	assert.Len(t, stderr, size)
}

func TestCaptureOutput_RestoresStreams(t *testing.T) {
	origStdout, origStderr := os.Stdout, os.Stderr

	// The report writer is not always os.Stderr: with -json the testing package replaces
	// os.Stderr after the logging package has bound its writer.
	origReports := logging.SetOutWriter(nil)
	logging.SetOutWriter(origReports)

	assertRestored := func(t *testing.T) {
		t.Helper()

		assert.Same(t, origStdout, os.Stdout)
		assert.Same(t, origStderr, os.Stderr)
		assert.Same(t, origReports, logging.SetOutWriter(origReports))
	}

	t.Run("after fn returns", func(t *testing.T) {
		CaptureOutput(t, func() {
			assert.NotSame(t, origStdout, os.Stdout)
			assert.NotSame(t, origStderr, os.Stderr)
		})

		assertRestored(t)
	})

	t.Run("after fn stops its goroutine", func(t *testing.T) {
		done := make(chan struct{})

		// runtime.Goexit is what a failed require does to the test goroutine.
		go func() {
			defer close(done)

			//nolint:testifylint // The assertions reached before fn exits only check os.Pipe.
			CaptureOutput(t, runtime.Goexit)
		}()

		<-done

		assertRestored(t)
	})

	t.Run("after fn skips the test", func(t *testing.T) {
		t.Run("skipped", func(t *testing.T) {
			CaptureOutput(t, t.SkipNow)
		})

		assertRestored(t)
	})
}

// TestCaptureOutput_Reports makes sure the reports of the logging package, which are
// printed through a writer bound to stderr at start, are captured.
func TestCaptureOutput_Reports(t *testing.T) {
	const estimate = 4242

	stdout, stderr := CaptureOutput(t, func() {
		logging.ReportEstimate(estimate, false, nil)
	})

	assert.Empty(t, stdout)
	assert.Contains(t, stderr, strconv.Itoa(estimate))
}

// TestCaptureOutput_FailureHelper is not a test of its own. It is run in a child process
// by TestCaptureOutput_LogsOutputOfFailedTest, which sets envFailureHelper to the way
// the test has to end.
func TestCaptureOutput_FailureHelper(t *testing.T) {
	mode := os.Getenv(envFailureHelper)

	write := func() {
		fmt.Println(testStdoutMsg)
		fmt.Fprintln(os.Stderr, testStderrMsg)
	}

	switch mode {
	case "":
		t.Skipf("skipping: runs only as a child process with %s set", envFailureHelper)
	case helperFailRunCLI:
		_, _, err := RunCLI(t, newTestCmd)
		require.NoError(t, err)

		t.Fail()

		return
	case helperGoexit:
		done := make(chan struct{})

		// Only the goroutine of the capture is stopped, the test goes on and passes.
		go func() {
			defer close(done)

			//nolint:testifylint // The assertions reached before fn exits only check os.Pipe.
			CaptureOutput(t, func() {
				write()
				runtime.Goexit()
			})
		}()

		<-done

		return
	}

	CaptureOutput(t, func() {
		if mode != helperFailEmpty {
			write()
		}

		switch mode {
		case helperPanic:
			panic(helperStopMsg)
		case helperFailNow:
			t.FailNow()
		case helperSkip:
			t.Skip(helperStopMsg)
		}
	})

	if mode == helperFail || mode == helperFailEmpty {
		t.Fail()
	}
}

// TestCaptureOutput_LogsOutputOfFailedTest makes sure the captured output gets into the
// log of a test that fails or whose fn does not return, once and under the right labels,
// and stays out of the log of one that passes, is skipped or has captured nothing. The log of a failed
// test can only be seen from outside, so the test binary runs itself.
func TestCaptureOutput_LogsOutputOfFailedTest(t *testing.T) {
	t.Parallel()

	exe, err := os.Executable()
	require.NoError(t, err)

	tests := []struct {
		giveMode   string
		wantFailed bool
		wantLogged bool
	}{
		{giveMode: helperFail, wantFailed: true, wantLogged: true},
		{giveMode: helperPanic, wantFailed: true, wantLogged: true},
		// Both the deferred call and the cleanup see this failure.
		{giveMode: helperFailNow, wantFailed: true, wantLogged: true},
		// The capture is made by RunCLI, which must not be reported as its position.
		{giveMode: helperFailRunCLI, wantFailed: true, wantLogged: true},
		// fn does not return, which is logged although the test passes.
		{giveMode: helperGoexit, wantLogged: true},
		{giveMode: helperFailEmpty, wantFailed: true},
		{giveMode: helperSkip},
		{giveMode: helperPass},
	}

	for _, tt := range tests {
		t.Run(tt.giveMode, func(t *testing.T) {
			t.Parallel()

			cmd := exec.CommandContext(t.Context(), exe, "-test.run=^TestCaptureOutput_FailureHelper$", "-test.v")
			// A binary built with the race detector waits a second before a clean exit.
			cmd.Env = append(os.Environ(),
				envFailureHelper+"="+tt.giveMode,
				envRaceOptions+"="+os.Getenv(envRaceOptions)+" atexit_sleep_ms=0",
			)

			got, err := cmd.CombinedOutput()
			log := string(got)

			// A child that could not be started is neither a failed nor a passed test.
			if tt.wantFailed {
				var exitErr *exec.ExitError

				require.ErrorAs(t, err, &exitErr, log)
			} else {
				require.NoError(t, err, log)
			}

			if !tt.wantLogged {
				assert.NotContains(t, log, logCapturedPrefix)

				return
			}

			require.Equal(t, 1, strings.Count(log, logCapturedPrefix), log)

			// The position is in this file, and each message is under its own label.
			_, entry, _ := strings.Cut(log, logCapturedPrefix)

			assert.True(t, strings.HasPrefix(entry, " cli_test.go:"), entry)
			assert.IsIncreasing(t, []int{
				0,
				strings.Index(entry, "stdout:"),
				strings.Index(entry, testStdoutMsg),
				strings.Index(entry, "stderr:"),
				strings.Index(entry, testStderrMsg),
			}, entry)
		})
	}
}

// TestCaptureOutput_RefusesParallelAfterCapture makes sure a test cannot become parallel
// once it has captured the streams.
func TestCaptureOutput_RefusesParallelAfterCapture(t *testing.T) {
	CaptureOutput(t, func() {})

	assert.Panics(t, t.Parallel)
}

// TestCaptureOutput_RefusesParallelTest makes sure a parallel test cannot capture the
// streams, neither directly nor through a parallel parent.
func TestCaptureOutput_RefusesParallelTest(t *testing.T) {
	t.Parallel()

	noop := func() {}

	t.Run("parallel test", func(t *testing.T) {
		t.Parallel()

		assert.Panics(t, func() { CaptureOutput(t, noop) })
	})

	t.Run("subtest of a parallel test", func(t *testing.T) {
		t.Parallel()

		t.Run("serial", func(t *testing.T) {
			assert.Panics(t, func() { CaptureOutput(t, noop) })
		})
	})
}
