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
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"

	"github.com/aerospike/absctl/internal/logging"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

// envCapturesOutput is set by CaptureOutput only to make the testing package refuse
// parallel tests, see CaptureOutput.
const envCapturesOutput = "ABSCTL_TEST_CAPTURES_OUTPUT"

// logCapturedPrefix starts the log entry with the output of a failed test.
const logCapturedPrefix = "output captured at"

// RunCLI builds a command and executes it with args through cobra, as main does. It
// returns what the run wrote to the standard streams and the error of the command.
//
// The command comes from build, so that this package does not import the command tree
// and can be used from any package of it. build runs with the streams already captured,
// so a writer that it binds to a stream, such as the logger of cli.NewCmd, is captured
// too. It must return a fresh command: flag holders keep the values of a previous parse.
//
// The command is executed as build returns it: its output wiring is part of what is
// tested, and whatever main does to the command besides executing it is up to build.
//
// The calling test must not be parallel, see CaptureOutput for this and for the limits
// of the capture.
func RunCLI(
	t *testing.T, build func() *cobra.Command, args ...string,
) (stdout, stderr string, err error) {
	t.Helper()

	// With nil arguments cobra falls back to the arguments of the test binary.
	if args == nil {
		args = []string{}
	}

	stdout, stderr = CaptureOutput(t, func() {
		cmd := build()
		cmd.SetArgs(args)

		err = cmd.ExecuteContext(t.Context())
	})

	return stdout, stderr, err
}

// CaptureOutput redirects os.Stdout and os.Stderr for the duration of fn and returns what
// was written to them. Both streams are restored when fn returns or stops the test.
//
// The captured output is added to the log of the test when the test fails, and when fn
// does not return for a reason other than skipping the test: a panic or runtime.Goexit.
// In the second case that is done whether or not the test fails in the end.
//
// Only a writer that reads os.Stdout or os.Stderr while fn runs is captured. One bound to
// a stream before that keeps writing to the real stream, except the report printer of
// the logging package, which is redirected together with os.Stderr. This package imports
// the logging package for that, so the tests of the logging package cannot use it.
//
// fn must not leave a child process running that inherited a captured stream:
// CaptureOutput returns only when every holder of the stream has closed it. CaptureOutput
// itself must return before the test ends.
//
// The streams are process-wide, so a test that captures them must not run in parallel
// with any other test. This is enforced: CaptureOutput panics when t or one of its
// parents is parallel, and t.Parallel panics when it is called afterwards.
func CaptureOutput(t *testing.T, fn func()) (stdout, stderr string) {
	t.Helper()

	// The value is not read anywhere. t.Setenv is the way the testing package offers to
	// mark a test as one that changes process-wide state and so cannot be parallel.
	t.Setenv(envCapturesOutput, "1")

	site := callSite()

	// The deferred calls restore the streams even when fn stops the test early, for
	// example through a failed require, so later tests do not write into a dead pipe.
	out := startCapture(t, &os.Stdout)
	defer func() { _ = out.stop() }()

	errOut := startCapture(t, &os.Stderr)
	defer func() { _ = errOut.stop() }()

	// The report printer holds the stderr it was initialized with.
	prevReportWriter := logging.SetOutWriter(os.Stderr)
	defer logging.SetOutWriter(prevReportWriter)

	// The output is the first thing to look at when a command does not behave as
	// expected. It is logged once, and only after both streams are stopped, which is
	// when the buffers are complete.
	logged := false
	logOutput := func() {
		if logged || out.buf.Len()+errOut.buf.Len() == 0 {
			return
		}

		logged = true

		t.Logf("%s %s:\nstdout:\n%s\nstderr:\n%s", logCapturedPrefix, site, out.buf.String(), errOut.buf.String())
	}

	// Covers a test that fails after the capture, for example on an assertion about
	// the output.
	t.Cleanup(func() {
		if t.Failed() {
			logOutput()
		}
	})

	// Covers fn that does not return. A failed require is covered by the cleanup as
	// well, a panic is not: the log of a cleanup is lost when the test panics.
	returned := false

	defer func() {
		if returned || t.Skipped() {
			return
		}

		_ = errOut.stop()
		_ = out.stop()

		logOutput()
	}()

	fn()

	returned = true

	require.NoError(t, errOut.stop())
	require.NoError(t, out.stop())

	return out.buf.String(), errOut.buf.String()
}

// callSite returns the position of the nearest caller outside of this file: the test
// that captures the output, or the helper of that test which does it.
func callSite() string {
	// The depth covers the helpers of this file and a few of the caller.
	const maxDepth = 16

	pcs := make([]uintptr, maxDepth)
	frames := runtime.CallersFrames(pcs[:runtime.Callers(1, pcs)])

	// The first frame is callSite itself.
	self, more := frames.Next()

	for more {
		var frame runtime.Frame

		frame, more = frames.Next()
		if frame.File != self.File {
			return filepath.Base(frame.File) + ":" + strconv.Itoa(frame.Line)
		}
	}

	return "unknown position"
}

// streamCapture redirects one standard stream into a buffer.
type streamCapture struct {
	target **os.File
	orig   *os.File
	r, w   *os.File

	buf     bytes.Buffer
	readErr error
	done    chan struct{}
	stopped bool
}

// startCapture replaces *target with a pipe that is drained into the buffer while the
// capture runs, so output larger than the pipe buffer cannot block the writer.
func startCapture(t *testing.T, target **os.File) *streamCapture {
	t.Helper()

	r, w, err := os.Pipe()
	require.NoError(t, err)

	c := &streamCapture{
		target: target,
		orig:   *target,
		r:      r,
		w:      w,
		done:   make(chan struct{}),
	}
	*target = w

	go func() {
		defer close(c.done)

		_, c.readErr = io.Copy(&c.buf, r)
	}()

	return c
}

// stop restores the stream and waits until all captured output is in the buffer.
// Only the first call does the work and reports its error.
func (c *streamCapture) stop() error {
	if c.stopped {
		return nil
	}

	c.stopped = true
	*c.target = c.orig
	closeErr := c.w.Close()
	// The reader ends when the write end is closed.
	<-c.done

	return errors.Join(closeErr, c.readErr, c.r.Close())
}
