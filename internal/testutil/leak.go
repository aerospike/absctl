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
	"runtime/debug"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/goleak"
)

// envVerifiesLeaks is the variable VerifyNoGoroutineLeaks sets; its value is never read.
const envVerifiesLeaks = "ABSCTL_TEST_VERIFIES_LEAKS"

const (
	// leakWaitTimeout bounds how long goroutines started by the test may take to exit
	// after it ends. It covers an Aerospike connect that outlives a canceled context
	// until the client's own timeouts end it.
	leakWaitTimeout = 10 * time.Second
	leakWaitTick    = 50 * time.Millisecond
)

// VerifyNoGoroutineLeaks fails t if a goroutine started after this call is still
// running when t ends. Goroutines get up to leakWaitTimeout to exit, so a test may
// leave work that winds down on its own, such as a connect bounded by a timeout.
//
// Goroutines are process-wide, so a test that checks them must not run in parallel
// with any other test. This is enforced through t.Setenv: VerifyNoGoroutineLeaks panics
// when t or one of its parents is parallel, and t.Parallel panics when it is called
// afterwards. Parallel top-level tests of the package are paused while a non-parallel
// one runs.
//
// Goroutines running at the call are ignored. Start a test server before the call, so
// that it outlives the check: a server stopped earlier closes the connections of the
// clients under test itself and hides their leak.
//
// Automatic garbage collection is off until the check ends. Some clients, such as the
// Aerospike client, close themselves in a finalizer: a collection would end the
// goroutines of a client that was dropped without being closed, and hide that leak.
func VerifyNoGoroutineLeaks(t *testing.T) {
	t.Helper()

	t.Setenv(envVerifiesLeaks, "1")

	ignore := goleak.IgnoreCurrent()
	gcPercent := debug.SetGCPercent(-1)

	// Registered first, so it runs after every cleanup the test registers later.
	t.Cleanup(func() {
		defer debug.SetGCPercent(gcPercent)

		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.NoError(c, goleak.Find(ignore))
		}, leakWaitTimeout, leakWaitTick, "goroutines leaked by the test")
	})
}
