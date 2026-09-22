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
	"errors"
	"log/slog"
	"runtime"
	"testing"
	"time"

	"github.com/aerospike/absctl/internal/config"
	"github.com/aerospike/absctl/internal/models"
	"github.com/aerospike/absctl/internal/testutil"
	"github.com/aerospike/tools-common-go/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testLiveHost     = "127.0.0.1"
	testLivePort     = 3000
	testLiveUser     = "admin"
	testLivePassword = "admin"

	// testLiveTimeoutMs is generous on purpose: these tests force repeated collections
	// while the client is connecting, and a tight timeout would turn that pause into a
	// connection failure that says nothing about the behavior under test.
	testLiveTimeoutMs = 10_000

	// finalizerGCRounds is how many collections the tests run to make sure a finalizer
	// that became eligible has actually executed, and finalizerGCPause is the time the
	// finalizer goroutine is given between them.
	finalizerGCRounds = 5
	finalizerGCPause  = 20 * time.Millisecond
)

// errNoActiveNodes reports an info client whose cluster has no active node left, which
// is the state a closed cluster ends in and the reason info commands then fail with
// INVALID_NODE_ERROR.
var errNoActiveNodes = errors.New("info client reports no active nodes")

// newLiveService builds a Service pointed at the Aerospike cluster the integration
// tests run against.
func newLiveService(t *testing.T) *Service {
	t.Helper()

	return &Service{
		backupCfg: &config.ServerBackupServiceConfig{
			App: &models.App{},
			ClientConfig: &client.AerospikeConfig{
				Seeds: client.HostTLSPortSlice{
					&client.HostTLSPort{Host: testLiveHost, Port: testLivePort},
				},
				User:     testLiveUser,
				Password: testLivePassword,
			},
			ClientPolicy: &models.ClientPolicy{
				Timeout:      testLiveTimeoutMs,
				IdleTimeout:  testLiveTimeoutMs,
				LoginTimeout: testLiveTimeoutMs,
			},
		},
		logger: slog.New(slog.DiscardHandler),
	}
}

// runFinalizers reclaims everything unreachable and gives the queued finalizers a chance
// to run. A finalizer is only queued by the collection that finds its object unreachable
// and then runs on a separate goroutine, so a single cycle is not enough to observe it.
func runFinalizers(t *testing.T) {
	t.Helper()

	for range finalizerGCRounds {
		runtime.GC()
		time.Sleep(finalizerGCPause)
	}
}

// TestInfoClientSurvivesGarbageCollection guards the fix for the intermittent
// INVALID_NODE_ERROR "cluster is empty" failures of the server-integrated commands.
// asinfo.Client keeps only the *aerospike.Cluster, while the *aerospike.Client that owns
// that cluster closes it from a runtime finalizer. An info client that does not keep the
// Aerospike client reachable therefore loses its cluster to the next collection, and
// every info command after that fails for good. The commands that spend time on object
// storage between creating the info client and using it - "snapshot-restore start" above
// all - gave the collector that chance often enough to fail in the field.
func TestInfoClientSurvivesGarbageCollection(t *testing.T) {
	t.Parallel()
	testutil.RequireIntegration(t, testutil.ServiceAerospike)

	tests := []struct {
		name string
		call func(ctx context.Context, c *infoClient) error
	}{
		{
			// A closed cluster keeps no active node, which is what turns every later
			// info command into INVALID_NODE_ERROR.
			name: "reports active nodes",
			call: func(_ context.Context, c *infoClient) error {
				if len(c.GetNodesNames()) == 0 {
					return errNoActiveNodes
				}

				return nil
			},
		},
		{
			// GetVersion reads the nodes of the cluster directly, and is the first
			// cluster call every server-integrated command makes.
			name: "gets the cluster version",
			call: func(ctx context.Context, c *infoClient) error {
				_, err := c.GetVersion(ctx)

				return err
			},
		},
		{
			// GetStatus goes through Cluster.GetRandomNode, the call that answers with
			// INVALID_NODE_ERROR once the cluster is closed.
			name: "gets the cluster status",
			call: func(ctx context.Context, c *infoClient) error {
				_, err := c.GetStatus(ctx)

				return err
			},
		},
		{
			// The cluster principal that "snapshot-restore start" looks up before it
			// requests a restore is read over this same path.
			name: "lists the namespaces",
			call: func(ctx context.Context, c *infoClient) error {
				_, err := c.GetNamespacesList(ctx)

				return err
			},
		},
	}

	// One client is shared by the cases on purpose: the collection that would have
	// closed its cluster happens once, before any of them run, and every case then
	// asks the same client whether it survived.
	ic, err := newLiveService(t).newInfoClient()
	require.NoError(t, err)

	t.Cleanup(ic.Close)

	runFinalizers(t)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			require.NoError(t, tt.call(t.Context(), ic))
		})
	}
}

// TestInfoClientClose checks that closing the info client is what takes its cluster
// down, and that closing it again - as the runtime finalizer of the Aerospike client
// eventually does - stays harmless.
func TestInfoClientClose(t *testing.T) {
	t.Parallel()
	testutil.RequireIntegration(t, testutil.ServiceAerospike)

	tests := []struct {
		name  string
		calls int
	}{
		{
			name:  "close shuts the cluster down",
			calls: 1,
		},
		{
			name:  "close is safe to repeat",
			calls: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ic, err := newLiveService(t).newInfoClient()
			require.NoError(t, err)

			require.NotEmpty(t, ic.GetNodesNames(),
				"the cluster must be up before the info client is closed")

			for range tt.calls {
				ic.Close()
			}

			assert.Empty(t, ic.GetNodesNames(),
				"a closed cluster must keep no active node")
		})
	}
}
