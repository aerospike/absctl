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

package storage

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aerospike/absctl/internal/models"
	"github.com/aerospike/absctl/internal/testutil"
	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/tools-common-go/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"
)

const (
	testASLoginPassword = "admin"

	testHost = "127.0.0.1"
	testPort = 3000
)

func testHostPort() *client.HostTLSPort {
	return &client.HostTLSPort{
		Host: testHost,
		Port: testPort,
	}
}

func TestClients_newAerospikeClient(t *testing.T) {
	t.Parallel()
	testutil.RequireIntegration(t, testutil.ServiceAerospike)

	hostPort := testHostPort()
	cfg := &client.AerospikeConfig{
		Seeds: client.HostTLSPortSlice{
			hostPort,
		},
		User:     testASLoginPassword,
		Password: testASLoginPassword,
	}
	cp := &models.ClientPolicy{
		Timeout:      1000,
		IdleTimeout:  1000,
		LoginTimeout: 1000,
	}
	asClient, err := NewAerospikeClient(t.Context(), cfg, cp, []int{1}, 10, slog.Default())
	require.NoError(t, err)

	t.Cleanup(asClient.Close)

	cfg = &client.AerospikeConfig{
		User:     testASLoginPassword,
		Password: testASLoginPassword,
	}
	_, err = NewAerospikeClient(t.Context(), cfg, cp, nil, 10, slog.Default())
	require.ErrorContains(t, err, "at least one seed must be provided")

	cfg = &client.AerospikeConfig{
		Seeds: client.HostTLSPortSlice{
			hostPort,
		},
		User:     testASLoginPassword,
		Password: testASLoginPassword,
		TLS: &client.TLSConfig{
			Cert: []byte("error"),
		},
	}
	_, err = NewAerospikeClient(t.Context(), cfg, cp, []int{}, 10, slog.Default())
	require.ErrorContains(t, err, "failed to create Aerospike client policy")

	hostPort.Host = "255.255.255.255"
	cfg = &client.AerospikeConfig{
		Seeds: client.HostTLSPortSlice{
			hostPort,
		},
		User:     testASLoginPassword,
		Password: testASLoginPassword,
	}
	_, err = NewAerospikeClient(t.Context(), cfg, cp, nil, 10, slog.Default())
	require.ErrorContains(t, err, "failed to create Aerospike client")
}

// TestNewAerospikeClient_ContextDone checks that a context done while the connect is
// in progress ends NewAerospikeClient without waiting for the connect, and that the
// connect left running in the background exits on its own once the client timeout ends
// it: the silent seed outlives the leak check, so nothing else can end the connect.
func TestNewAerospikeClient_ContextDone(t *testing.T) {
	// Not parallel: VerifyNoGoroutineLeaks needs the goroutines of this test alone.

	// clientTimeout is how long the connect waits for the silent seed.
	const clientTimeout = time.Second

	// Started before the leak check, so that the server outlives it.
	seed, accepted := startSilentServer(t)

	testutil.VerifyNoGoroutineLeaks(t)

	cfg := &client.AerospikeConfig{
		Seeds: client.HostTLSPortSlice{seed},
	}
	cp := &models.ClientPolicy{
		Timeout:      clientTimeout.Milliseconds(),
		IdleTimeout:  clientTimeout.Milliseconds(),
		LoginTimeout: clientTimeout.Milliseconds(),
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	// Cancel once the connect is in progress.
	go func() {
		select {
		case <-accepted:
			cancel()
		case <-ctx.Done():
		}
	}()

	// Waiting for the connect would end it with the client timeout error instead.
	asClient, err := NewAerospikeClient(ctx, cfg, cp, nil, 1, slog.New(slog.DiscardHandler))
	require.ErrorIs(t, err, context.Canceled)
	assert.Nil(t, asClient)
}

// TestConnectWithContext checks which side owns the connected value: the caller when
// it gets the value, the connecting goroutine when the caller has given up on ctx.
func TestConnectWithContext(t *testing.T) {
	t.Parallel()

	const (
		connected = 42
		// rounds repeats each case: a select picks among ready cases at random, so an
		// ownership bug that loses the race only sometimes must get many chances to show.
		rounds = 64
	)

	errConnect := errors.New("connect failed")

	tests := []struct {
		name string
		// giveCancel cancels ctx while connect runs, before it returns.
		giveCancel   bool
		giveErr      error
		want         int
		wantErr      error
		wantReleased []int
	}{
		{
			name: "connected before done",
			want: connected,
		},
		{
			name:    "failed before done",
			giveErr: errConnect,
			wantErr: errConnect,
		},
		{
			name:         "connected after done is released",
			giveCancel:   true,
			wantErr:      context.Canceled,
			wantReleased: []int{connected},
		},
		{
			name:       "failed after done releases nothing",
			giveCancel: true,
			giveErr:    errConnect,
			wantErr:    context.Canceled,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			for range rounds {
				synctest.Test(t, func(t *testing.T) {
					ctx, cancel := context.WithCancel(t.Context())
					defer cancel()

					// connect returns only after the caller has, when it cancels ctx.
					unblock := make(chan struct{})

					connect := func() (int, error) {
						if tt.giveCancel {
							cancel()
							<-unblock
						}

						if tt.giveErr != nil {
							return 0, tt.giveErr
						}

						return connected, nil
					}

					var released []int

					got, err := connectWithContext(ctx, connect, func(v int) {
						released = append(released, v)
					})

					close(unblock)
					// Let the connecting goroutine finish before checking what it released.
					synctest.Wait()

					require.ErrorIs(t, err, tt.wantErr)
					assert.Equal(t, tt.want, got)
					assert.Equal(t, tt.wantReleased, released)
				})
			}
		})
	}
}

// TestConnectWithContext_DoneBeforeStart checks that a context done before the call
// fails it without starting the connect.
func TestConnectWithContext_DoneBeforeStart(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	connect := func() (int, error) {
		t.Error("connect must not run")

		return 0, nil
	}

	_, err := connectWithContext(ctx, connect, func(int) {
		t.Error("release must not run")
	})
	require.ErrorIs(t, err, context.Canceled)
}

// startSilentServer starts a TCP server that accepts connections and never answers,
// so an Aerospike connect to it waits for the client's timeouts. It returns the seed
// to connect to and a channel closed on the first accepted connection. The server
// stops when the test ends, after the cleanups registered later.
func startSilentServer(t *testing.T) (seed *client.HostTLSPort, accepted <-chan struct{}) {
	t.Helper()

	var lc net.ListenConfig

	ln, err := lc.Listen(t.Context(), "tcp", net.JoinHostPort(testHost, "0"))
	require.NoError(t, err)

	var (
		wg           sync.WaitGroup
		acceptedOnce = make(chan struct{})
		// conns is written only by the accept goroutine and read after wg.Wait.
		conns []net.Conn
	)

	wg.Go(func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				// The listener is closed.
				return
			}

			if len(conns) == 0 {
				close(acceptedOnce)
			}

			conns = append(conns, conn)
		}
	})

	t.Cleanup(func() {
		_ = ln.Close()

		wg.Wait()

		for _, conn := range conns {
			_ = conn.Close()
		}
	})

	addr, ok := ln.Addr().(*net.TCPAddr)
	require.True(t, ok)

	return &client.HostTLSPort{Host: testHost, Port: addr.Port}, acceptedOnce
}

func TestClients_newS3Client(t *testing.T) {
	t.Parallel()

	cfg := &models.AwsS3{
		Region:   testS3Region,
		Profile:  testS3Profile,
		Endpoint: testS3Endpoint,
	}

	ctx := t.Context()
	_, err := NewS3Client(ctx, cfg)
	require.NoError(t, err)
}

func TestClients_newGcpClient(t *testing.T) {
	t.Parallel()

	cfg := &models.GcpStorage{
		Endpoint: testGcpEndpoint,
	}

	ctx := t.Context()
	_, _, err := newGcpClient(ctx, cfg)
	require.NoError(t, err)
}

func TestClients_newAzureClient(t *testing.T) {
	t.Parallel()

	cfg := &models.AzureBlob{
		AccountName:   testAzureAccountName,
		AccountKey:    testAzureAccountKey,
		Endpoint:      testAzureEndpoint,
		ContainerName: testBucket,
	}

	_, _, err := newAzureClient(cfg)
	require.NoError(t, err)
}

func TestNewAuthTransport(t *testing.T) {
	t.Parallel()

	base := http.DefaultTransport
	ts := oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "tok"})

	tr := newAuthTransport(base, ts)
	require.NotNil(t, tr)
	assert.Same(t, base, tr.Base)
	assert.NotNil(t, tr.Source)
}

func TestNewTransportFields(t *testing.T) {
	t.Parallel()

	tr := newTransport(42)
	require.NotNil(t, tr)
	assert.Equal(t, 42, tr.MaxConnsPerHost)
	assert.Equal(t, maxIdleConnsPerHost, tr.MaxIdleConnsPerHost)

	// A connection limit above the idle pool raises the pool to match it.
	tr = newTransport(maxIdleConnsPerHost + 1)
	assert.Equal(t, maxIdleConnsPerHost+1, tr.MaxIdleConnsPerHost)
	assert.True(t, tr.ForceAttemptHTTP2)
}

func TestNewHTTPClientTimeout(t *testing.T) {
	t.Parallel()

	c := newHTTPClient(http.DefaultTransport, 1500)
	require.NotNil(t, c)
	assert.NotZero(t, c.Timeout)
	assert.Equal(t, http.DefaultTransport, c.Transport)
}

func TestGetGcpAuth_MissingFile(t *testing.T) {
	t.Parallel()

	missing := filepath.Join(t.TempDir(), "no-such-file.json")

	creds, err := getGcpAuth(t.Context(), missing)
	require.Error(t, err)
	assert.Nil(t, creds)
	assert.Contains(t, err.Error(), "failed to read key file")
}

func TestGetGcpAuth_InvalidJSON(t *testing.T) {
	t.Parallel()

	keyFile := filepath.Join(t.TempDir(), "bad.json")
	require.NoError(t, os.WriteFile(keyFile, []byte("not-json"), 0o600))

	creds, err := getGcpAuth(t.Context(), keyFile)
	require.Error(t, err)
	assert.Nil(t, creds)
	assert.Contains(t, err.Error(), "failed to parse JSON key file")
}

func TestNewGcpAuthTransport_KeyFileMissing(t *testing.T) {
	t.Parallel()

	cfg := &models.GcpStorage{
		KeyFile: filepath.Join(t.TempDir(), "missing-key.json"),
	}

	transport, err := newGcpAuthTransport(t.Context(), cfg, newTransport(0))
	require.Error(t, err)
	assert.Nil(t, transport)
	assert.Contains(t, err.Error(), "failed to read key file")
}

func TestNewGcpClient_KeyFileMissing(t *testing.T) {
	t.Parallel()

	cfg := &models.GcpStorage{
		// No Endpoint forces newGcpClient to call newGcpAuthTransport,
		// which then fails because the key file is missing.
		KeyFile: filepath.Join(t.TempDir(), "missing-key.json"),
	}

	c, _, err := newGcpClient(t.Context(), cfg)
	require.Error(t, err)
	assert.Nil(t, c)
	assert.Contains(t, err.Error(), "failed to get GCP transport")
}

func TestNewAzureClient_AAD(t *testing.T) {
	t.Parallel()

	cfg := &models.AzureBlob{
		Endpoint:     testAzureEndpoint,
		TenantID:     "00000000-0000-0000-0000-000000000000",
		ClientID:     "00000000-0000-0000-0000-000000000001",
		ClientSecret: "fake-secret",
	}

	c, _, err := newAzureClient(cfg)
	require.NoError(t, err)
	assert.NotNil(t, c)
}

func TestNewAzureClient_NoCredentials(t *testing.T) {
	t.Parallel()

	cfg := &models.AzureBlob{Endpoint: testAzureEndpoint}

	c, _, err := newAzureClient(cfg)
	require.NoError(t, err)
	assert.NotNil(t, c)
}

func TestNewAzureClient_InvalidSharedKey(t *testing.T) {
	t.Parallel()

	cfg := &models.AzureBlob{
		Endpoint:    testAzureEndpoint,
		AccountName: "devstoreaccount1",
		// Not valid base64.
		AccountKey: "!!!not-base64!!!",
	}

	c, _, err := newAzureClient(cfg)
	require.Error(t, err)
	assert.Nil(t, c)
	assert.Contains(t, err.Error(), "Azure shared key credentials")
}

func TestToHosts(t *testing.T) {
	tests := []struct {
		name     string
		input    client.HostTLSPortSlice
		expected []*aerospike.Host
	}{
		{
			name: "Single Host",
			input: client.HostTLSPortSlice{
				{Host: "localhost", TLSName: "tls1", Port: 3000},
			},
			expected: []*aerospike.Host{
				{Name: "localhost", TLSName: "tls1", Port: 3000},
			},
		},
		{
			name: "Multiple Hosts",
			input: client.HostTLSPortSlice{
				{Host: "host1", TLSName: "tls1", Port: 3000},
				{Host: "host2", TLSName: "tls2", Port: 3001},
			},
			expected: []*aerospike.Host{
				{Name: "host1", TLSName: "tls1", Port: 3000},
				{Name: "host2", TLSName: "tls2", Port: 3001},
			},
		},
		{
			name:     "Empty Input",
			input:    client.HostTLSPortSlice{},
			expected: []*aerospike.Host{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := toHosts(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}
