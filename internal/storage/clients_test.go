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

package storage

import (
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/aerospike/absctl/internal/models"
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
	_, err := NewAerospikeClient(cfg, cp, []int{1}, 10, slog.Default())
	require.NoError(t, err)

	cfg = &client.AerospikeConfig{
		User:     testASLoginPassword,
		Password: testASLoginPassword,
	}
	_, err = NewAerospikeClient(cfg, cp, nil, 10, slog.Default())
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
	_, err = NewAerospikeClient(cfg, cp, []int{}, 10, slog.Default())
	require.ErrorContains(t, err, "failed to create Aerospike client policy")

	hostPort.Host = "255.255.255.255"
	cfg = &client.AerospikeConfig{
		Seeds: client.HostTLSPortSlice{
			hostPort,
		},
		User:     testASLoginPassword,
		Password: testASLoginPassword,
	}
	_, err = NewAerospikeClient(cfg, cp, nil, 10, slog.Default())
	require.ErrorContains(t, err, "failed to create Aerospike client")
}

func TestClients_newS3Client(t *testing.T) {
	t.Parallel()

	err := createAwsCredentials()
	require.NoError(t, err)

	cfg := &models.AwsS3{
		Region:   testS3Region,
		Profile:  testS3Profile,
		Endpoint: testS3Endpoint,
	}

	ctx := t.Context()
	_, err = newS3Client(ctx, cfg)
	require.NoError(t, err)
}

func TestClients_newGcpClient(t *testing.T) {
	t.Parallel()

	cfg := &models.GcpStorage{
		Endpoint: testGcpEndpoint,
	}

	ctx := t.Context()
	_, err := newGcpClient(ctx, cfg)
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

	_, err := newAzureClient(cfg)
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

func TestGetGcpTransport_KeyFileMissing(t *testing.T) {
	t.Parallel()

	cfg := &models.GcpStorage{
		KeyFile: filepath.Join(t.TempDir(), "missing-key.json"),
	}

	transport, err := getGcpTransport(t.Context(), cfg)
	require.Error(t, err)
	assert.Nil(t, transport)
	assert.Contains(t, err.Error(), "failed to read key file")
}

func TestNewGcpClient_KeyFileMissing(t *testing.T) {
	t.Parallel()

	cfg := &models.GcpStorage{
		// No Endpoint forces newGcpClient to call getGcpTransport,
		// which then fails because the key file is missing.
		KeyFile: filepath.Join(t.TempDir(), "missing-key.json"),
	}

	client, err := newGcpClient(t.Context(), cfg)
	require.Error(t, err)
	assert.Nil(t, client)
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

	c, err := newAzureClient(cfg)
	require.NoError(t, err)
	assert.NotNil(t, c)
}

func TestNewAzureClient_NoCredentials(t *testing.T) {
	t.Parallel()

	cfg := &models.AzureBlob{Endpoint: testAzureEndpoint}

	c, err := newAzureClient(cfg)
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

	c, err := newAzureClient(cfg)
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
