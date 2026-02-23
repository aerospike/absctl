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
	"testing"

	"github.com/aerospike/absctl/internal/models"
	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/tools-common-go/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	_, err := NewAerospikeClient(t.Context(), cfg, cp, "1", 10, slog.Default(), nil)
	require.NoError(t, err)

	cfg = &client.AerospikeConfig{
		User:     testASLoginPassword,
		Password: testASLoginPassword,
	}
	_, err = NewAerospikeClient(t.Context(), cfg, cp, "", 10, slog.Default(), nil)
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
	_, err = NewAerospikeClient(t.Context(), cfg, cp, "", 10, slog.Default(), nil)
	require.ErrorContains(t, err, "failed to create Aerospike client policy")

	hostPort.Host = "255.255.255.255"
	cfg = &client.AerospikeConfig{
		Seeds: client.HostTLSPortSlice{
			hostPort,
		},
		User:     testASLoginPassword,
		Password: testASLoginPassword,
	}
	_, err = NewAerospikeClient(t.Context(), cfg, cp, "", 10, slog.Default(), nil)
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
