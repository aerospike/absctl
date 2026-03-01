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

package models

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSecretAgent_Validate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		agent   *SecretAgent
		wantErr bool
		errMsg  string
	}{
		{
			name:    "nil agent returns no error",
			agent:   nil,
			wantErr: false,
		},
		{
			name: "empty connection type returns error",
			agent: &SecretAgent{
				ConnectionType: "",
				Address:        "localhost",
				Port:           3000,
			},
			wantErr: true,
			errMsg:  "missing connection type",
		},
		{
			name: "unsupported connection type returns error",
			agent: &SecretAgent{
				ConnectionType: "HTTP",
				Address:        "localhost",
				Port:           3000,
			},
			wantErr: true,
			errMsg:  "unsupported connection type: HTTP",
		},
		{
			name: "valid TCP connection type uppercase",
			agent: &SecretAgent{
				ConnectionType:     "TCP",
				Address:            "localhost",
				Port:               3000,
				TimeoutMillisecond: 5000,
			},
			wantErr: false,
		},
		{
			name: "valid TCP connection type lowercase",
			agent: &SecretAgent{
				ConnectionType: "tcp",
				Address:        "127.0.0.1",
				Port:           3000,
			},
			wantErr: false,
		},
		{
			name: "valid TCP connection type mixed case",
			agent: &SecretAgent{
				ConnectionType: "TcP",
				Address:        "localhost",
				Port:           3000,
			},
			wantErr: false,
		},
		{
			name: "valid UDS connection type uppercase",
			agent: &SecretAgent{
				ConnectionType: "UNIX",
				Address:        "/tmp/socket",
			},
			wantErr: false,
		},
		{
			name: "valid UDS connection type lowercase",
			agent: &SecretAgent{
				ConnectionType: "unix",
				Address:        "/var/run/socket",
			},
			wantErr: false,
		},
		{
			name: "valid UDS connection type mixed case",
			agent: &SecretAgent{
				ConnectionType: "UnIx",
				Address:        "/tmp/socket",
			},
			wantErr: false,
		},
		{
			name: "valid TCP with TLS configuration",
			agent: &SecretAgent{
				ConnectionType:     "TCP",
				Address:            "secure.example.com",
				Port:               4333,
				TimeoutMillisecond: 10000,
				CaFile:             "/path/to/ca.pem",
				TLSName:            "example.com",
				CertFile:           "/path/to/cert.pem",
				KeyFile:            "/path/to/key.pem",
				IsBase64:           true,
			},
			wantErr: false,
		},
		{
			name: "invalid connection type with special characters",
			agent: &SecretAgent{
				ConnectionType: "TCP/IP",
				Address:        "localhost",
				Port:           3000,
			},
			wantErr: true,
			errMsg:  "unsupported connection type: TCP/IP",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.agent.Validate()

			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errMsg)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// Secret Agent Tests
func TestMapSecretAgentConfig_Success(t *testing.T) {
	t.Parallel()

	secretAgentModel := &SecretAgent{
		Address:            "localhost",
		ConnectionType:     "tcp",
		Port:               8080,
		TimeoutMillisecond: 1000,
		CaFile:             "/path/to/ca.pem",
		CertFile:           "/path/to/cert.pem",
		KeyFile:            "/path/to/key.pem",
		TLSName:            "example.com",
		IsBase64:           true,
	}

	secretAgentConfig := secretAgentModel.Config()
	assert.NotNil(t, secretAgentConfig)
	assert.Equal(t, "localhost", *secretAgentConfig.Address)
	assert.Equal(t, "tcp", *secretAgentConfig.ConnectionType)
	assert.Equal(t, 8080, *secretAgentConfig.Port)
	assert.Equal(t, 1000, *secretAgentConfig.TimeoutMillisecond)
	assert.Equal(t, "/path/to/ca.pem", *secretAgentConfig.CaFile)
	assert.Equal(t, "/path/to/cert.pem", *secretAgentConfig.CertFile)
	assert.Equal(t, "/path/to/key.pem", *secretAgentConfig.KeyFile)
	assert.Equal(t, "example.com", *secretAgentConfig.TLSName)
	assert.True(t, *secretAgentConfig.IsBase64)
}

func TestMapSecretAgentConfig_EmptyAddress(t *testing.T) {
	t.Parallel()

	secretAgentModel := &SecretAgent{}
	secretAgentConfig := secretAgentModel.Config()
	assert.Nil(t, secretAgentConfig)
}

func TestMapSecretAgentConfig_PartialConfig(t *testing.T) {
	t.Parallel()

	secretAgentModel := &SecretAgent{
		Address: "localhost",
		Port:    8080,
	}

	secretAgentConfig := secretAgentModel.Config()
	assert.NotNil(t, secretAgentConfig)
	assert.Equal(t, "localhost", *secretAgentConfig.Address)
	assert.Equal(t, 8080, *secretAgentConfig.Port)
	assert.Nil(t, secretAgentConfig.CaFile, "CaFile should be nil if not set")
}
