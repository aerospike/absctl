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

func TestServiceConnection_Validate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		conn    ServiceConnection
		wantErr bool
		errMsg  string
	}{
		{
			name:    "valid connection",
			conn:    ServiceConnection{Host: "localhost", Port: 8080},
			wantErr: false,
		},
		{
			name:    "valid connection with IP",
			conn:    ServiceConnection{Host: "127.0.0.1", Port: 1},
			wantErr: false,
		},
		{
			name:    "valid connection with max port",
			conn:    ServiceConnection{Host: "example.com", Port: 65535},
			wantErr: false,
		},
		{
			name:    "empty host",
			conn:    ServiceConnection{Host: "", Port: 8080},
			wantErr: true,
			errMsg:  "service host is required",
		},
		{
			name:    "zero port",
			conn:    ServiceConnection{Host: "localhost", Port: 0},
			wantErr: true,
			errMsg:  "service port must be between 1 and 65535",
		},
		{
			name:    "negative port",
			conn:    ServiceConnection{Host: "localhost", Port: -1},
			wantErr: true,
			errMsg:  "service port must be between 1 and 65535",
		},
		{
			name:    "port too high",
			conn:    ServiceConnection{Host: "localhost", Port: 65536},
			wantErr: true,
			errMsg:  "service port must be between 1 and 65535",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.conn.Validate()

			if tt.wantErr {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errMsg)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestServiceConnection_ServerURL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		conn ServiceConnection
		want string
	}{
		{
			name: "localhost with default port",
			conn: ServiceConnection{Host: "localhost", Port: 8080},
			want: "http://localhost:8080",
		},
		{
			name: "ipv4 address",
			conn: ServiceConnection{Host: "192.168.1.10", Port: 9090},
			want: "http://192.168.1.10:9090",
		},
		{
			name: "fqdn",
			conn: ServiceConnection{Host: "abs.example.com", Port: 443},
			want: "http://abs.example.com:443",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, tt.conn.ServerURL())
		})
	}
}
