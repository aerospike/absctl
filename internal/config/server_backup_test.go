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

package config

import (
	"testing"

	"github.com/aerospike/absctl/internal/models"
	"github.com/aerospike/tools-common-go/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewServerBackupServiceConfig(t *testing.T) {
	t.Parallel()

	var (
		serverBackup = &models.ServerBackup{}
		app          = &models.App{}
		clientCfg    = &client.AerospikeConfig{}
		clientPolicy = &models.ClientPolicy{}
		secretAgent  = &models.SecretAgent{}
		awsS3        = &models.AwsS3{}
	)

	tests := []struct {
		name             string
		integratedBackup *models.ServerBackup
		app              *models.App
		clientConfig     *client.AerospikeConfig
		clientPolicy     *models.ClientPolicy
		secretAgent      *models.SecretAgent
		awsS3            *models.AwsS3
	}{
		{
			name:             "all fields set",
			integratedBackup: serverBackup,
			app:              app,
			clientConfig:     clientCfg,
			clientPolicy:     clientPolicy,
			secretAgent:      secretAgent,
			awsS3:            awsS3,
		},
		{
			name: "all fields nil",
		},
		{
			name:             "only mandatory-looking fields",
			integratedBackup: serverBackup,
			app:              app,
			clientConfig:     clientCfg,
		},
		{
			name:  "only cloud storage fields set",
			awsS3: awsS3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := NewServerBackupServiceConfig(
				tt.integratedBackup,
				tt.app,
				tt.clientConfig,
				tt.clientPolicy,
				tt.secretAgent,
				tt.awsS3,
			)

			require.NotNil(t, got)

			assert.Same(t, tt.integratedBackup, got.ServerBackup, "ServerBackup")
			assert.Same(t, tt.app, got.App, "App")
			assert.Same(t, tt.clientConfig, got.ClientConfig, "ClientConfig")
			assert.Same(t, tt.clientPolicy, got.ClientPolicy, "ClientPolicy")
			assert.Same(t, tt.secretAgent, got.SecretAgent, "SecretAgent")
			assert.Same(t, tt.awsS3, got.AwsS3, "AwsS3")
		})
	}
}

func TestServerBackupServiceConfig_Validate(t *testing.T) {
	t.Parallel()

	validConfig := func() *ServerBackupServiceConfig {
		return &ServerBackupServiceConfig{
			ServerBackup: &models.ServerBackup{},
			ServiceConfigCommon: ServiceConfigCommon{
				App:          &models.App{},
				ClientConfig: &client.AerospikeConfig{},
				ClientPolicy: &models.ClientPolicy{},
				Encryption:   &models.Encryption{},
				Compression:  &models.Compression{},
			},
		}
	}

	tests := []struct {
		name       string
		cfg        *ServerBackupServiceConfig
		isBackup   bool
		wantErr    bool
		wantErrMsg string
	}{
		{
			name:     "valid config in backup mode",
			cfg:      validConfig(),
			isBackup: true,
			wantErr:  false,
		},
		{
			name:     "valid config in restore mode",
			cfg:      validConfig(),
			isBackup: false,
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.cfg.Validate(tt.isBackup)

			if tt.wantErr {
				require.Error(t, err)
				if tt.wantErrMsg != "" {
					assert.Contains(t, err.Error(), tt.wantErrMsg)
				}
				return
			}
			require.NoError(t, err)
		})
	}
}
