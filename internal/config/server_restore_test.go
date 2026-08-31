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

func validServerRestoreServiceConfig() *ServerRestoreServiceConfig {
	return &ServerRestoreServiceConfig{
		Start: &models.ServerRestore{
			ServerCommon: models.ServerCommon{
				Namespace:   testServerNamespace,
				StorageType: testServerStorage,
			},
			JobID: testServerJobID,
		},
		Prepare: &models.ServerRestorePrepare{
			Namespace: testServerNamespace,
			JobID:     testServerJobID,
		},
		ServiceConfigCommon: ServiceConfigCommon{
			App:          &models.App{},
			ClientConfig: &client.AerospikeConfig{},
			ClientPolicy: &models.ClientPolicy{},
			Encryption:   &models.Encryption{},
			Compression:  &models.Compression{},
		},
	}
}

func TestNewServerRestoreServiceConfig(t *testing.T) {
	t.Parallel()

	var (
		restore      = &models.ServerRestore{}
		prepare      = &models.ServerRestorePrepare{}
		progress     = &models.ServerRestoreProgress{}
		app          = &models.App{}
		clientCfg    = &client.AerospikeConfig{}
		clientPolicy = &models.ClientPolicy{}
		secretAgent  = &models.SecretAgent{}
		awsS3        = &models.AwsS3{}
	)

	tests := []struct {
		name      string
		restore   *models.ServerRestore
		prepare   *models.ServerRestorePrepare
		progress  *models.ServerRestoreProgress
		app       *models.App
		clientCfg *client.AerospikeConfig
		clientPol *models.ClientPolicy
		secret    *models.SecretAgent
		awsS3     *models.AwsS3
	}{
		{
			name:      "all fields set",
			restore:   restore,
			prepare:   prepare,
			progress:  progress,
			app:       app,
			clientCfg: clientCfg,
			clientPol: clientPolicy,
			secret:    secretAgent,
			awsS3:     awsS3,
		},
		{
			name: "all fields nil",
		},
		{
			name:      "only mandatory-looking fields",
			restore:   restore,
			app:       app,
			clientCfg: clientCfg,
		},
		{
			name:  "only cloud storage fields set",
			awsS3: awsS3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := NewServerRestoreServiceConfig(
				tt.restore,
				tt.prepare,
				tt.progress,
				tt.app,
				tt.clientCfg,
				tt.clientPol,
				tt.secret,
				tt.awsS3,
			)

			require.NotNil(t, got)

			assert.Same(t, tt.restore, got.Start)
			assert.Same(t, tt.prepare, got.Prepare)
			assert.Same(t, tt.app, got.App)
			assert.Same(t, tt.clientCfg, got.ClientConfig)
			assert.Same(t, tt.clientPol, got.ClientPolicy)
			assert.Same(t, tt.secret, got.SecretAgent)
			assert.Same(t, tt.awsS3, got.AwsS3)
		})
	}
}

func TestServerRestoreServiceConfig_Validate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		cfg        func() *ServerRestoreServiceConfig
		isBackup   bool
		wantErr    bool
		wantErrMsg string
	}{
		{
			name:     "valid config in backup mode",
			cfg:      validServerRestoreServiceConfig,
			isBackup: true,
			wantErr:  false,
		},
		{
			name:     "valid config in restore mode",
			cfg:      validServerRestoreServiceConfig,
			isBackup: false,
			wantErr:  false,
		},
		{
			name: "nil start skips start validation",
			cfg: func() *ServerRestoreServiceConfig {
				cfg := validServerRestoreServiceConfig()
				cfg.Start = nil
				return cfg
			},
			isBackup: false,
			wantErr:  false,
		},
		{
			name: "missing storage type",
			cfg: func() *ServerRestoreServiceConfig {
				cfg := validServerRestoreServiceConfig()
				cfg.Start.StorageType = ""
				return cfg
			},
			isBackup:   false,
			wantErr:    true,
			wantErrMsg: "storage-type is required",
		},
		{
			name: "missing namespace in start",
			cfg: func() *ServerRestoreServiceConfig {
				cfg := validServerRestoreServiceConfig()
				cfg.Start.Namespace = ""
				return cfg
			},
			isBackup:   false,
			wantErr:    true,
			wantErrMsg: "namespace is required",
		},
		{
			name: "nil prepare skips prepare validation",
			cfg: func() *ServerRestoreServiceConfig {
				cfg := validServerRestoreServiceConfig()
				cfg.Prepare = nil
				return cfg
			},
			isBackup: false,
			wantErr:  false,
		},
		{
			name: "missing backup id in prepare",
			cfg: func() *ServerRestoreServiceConfig {
				cfg := validServerRestoreServiceConfig()
				cfg.Prepare.JobID = ""
				return cfg
			},
			isBackup:   false,
			wantErr:    true,
			wantErrMsg: "backup-id is required",
		},
		{
			name: "multiple cloud providers configured",
			cfg: func() *ServerRestoreServiceConfig {
				cfg := validServerRestoreServiceConfig()
				cfg.AwsS3 = &models.AwsS3{
					BucketName:          testBucket,
					Region:              "us-west-2",
					RestorePollDuration: 1,
					StorageCommon: models.StorageCommon{
						RetryReadMultiplier: 2,
						RetryReadBackoff:    100,
					},
					ChunkSize: 5,
				}
				cfg.GcpStorage = &models.GcpStorage{
					BucketName:             testBucket,
					RetryBackoffMultiplier: 2,
					StorageCommon: models.StorageCommon{
						RetryReadMultiplier: 2,
						RetryReadBackoff:    100,
					},
					ChunkSize: 5,
				}
				return cfg
			},
			isBackup:   false,
			wantErr:    true,
			wantErrMsg: "only one cloud provider can be configured",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.cfg().Validate(tt.isBackup)

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
