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

const (
	testServerNamespace = "test-ns"
	testServerJobID     = "backup-job-1"
	testServerListPath  = "/backups"
	testServerStorage   = "s3"
)

func validServerBackupServiceConfig() *ServerBackupServiceConfig {
	return &ServerBackupServiceConfig{
		Start: &models.ServerBackup{
			ServerCommon: models.ServerCommon{
				Namespace:   testServerNamespace,
				StorageType: testServerStorage,
			},
		},
		List: &models.ServerBackupList{
			ListPath: testServerListPath,
		},
		Validation: &models.ServerBackupValidate{
			JobID: testServerJobID,
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

func TestNewServerBackupServiceConfig(t *testing.T) {
	t.Parallel()

	var (
		start        = &models.ServerBackup{}
		list         = &models.ServerBackupList{}
		validation   = &models.ServerBackupValidate{}
		progress     = &models.ServerBackupProgress{}
		app          = &models.App{}
		clientCfg    = &client.AerospikeConfig{}
		clientPolicy = &models.ClientPolicy{}
		secretAgent  = &models.SecretAgent{}
		awsS3        = &models.AwsS3{}
	)

	tests := []struct {
		name       string
		start      *models.ServerBackup
		list       *models.ServerBackupList
		validation *models.ServerBackupValidate
		progress   *models.ServerBackupProgress
		app        *models.App
		clientCfg  *client.AerospikeConfig
		clientPol  *models.ClientPolicy
		secret     *models.SecretAgent
		awsS3      *models.AwsS3
	}{
		{
			name:       "all fields set",
			start:      start,
			list:       list,
			validation: validation,
			progress:   progress,
			app:        app,
			clientCfg:  clientCfg,
			clientPol:  clientPolicy,
			secret:     secretAgent,
			awsS3:      awsS3,
		},
		{
			name: "all fields nil",
		},
		{
			name:      "only mandatory-looking fields",
			start:     start,
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

			got := NewServerBackupServiceConfig(
				tt.start,
				tt.list,
				tt.validation,
				tt.progress,
				tt.app,
				tt.clientCfg,
				tt.clientPol,
				tt.secret,
				tt.awsS3,
			)

			require.NotNil(t, got)

			assert.Same(t, tt.start, got.Start)
			assert.Same(t, tt.list, got.List)
			assert.Same(t, tt.validation, got.Validation)
			assert.Same(t, tt.progress, got.Progress)
			assert.Same(t, tt.app, got.App)
			assert.Same(t, tt.clientCfg, got.ClientConfig)
			assert.Same(t, tt.clientPol, got.ClientPolicy)
			assert.Same(t, tt.secret, got.SecretAgent)
			assert.Same(t, tt.awsS3, got.AwsS3)
		})
	}
}

func TestServerBackupServiceConfig_Validate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		cfg        func() *ServerBackupServiceConfig
		isBackup   bool
		wantErr    bool
		wantErrMsg string
	}{
		{
			name:     "valid config in backup mode",
			cfg:      validServerBackupServiceConfig,
			isBackup: true,
			wantErr:  false,
		},
		{
			name:     "valid config in restore mode",
			cfg:      validServerBackupServiceConfig,
			isBackup: false,
			wantErr:  false,
		},
		{
			name: "nil start skips start validation",
			cfg: func() *ServerBackupServiceConfig {
				cfg := validServerBackupServiceConfig()
				cfg.Start = nil
				return cfg
			},
			isBackup: true,
			wantErr:  false,
		},
		{
			name: "missing storage type",
			cfg: func() *ServerBackupServiceConfig {
				cfg := validServerBackupServiceConfig()
				cfg.Start.StorageType = ""
				return cfg
			},
			isBackup:   true,
			wantErr:    true,
			wantErrMsg: "storage-type is required",
		},
		{
			name: "nil list skips list validation",
			cfg: func() *ServerBackupServiceConfig {
				cfg := validServerBackupServiceConfig()
				cfg.List = nil
				return cfg
			},
			isBackup: true,
			wantErr:  false,
		},
		{
			name: "missing list path",
			cfg: func() *ServerBackupServiceConfig {
				cfg := validServerBackupServiceConfig()
				cfg.List.ListPath = ""
				return cfg
			},
			isBackup:   true,
			wantErr:    true,
			wantErrMsg: "list-path is required",
		},
		{
			name: "nil validation skips validation config check",
			cfg: func() *ServerBackupServiceConfig {
				cfg := validServerBackupServiceConfig()
				cfg.Validation = nil
				return cfg
			},
			isBackup: true,
			wantErr:  false,
		},
		{
			name: "missing validation backup id",
			cfg: func() *ServerBackupServiceConfig {
				cfg := validServerBackupServiceConfig()
				cfg.Validation.JobID = ""
				return cfg
			},
			isBackup:   true,
			wantErr:    true,
			wantErrMsg: "backup-id is required",
		},
		{
			name: "multiple cloud providers configured",
			cfg: func() *ServerBackupServiceConfig {
				cfg := validServerBackupServiceConfig()
				cfg.AwsS3 = &models.AwsS3{
					BucketName: testBucket,
					Region:     "us-west-2",
					ChunkSize:  5,
				}
				cfg.GcpStorage = &models.GcpStorage{
					BucketName:             testBucket,
					RetryBackoffMultiplier: 2,
					ChunkSize:              5,
				}
				return cfg
			},
			isBackup:   true,
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
