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
	testServerJobID     = "261025T120000-ab12"
	testServerListPath  = "/backups"
)

// validServerObjectStorage returns the S3 section a server-integrated command
// needs to pass validation in both backup and restore mode.
func validServerObjectStorage() *models.AwsS3 {
	return &models.AwsS3{
		BucketName:          testBucket,
		RestorePollDuration: 1,
		ChunkSize:           5,
		RetryReadBackoff:    100,
		RetryReadMultiplier: 2,
	}
}

func validServerBackupServiceConfig() *ServerBackupServiceConfig {
	return &ServerBackupServiceConfig{
		Start: &models.ServerBackup{
			Namespace: testServerNamespace,
		},
		List: &models.ServerBackupList{
			Path: testServerListPath,
		},
		Validation: &models.ServerBackupValidate{
			JobID: testServerJobID,
		},
		App:          &models.App{},
		ClientConfig: &client.AerospikeConfig{},
		ClientPolicy: &models.ClientPolicy{},
		Encryption:   &models.Encryption{},
		Compression:  &models.Compression{},
		AwsS3:        validServerObjectStorage(),
	}
}

func TestNewServerBackupServiceConfig(t *testing.T) {
	t.Parallel()

	var (
		start        = &models.ServerBackup{}
		list         = &models.ServerBackupList{}
		validation   = &models.ServerBackupValidate{}
		progress     = &models.ServerBackupProgress{}
		abort        = &models.ServerBackupAbort{}
		app          = &models.App{}
		clientCfg    = &client.AerospikeConfig{}
		clientPolicy = &models.ClientPolicy{}
		secretAgent  = &models.SecretAgent{}
		s3           = &models.SnapshotS3{BucketName: testBucket, Throughput: 10}
	)

	tests := []struct {
		name       string
		start      *models.ServerBackup
		list       *models.ServerBackupList
		validation *models.ServerBackupValidate
		progress   *models.ServerBackupProgress
		abort      *models.ServerBackupAbort
		app        *models.App
		clientCfg  *client.AerospikeConfig
		clientPol  *models.ClientPolicy
		secret     *models.SecretAgent
		s3         *models.SnapshotS3
	}{
		{
			name:       "all fields set",
			start:      start,
			list:       list,
			validation: validation,
			progress:   progress,
			abort:      abort,
			app:        app,
			clientCfg:  clientCfg,
			clientPol:  clientPolicy,
			secret:     secretAgent,
			s3:         s3,
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
			name: "only cloud storage fields set",
			s3:   s3,
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
				tt.abort,
				tt.app,
				tt.clientCfg,
				tt.clientPol,
				tt.secret,
				tt.s3,
			)

			require.NotNil(t, got)

			assert.Same(t, tt.start, got.Start)
			assert.Same(t, tt.list, got.List)
			assert.Same(t, tt.validation, got.Validation)
			assert.Same(t, tt.progress, got.Progress)
			assert.Same(t, tt.abort, got.Abort)
			assert.Same(t, tt.app, got.App)
			assert.Same(t, tt.clientCfg, got.ClientConfig)
			assert.Same(t, tt.clientPol, got.ClientPolicy)
			assert.Same(t, tt.secret, got.SecretAgent)
			// The local S3 client configuration and the throughput are taken from
			// the snapshot one.
			assert.Equal(t, tt.s3.ToAwsS3(), got.AwsS3)

			var wantThroughput int
			if tt.s3 != nil {
				wantThroughput = tt.s3.Throughput
			}

			assert.Equal(t, wantThroughput, got.S3Throughput)
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
			name: "empty object storage",
			cfg: func() *ServerBackupServiceConfig {
				cfg := validServerBackupServiceConfig()
				cfg.AwsS3.BucketName = ""
				return cfg
			},
			isBackup:   true,
			wantErr:    true,
			wantErrMsg: testErrMsgStorageNotConfigured,
		},
		{
			name: "missing bucket name",
			cfg: func() *ServerBackupServiceConfig {
				cfg := validServerBackupServiceConfig()
				cfg.AwsS3.BucketName = ""
				cfg.AwsS3.Endpoint = testEndpoint
				return cfg
			},
			isBackup:   true,
			wantErr:    true,
			wantErrMsg: testErrMsgBucketRequired,
		},
		{
			name: "nil object storage",
			cfg: func() *ServerBackupServiceConfig {
				cfg := validServerBackupServiceConfig()
				cfg.AwsS3 = nil
				return cfg
			},
			isBackup:   true,
			wantErr:    true,
			wantErrMsg: testErrMsgStorageNotConfigured,
		},
		{
			name: "valid abort config without object storage",
			cfg: func() *ServerBackupServiceConfig {
				return &ServerBackupServiceConfig{
					Abort: &models.ServerBackupAbort{
						JobID: testServerJobID,
					},
					App:          &models.App{},
					ClientConfig: &client.AerospikeConfig{},
					ClientPolicy: &models.ClientPolicy{},
				}
			},
			isBackup: true,
			wantErr:  false,
		},
		{
			name: "missing abort backup id",
			cfg: func() *ServerBackupServiceConfig {
				return &ServerBackupServiceConfig{
					Abort: &models.ServerBackupAbort{},
					App:   &models.App{},
				}
			},
			isBackup:   true,
			wantErr:    true,
			wantErrMsg: "backup-id is required",
		},
		{
			name: "s3 throughput at the limit",
			cfg: func() *ServerBackupServiceConfig {
				cfg := validServerBackupServiceConfig()
				cfg.S3Throughput = 400
				return cfg
			},
			isBackup: true,
		},
		{
			name: "s3 throughput above range",
			cfg: func() *ServerBackupServiceConfig {
				cfg := validServerBackupServiceConfig()
				cfg.S3Throughput = 401
				return cfg
			},
			isBackup:   true,
			wantErr:    true,
			wantErrMsg: "s3-throughput must be between 0 and 400, got 401",
		},
		{
			// Only start sends the throughput to the server.
			name: "s3 throughput above range without start",
			cfg: func() *ServerBackupServiceConfig {
				cfg := validServerBackupServiceConfig()
				cfg.Start = nil
				cfg.S3Throughput = 401
				return cfg
			},
			isBackup: true,
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
