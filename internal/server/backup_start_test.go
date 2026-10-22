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
	"log/slog"
	"strconv"
	"testing"

	"github.com/aerospike/absctl/internal/config"
	"github.com/aerospike/absctl/internal/models"
	infomodels "github.com/aerospike/backup-go/pkg/asinfo/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testModifiedAfter  = "2024-01-01_00:00:00"
	testRequestBinList = "bin1"
	testBackupRPS      = 5000
	testParallelRead   = 8
	testParallelWrite  = 4
	testS3Throughput   = 10
)

func TestServiceBackupRequest(t *testing.T) {
	t.Parallel()

	modifiedAfter, err := models.ParseLocalTimeToUTC(testModifiedAfter)
	require.NoError(t, err)

	tests := []struct {
		name    string
		give    *models.ServerBackup
		want    func() *infomodels.RequestBackup
		wantErr bool
	}{
		{
			name: "defaults send the bools as false",
			give: &models.ServerBackup{
				Namespace:   testRequestNamespace,
				StorageType: testRequestStorage,
			},
			want: func() *infomodels.RequestBackup {
				return &infomodels.RequestBackup{RequestCommon: testRequestCommon()}
			},
		},
		{
			name: "every field",
			give: &models.ServerBackup{
				Namespace:     testRequestNamespace,
				StorageType:   testRequestStorage,
				Path:          testBackupPath,
				ModifiedAfter: testModifiedAfter,
				SetList:       testRequestSetList,
				BinList:       testRequestBinList,
				FilterExp:     testRequestFilterExp,
				NoIndexes:     true,
				NoUDFs:        true,
				Policy:        string(infomodels.BackupPolicyBestEffort),

				RecordsPerSecond: testBackupRPS,
				ParallelRead:     testParallelRead,
				ParallelWrite:    testParallelWrite,
				S3Throughput:     testS3Throughput,
			},
			want: func() *infomodels.RequestBackup {
				common := testRequestCommon()
				common.Path = testBackupPath
				common.SetList = testRequestSetList
				common.FilterExp = testRequestFilterExp
				common.NoIndexes = new(true)
				common.NoUDFs = new(true)

				return &infomodels.RequestBackup{
					RequestCommon: common,
					BinList:       testRequestBinList,
					ModifiedAfter: strconv.FormatInt(modifiedAfter.Unix(), 10),
					Policy:        infomodels.BackupPolicyBestEffort,

					BackupRPS:                    testBackupRPS,
					BackupWorkerThreads:          testParallelRead,
					BackupUploadThreads:          testParallelWrite,
					BackupS3ThroughputTargetGbps: testS3Throughput,
				}
			},
		},
		{
			name: "strict policy",
			give: &models.ServerBackup{
				Namespace:   testRequestNamespace,
				StorageType: testRequestStorage,
				Policy:      string(infomodels.BackupPolicyStrict),
			},
			want: func() *infomodels.RequestBackup {
				return &infomodels.RequestBackup{
					RequestCommon: testRequestCommon(),
					Policy:        infomodels.BackupPolicyStrict,
				}
			},
		},
		{
			name: "fuzzy policy",
			give: &models.ServerBackup{
				Namespace:   testRequestNamespace,
				StorageType: testRequestStorage,
				Policy:      string(infomodels.BackupPolicyFuzzy),
			},
			want: func() *infomodels.RequestBackup {
				return &infomodels.RequestBackup{
					RequestCommon: testRequestCommon(),
					Policy:        infomodels.BackupPolicyFuzzy,
				}
			},
		},
		{
			name: "invalid modified before",
			give: &models.ServerBackup{
				Namespace:      testRequestNamespace,
				StorageType:    testRequestStorage,
				ModifiedBefore: "not-a-date",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			svc := &Service{backupCfg: &config.ServerBackupServiceConfig{Start: tt.give}}
			svc.backupCfg.AwsS3 = testRequestS3()

			got, err := svc.backupRequest()
			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want(), got)
		})
	}
}

// stubMetadataS3Client is a metadataS3Client that hands out a client without opening
// anything, so the listers can be built in a test.
func stubMetadataS3Client(_ context.Context, _ *models.AwsS3) (s3API, error) {
	return &fakeManifestS3{}, nil
}

func TestServiceStartedBackupMetadataLister(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		giveAsync  bool
		givePath   string
		wantLister bool
		wantPrefix string
	}{
		{
			name:      "async start reads no manifest",
			giveAsync: true,
			givePath:  testBackupPath,
		},
		{
			name:       "no path",
			wantLister: true,
		},
		{
			name:       "path",
			givePath:   testBackupPath,
			wantLister: true,
			wantPrefix: testBackupPath,
		},
		{
			name:       "path is normalized",
			givePath:   "/" + testBackupPath + "/",
			wantLister: true,
			wantPrefix: testBackupPath,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			svc := &Service{
				backupCfg: &config.ServerBackupServiceConfig{
					Start: &models.ServerBackup{Path: tt.givePath, Async: tt.giveAsync},
					AwsS3: testRequestS3(),
				},
				logger:           slog.New(slog.DiscardHandler),
				metadataS3Client: stubMetadataS3Client,
			}

			l, err := svc.startedBackupMetadataLister(t.Context())
			require.NoError(t, err)

			if !tt.wantLister {
				assert.Nil(t, l)
				return
			}

			g, ok := l.(prefixedMetadataGetter)
			require.True(t, ok)
			assert.Equal(t, tt.wantPrefix, g.prefix)
		})
	}
}
