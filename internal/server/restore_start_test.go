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
	"encoding/json"
	"log/slog"
	"testing"

	"github.com/aerospike/absctl/internal/config"
	"github.com/aerospike/absctl/internal/models"
	infomodels "github.com/aerospike/backup-go/pkg/asinfo/models"
	servermodels "github.com/aerospike/backup-go/pkg/server/lister/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	// testRestoreParallelRead differs from the default, so a value lost on the way to the
	// request is caught.
	testRestoreParallelRead  = 32
	testRestoreParallelWrite = 16
	testRestoreRPS           = 1000
	testRestoreExtraTTL      = 3600
	testIncrementalPath      = "incr_dir"
)

// TestServiceStartRestore covers what the start does around the request: the checks
// that keep a restore from being requested, and whether the restore is followed.
func TestServiceStartRestore(t *testing.T) {
	t.Parallel()

	manifest, err := json.Marshal(servermodels.Metadata{
		BackupID:  testRequestBackupID,
		Namespace: testRequestNamespace,
		Status:    servermodels.BackupStatusComplete,
	})
	require.NoError(t, err)

	fuzzyComplete := restoreAnswer{status: testFinishedRestore(testFuzzyStatus(infomodels.RestoreStateComplete))}

	tests := []struct {
		name           string
		giveAsync      bool
		giveUnstable   bool
		giveStartErr   error
		answers        []restoreAnswer
		wantErr        error
		wantStartCalls int
		wantCalls      int
	}{
		{
			name:           "follows the restore until it ends",
			answers:        []restoreAnswer{fuzzyComplete},
			wantStartCalls: 1,
			wantCalls:      1,
		},
		{
			name:           "returns as soon as the restore is accepted when async",
			giveAsync:      true,
			wantStartCalls: 1,
		},
		{
			name:         "does not start on an unstable cluster",
			giveUnstable: true,
			wantErr:      errClusterNotStable,
		},
		{
			name:           "propagates a rejected start without following it",
			giveStartErr:   errBoom,
			wantErr:        errBoom,
			wantStartCalls: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			svc, _ := newTestService(t)
			svc.restoreCfg.Start = &models.ServerRestore{
				Namespace: testRequestNamespace,
				BackupIDs: testRequestBackupID,
				JobID:     testJobID,
				Path:      testBackupPath,
				Async:     tt.giveAsync,
			}
			svc.restoreCfg.AwsS3 = testRequestS3()
			svc.metadataS3Client = func(context.Context, *models.AwsS3) (s3API, error) {
				return &fakeManifestS3{manifest: manifest}, nil
			}

			client := &fakeRestoreInfoClient{
				answers:  tt.answers,
				unstable: tt.giveUnstable,
				startErr: tt.giveStartErr,
			}

			err := svc.startRestore(t.Context(), client, testLayout)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}

			assert.Equal(t, tt.wantStartCalls, client.startCalls)
			assert.Equal(t, tt.wantCalls, client.calls)

			// The job the start follows is the one it requested.
			if tt.wantStartCalls > 0 {
				require.NotNil(t, client.startRequest)
				assert.Equal(t, testJobID, client.startRequest.JobID)
			}
		})
	}
}

func TestServiceRestoreRequest(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		give *models.ServerRestore
		want func() *infomodels.RequestRestore
	}{
		{
			name: "job id defaults to backup id",
			give: &models.ServerRestore{
				Namespace:   testRequestNamespace,
				StorageType: testRequestStorage,
				BackupIDs:   testRequestBackupID,
			},
			want: func() *infomodels.RequestRestore {
				return &infomodels.RequestRestore{
					RequestCommon: testRequestCommon(),
					JobID:         testRequestBackupID,
					BackupIDs:     testRequestBackupID,
				}
			},
		},
		{
			// A restore without a policy sends no fuzzy only parameters: the server
			// refuses them with its default policy, strict.
			name: "cold restore",
			give: &models.ServerRestore{
				Namespace:        testRequestNamespace,
				StorageType:      testRequestStorage,
				BackupIDs:        testRequestBackupID,
				JobID:            testRequestJobID,
				Path:             testBackupPath,
				SetList:          testRequestSetList,
				FilterExp:        testRequestFilterExp,
				NoIndexes:        true,
				NoUDFs:           true,
				ParallelWrite:    testRestoreParallelWrite,
				RecordsPerSecond: testRestoreRPS,
				ExtraTTL:         testRestoreExtraTTL,

				ParallelRead: testRestoreParallelRead,
			},
			want: func() *infomodels.RequestRestore {
				common := testRequestCommon()
				common.Path = testBackupPath
				common.SetList = testRequestSetList
				common.FilterExp = testRequestFilterExp
				common.NoIndexes = new(true)
				common.NoUDFs = new(true)

				return &infomodels.RequestRestore{
					RequestCommon:          common,
					JobID:                  testRequestJobID,
					BackupIDs:              testRequestBackupID,
					RestoreWorkerThreads:   testRestoreParallelWrite,
					RestoreDownloadThreads: testRestoreParallelRead,
					RestoreRPS:             testRestoreRPS,
					ExtraTTL:               testRestoreExtraTTL,
				}
			},
		},
		{
			name: "fuzzy restore",
			give: &models.ServerRestore{
				Namespace:         testRequestNamespace,
				StorageType:       testRequestStorage,
				BackupIDs:         testRequestBackupID,
				JobID:             testRequestJobID,
				Policy:            string(infomodels.RestorePolicyFuzzy),
				AllowUnhosted:     true,
				ParallelWrite:     testRestoreParallelWrite,
				RecordsPerSecond:  testRestoreRPS,
				MaxInflight:       500,
				RetryBaseInterval: 2000,
				RetryMultiplier:   1.5,
				RetryMaxAttempts:  3,
				IgnoreRecordError: true,
				ApplyMetadataLast: true,

				ParallelRead: testRestoreParallelRead,
			},
			want: func() *infomodels.RequestRestore {
				return &infomodels.RequestRestore{
					RequestCommon:          testRequestCommon(),
					JobID:                  testRequestJobID,
					BackupIDs:              testRequestBackupID,
					RestoreWorkerThreads:   testRestoreParallelWrite,
					RestoreDownloadThreads: testRestoreParallelRead,
					RestoreRPS:             testRestoreRPS,
					Policy:                 infomodels.RestorePolicyFuzzy,
					AllowUnhosted:          new(true),
					MaxInflight:            500,
					RetryBaseIntervalMs:    new(2000),
					RetryMultiplier:        1.5,
					RetryMaxAttempts:       3,
					IgnoreRecordError:      new(true),
					ApplyMetadataLast:      new(true),
				}
			},
		},
		{
			// Zero is a valid retry base interval, so it is sent; apply-metadata-last is
			// refused for a restore with neither indexes nor UDFs even when false, so it
			// is left out unless set.
			name: "fuzzy restore with defaults",
			give: &models.ServerRestore{
				Namespace:   testRequestNamespace,
				StorageType: testRequestStorage,
				BackupIDs:   testRequestBackupID,
				JobID:       testRequestJobID,
				Policy:      string(infomodels.RestorePolicyFuzzy),
			},
			want: func() *infomodels.RequestRestore {
				return &infomodels.RequestRestore{
					RequestCommon:       testRequestCommon(),
					JobID:               testRequestJobID,
					BackupIDs:           testRequestBackupID,
					Policy:              infomodels.RestorePolicyFuzzy,
					AllowUnhosted:       new(false),
					RetryBaseIntervalMs: new(0),
					IgnoreRecordError:   new(false),
				}
			},
		},
		{
			// The flags and the YAML config fill the fuzzy only settings with their
			// defaults, which the strict policy must not send.
			name: "strict restore with fuzzy settings at their defaults",
			give: &models.ServerRestore{
				Namespace:         testRequestNamespace,
				StorageType:       testRequestStorage,
				BackupIDs:         testRequestBackupIDs,
				JobID:             testRequestJobID,
				Policy:            string(infomodels.RestorePolicyStrict),
				MaxInflight:       models.DefaultServerRestoreMaxInflight,
				RetryBaseInterval: models.DefaultServerRestoreRetryBaseInterval,
				RetryMultiplier:   models.DefaultServerRestoreRetryMultiplier,
				RetryMaxAttempts:  models.DefaultServerRestoreRetryMaxAttempts,
			},
			want: func() *infomodels.RequestRestore {
				return &infomodels.RequestRestore{
					RequestCommon: testRequestCommon(),
					JobID:         testRequestJobID,
					BackupIDs:     testRequestBackupIDs,
					Policy:        infomodels.RestorePolicyStrict,
				}
			},
		},
		{
			// The server takes the backup ids as one comma-separated parameter, so the
			// list reaches the request unchanged.
			name: "several backup ids",
			give: &models.ServerRestore{
				Namespace:   testRequestNamespace,
				StorageType: testRequestStorage,
				BackupIDs:   testRequestBackupIDs,
				JobID:       testRequestJobID,
			},
			want: func() *infomodels.RequestRestore {
				return &infomodels.RequestRestore{
					RequestCommon: testRequestCommon(),
					JobID:         testRequestJobID,
					BackupIDs:     testRequestBackupIDs,
				}
			},
		},
		{
			name: "incremental path is normalized",
			give: &models.ServerRestore{
				Namespace:       testRequestNamespace,
				StorageType:     testRequestStorage,
				BackupIDs:       testRequestBackupIDs,
				JobID:           testRequestJobID,
				IncrementalPath: "/" + testIncrementalPath + "/",
			},
			want: func() *infomodels.RequestRestore {
				return &infomodels.RequestRestore{
					RequestCommon:   testRequestCommon(),
					JobID:           testRequestJobID,
					BackupIDs:       testRequestBackupIDs,
					IncrementalPath: testIncrementalPath,
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			svc := &Service{restoreCfg: &config.ServerRestoreServiceConfig{Start: tt.give}}
			svc.restoreCfg.AwsS3 = testRequestS3()

			assert.Equal(t, tt.want(), svc.restoreRequest())
		})
	}
}

func TestServiceCheckBackupExistsReadsUnderPrefix(t *testing.T) {
	t.Parallel()

	manifest, err := json.Marshal(servermodels.Metadata{
		BackupID:  testRequestBackupID,
		Namespace: testRequestNamespace,
		Status:    servermodels.BackupStatusComplete,
	})
	require.NoError(t, err)

	tests := []struct {
		name       string
		givePrefix string
		wantKey    string
	}{
		{name: "no prefix", givePrefix: "", wantKey: testRequestBackupID + testManifestSuffix},
		{name: "prefix", givePrefix: testBackupPath, wantKey: testMetadataKey + testManifestSuffix},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := &fakeManifestS3{manifest: manifest}
			svc := &Service{logger: slog.New(slog.DiscardHandler)}

			err := svc.checkBackupExists(t.Context(), client, testBucket, tt.givePrefix, testRequestBackupID)
			require.NoError(t, err)

			assert.Equal(t, []string{tt.wantKey}, client.keys)
		})
	}
}

func TestServiceCheckBackupsExistReadsEveryBackup(t *testing.T) {
	t.Parallel()

	manifest, err := json.Marshal(servermodels.Metadata{
		BackupID:  testRequestBackupID,
		Namespace: testRequestNamespace,
		Status:    servermodels.BackupStatusComplete,
	})
	require.NoError(t, err)

	tests := []struct {
		name          string
		giveBackupIDs []string
		wantKeys      []string
	}{
		{
			name:          "single backup",
			giveBackupIDs: []string{testRequestBackupID},
			wantKeys:      []string{testRequestBackupID + testManifestSuffix},
		},
		{
			name:          "several backups",
			giveBackupIDs: []string{testRequestBackupID, testRequestBackupID2},
			wantKeys: []string{
				testRequestBackupID + testManifestSuffix,
				testRequestBackupID2 + testManifestSuffix,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := &fakeManifestS3{manifest: manifest}
			svc := &Service{logger: slog.New(slog.DiscardHandler)}

			err := svc.checkBackupsExist(t.Context(), client, testBucket, "", tt.giveBackupIDs)
			require.NoError(t, err)

			assert.Equal(t, tt.wantKeys, client.keys)
		})
	}
}

func TestServiceCheckRestoreBackupsExist(t *testing.T) {
	t.Parallel()

	manifest, err := json.Marshal(servermodels.Metadata{
		BackupID:  testRequestBackupID,
		Namespace: testRequestNamespace,
		Status:    servermodels.BackupStatusComplete,
	})
	require.NoError(t, err)

	tests := []struct {
		name                string
		giveBackupIDs       string
		giveIncrementalPath string
		wantKeys            []string
	}{
		{
			name:          "single backup",
			giveBackupIDs: testRequestBackupID,
			wantKeys:      []string{testMetadataKey + testManifestSuffix},
		},
		{
			name:          "incremental backups default to the path",
			giveBackupIDs: testRequestBackupIDs,
			wantKeys: []string{
				testMetadataKey + testManifestSuffix,
				testBackupPath + "/" + testRequestBackupID2 + testManifestSuffix,
			},
		},
		{
			name:                "incremental backups are read under the incremental path",
			giveBackupIDs:       testRequestBackupIDs,
			giveIncrementalPath: "/" + testIncrementalPath + "/",
			wantKeys: []string{
				testMetadataKey + testManifestSuffix,
				testIncrementalPath + "/" + testRequestBackupID2 + testManifestSuffix,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := &fakeManifestS3{manifest: manifest}
			svc := &Service{
				logger: slog.New(slog.DiscardHandler),
				restoreCfg: &config.ServerRestoreServiceConfig{
					Start: &models.ServerRestore{
						BackupIDs:       tt.giveBackupIDs,
						Path:            testBackupPath,
						IncrementalPath: tt.giveIncrementalPath,
					},
					AwsS3: testRequestS3(),
				},
			}

			require.NoError(t, svc.checkRestoreBackupsExist(t.Context(), client))

			assert.Equal(t, tt.wantKeys, client.keys)
		})
	}
}
