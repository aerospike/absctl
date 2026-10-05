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
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"strconv"
	"testing"

	"github.com/aerospike/absctl/internal/config"
	"github.com/aerospike/absctl/internal/models"
	infomodels "github.com/aerospike/backup-go/pkg/asinfo/models"
	servermodels "github.com/aerospike/backup-go/pkg/server/lister/models"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testRequestNamespace = "source-ns"
	testRequestStorage   = models.StorageTypeAwsS3
	testRequestRegion    = "eu-central-1"
	testRequestProfile   = "default"
	testRequestEndpoint  = "http://127.0.0.1:9000"
	testRequestSetList   = "set1,set2"
	testRequestFilterExp = "kwGTUQKkYmluMQE="
	testRequestBackupID  = "bkp-1"
	testRequestJobID     = "rst-1"
	testMetadataKey      = testBackupPath + "/" + testRequestBackupID
	testModifiedAfter    = "2024-01-01_00:00:00"
	testRequestBinList   = "bin1"
	testManifestSuffix   = "/metadata.json"
)

// testRequestS3 returns the storage settings the request tests share. The access keys
// are set to make sure they never reach a request.
func testRequestS3() *models.AwsS3 {
	return &models.AwsS3{
		BucketName:      testBucket,
		Region:          testRequestRegion,
		Profile:         testRequestProfile,
		Endpoint:        testRequestEndpoint,
		AccessKeyID:     "access",
		SecretAccessKey: "secret",
	}
}

// testRequestCommon returns the storage part of a request built from testRequestS3.
func testRequestCommon() infomodels.RequestCommon {
	return infomodels.RequestCommon{
		Namespace: testRequestNamespace,
		Storage:   testRequestStorage,
		Bucket:    testBucket,
		Region:    testRequestRegion,
		Profile:   testRequestProfile,
		Endpoint:  testRequestEndpoint,
		NoIndexes: new(false),
		NoUDFs:    new(false),
	}
}

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
				BackupID:    testRequestBackupID,
			},
			want: func() *infomodels.RequestRestore {
				return &infomodels.RequestRestore{
					RequestCommon:     testRequestCommon(),
					JobID:             testRequestBackupID,
					BackupID:          testRequestBackupID,
					FuzzyRestore:      new(false),
					AllowUnhosted:     new(false),
					IgnoreRecordError: new(false),
				}
			},
		},
		{
			name: "cold restore",
			give: &models.ServerRestore{
				Namespace:   testRequestNamespace,
				StorageType: testRequestStorage,
				BackupID:    testRequestBackupID,
				JobID:       testRequestJobID,
				Path:        testBackupPath,
				SetList:     testRequestSetList,
				FilterExp:   testRequestFilterExp,
				NoIndexes:   true,
				NoUDFs:      true,
			},
			want: func() *infomodels.RequestRestore {
				common := testRequestCommon()
				common.Path = testBackupPath
				common.SetList = testRequestSetList
				common.FilterExp = testRequestFilterExp
				common.NoIndexes = new(true)
				common.NoUDFs = new(true)

				return &infomodels.RequestRestore{
					RequestCommon:     common,
					JobID:             testRequestJobID,
					BackupID:          testRequestBackupID,
					FuzzyRestore:      new(false),
					AllowUnhosted:     new(false),
					IgnoreRecordError: new(false),
				}
			},
		},
		{
			name: "fuzzy restore",
			give: &models.ServerRestore{
				Namespace:         testRequestNamespace,
				StorageType:       testRequestStorage,
				BackupID:          testRequestBackupID,
				JobID:             testRequestJobID,
				FuzzyRestore:      true,
				AllowUnhosted:     true,
				Parallel:          16,
				RecordsPerSecond:  1000,
				MaxInflight:       500,
				RetryBaseInterval: 2000,
				RetryMultiplier:   1.5,
				RetryMaxAttempts:  3,
				IgnoreRecordError: true,
			},
			want: func() *infomodels.RequestRestore {
				return &infomodels.RequestRestore{
					RequestCommon:       testRequestCommon(),
					JobID:               testRequestJobID,
					BackupID:            testRequestBackupID,
					FuzzyRestore:        new(true),
					AllowUnhosted:       new(true),
					Parallel:            16,
					RecordsPerSecond:    1000,
					MaxInflight:         500,
					RetryBaseIntervalMs: 2000,
					RetryMultiplier:     1.5,
					RetryMaxAttempts:    3,
					IgnoreRecordError:   new(true),
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

func TestServicePrepareRestoreRequest(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name               string
		giveHydrateReplica bool
	}{
		{name: "all replicas", giveHydrateReplica: true},
		{name: "master only", giveHydrateReplica: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			svc := &Service{restoreCfg: &config.ServerRestoreServiceConfig{
				Prepare: &models.ServerRestorePrepare{
					Namespace:      testRequestNamespace,
					JobID:          testRequestJobID,
					HydrateReplica: tt.giveHydrateReplica,
				},
			}}

			assert.Equal(t, &infomodels.RequestPrepareRestore{
				Namespace:      testRequestNamespace,
				JobID:          testRequestJobID,
				HydrateReplica: new(tt.giveHydrateReplica),
			}, svc.prepareRestoreRequest())
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

// recordingMetadataGetter records the keys the manifests are read with.
type recordingMetadataGetter struct {
	keys []string
}

func (r *recordingMetadataGetter) GetMetadata(_ context.Context, key string) (servermodels.Metadata, error) {
	r.keys = append(r.keys, key)

	return servermodels.Metadata{}, nil
}

func TestPrefixedMetadataGetter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		givePrefix string
		wantKey    string
	}{
		{name: "no prefix", givePrefix: "", wantKey: testRequestBackupID},
		{name: "prefix", givePrefix: testBackupPath, wantKey: testMetadataKey},
		{name: "prefix with trailing slash", givePrefix: testBackupPath + "/", wantKey: testMetadataKey},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := &recordingMetadataGetter{}
			g := prefixedMetadataGetter{metadataGetter: rec, prefix: tt.givePrefix}

			_, err := g.GetMetadata(t.Context(), testRequestBackupID)
			require.NoError(t, err)

			assert.Equal(t, []string{tt.wantKey}, rec.keys)
		})
	}
}

// fakeManifestS3 serves a single complete manifest and records the object keys read.
type fakeManifestS3 struct {
	manifest []byte
	keys     []string
}

func (f *fakeManifestS3) ListObjectsV2(
	context.Context, *s3.ListObjectsV2Input, ...func(*s3.Options),
) (*s3.ListObjectsV2Output, error) {
	return &s3.ListObjectsV2Output{}, nil
}

func (f *fakeManifestS3) GetObject(
	_ context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options),
) (*s3.GetObjectOutput, error) {
	f.keys = append(f.keys, aws.ToString(in.Key))

	return &s3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader(f.manifest))}, nil
}

func TestServiceCheckBackupExistsReadsUnderPrefix(t *testing.T) {
	t.Parallel()

	manifest, err := json.Marshal(servermodels.Metadata{
		BackupID:  testRequestBackupID,
		Namespace: testRequestNamespace,
		Status:    servermodels.MetadataStatusComplete,
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
