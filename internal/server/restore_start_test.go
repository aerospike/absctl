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

	fuzzyComplete := restoreAnswer{status: testCompleteRestore(testFuzzyStatus(infomodels.RestoreStateComplete))}
	coldClearRequired := restoreAnswer{
		status: testClearStatus(infomodels.ClearStateRequired, infomodels.NamespaceStatePreparing),
	}
	coldRestoring := restoreAnswer{status: testColdStatus(infomodels.RestoreStateActive, infomodels.NamespaceStateRestoring)}
	coldComplete := restoreAnswer{
		status: testCompleteRestore(testColdStatus(infomodels.RestoreStateComplete, infomodels.NamespaceStateNone)),
	}

	// stoppingClearRequired is a cold restore some node already stopped, while another
	// node still asks for its storage to be cleared.
	stoppingClearRequired := testClearStatus(infomodels.ClearStateRequired, infomodels.NamespaceStatePreparing)
	stoppingClearRequired.State = infomodels.RestoreStateAborting

	// logStarted is the line that names the job id generated for a started restore, which
	// the other restore commands take.
	logStarted := "job-id=" + testGeneratedRestoreJobID

	tests := []struct {
		name            string
		givePolicy      string
		giveConsistency servermodels.AchievedConsistency
		giveAsync       bool
		giveUnstable    bool
		giveStartErr    error
		giveStartSent   bool
		answers         []restoreAnswer
		wantErr         error
		wantErrMsg      string
		wantStartCalls  int
		wantCalls       int
		// wantStatusJobID is the job the status lookups follow, the generated one.
		wantStatusJobID string
		wantLogs        []string
	}{
		{
			name:            "follows the restore until it ends",
			answers:         []restoreAnswer{fuzzyComplete},
			wantStartCalls:  1,
			wantCalls:       1,
			wantStatusJobID: testGeneratedRestoreJobID,
			wantLogs:        []string{logStarted},
		},
		{
			// The cold restore cannot start before the storage is cleared, which is left
			// to the user, so the command says how, waits for the clear and then follows
			// the restore to its end.
			name: "waits for a clear of a cold restore and follows it",
			answers: []restoreAnswer{
				coldClearRequired,
				coldRestoring,
				coldComplete,
			},
			wantStartCalls:  1,
			wantCalls:       3,
			wantStatusJobID: testGeneratedRestoreJobID,
			wantLogs:        []string{logStarted, logClearHint, logClearWait, msgRestoreComplete},
		},
		{
			// Clearing cannot help a job that is already stopping, so it is followed to
			// its end and reported as what it turns out to be.
			name: "follows a stopping cold restore instead of asking for a clear",
			answers: []restoreAnswer{
				{status: stoppingClearRequired},
				{status: testFailedRestore(testColdStatus(infomodels.RestoreStateFailed, infomodels.NamespaceStateFailed))},
			},
			wantErr:         errRestoreFailed,
			wantStartCalls:  1,
			wantCalls:       2,
			wantStatusJobID: testGeneratedRestoreJobID,
		},
		{
			// A strict restore cannot read a fuzzy backup, which the metadata tells
			// before the cluster is asked for anything.
			name:            "does not start a strict restore of a fuzzy backup",
			giveConsistency: servermodels.AchievedConsistencyFuzzy,
			wantErr:         errBackupConsistency,
		},
		{
			// The policy is matched in any case, as it is sent.
			name:            "starts a fuzzy restore of a fuzzy backup with the policy in upper case",
			givePolicy:      "FUZZY",
			giveConsistency: servermodels.AchievedConsistencyFuzzy,
			answers:         []restoreAnswer{fuzzyComplete},
			wantStartCalls:  1,
			wantCalls:       1,
			wantStatusJobID: testGeneratedRestoreJobID,
		},
		{
			name:           "returns as soon as the restore is accepted when async",
			giveAsync:      true,
			wantStartCalls: 1,
			wantLogs:       []string{logStarted},
		},
		{
			name:         "does not start on an unstable cluster",
			giveUnstable: true,
			wantErr:      errClusterNotStable,
		},
		{
			// A start whose answer was lost may have been accepted, and the generated id
			// is the only way to find the job.
			name:          "names the job id of a start that may have been accepted",
			giveStartErr:  errBoom,
			giveStartSent: true,
			wantErr:       errBoom,
			// The error tells how to check the job, with the values the user can copy.
			wantErrMsg: "check it with 'absctl snapshot-restore progress --namespace " + testRequestNamespace +
				" --job-id " + testGeneratedRestoreJobID + " ...'",
			wantStartCalls: 1,
		},
		{
			// A refused start comes with the generated job id too, but names no job.
			name:           "propagates a rejected start without following it",
			giveStartErr:   errBoom,
			wantErr:        errBoom,
			wantErrMsg:     "failed to start restore: " + errBoom.Error(),
			wantStartCalls: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			manifest, err := json.Marshal(servermodels.Metadata{
				BackupID:            testRequestBackupID,
				Namespace:           testRequestNamespace,
				Status:              servermodels.BackupStatusComplete,
				AchievedConsistency: tt.giveConsistency,
			})
			require.NoError(t, err)

			svc, buf := newTestService(t)
			svc.restoreCfg.Start = &models.ServerRestore{
				Namespace: testRequestNamespace,
				BackupIDs: testRequestBackupID,
				Path:      testBackupPath,
				Async:     tt.giveAsync,
			}
			svc.restoreCfg.Start.Policy = tt.givePolicy
			svc.restoreCfg.AwsS3 = testRequestS3()
			svc.metadataS3Client = func(context.Context, *models.AwsS3) (s3API, error) {
				return &fakeManifestS3{manifest: manifest}, nil
			}

			client := &fakeRestoreInfoClient{
				answers:   tt.answers,
				unstable:  tt.giveUnstable,
				startErr:  tt.giveStartErr,
				startSent: tt.giveStartSent,
			}

			err = svc.startRestore(t.Context(), client)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				require.ErrorContains(t, err, tt.wantErrMsg)
			} else {
				require.NoError(t, err)
			}

			assert.Equal(t, tt.wantStartCalls, client.startCalls)
			assert.Equal(t, tt.wantCalls, client.calls)
			assert.Equal(t, tt.wantStatusJobID, client.statusJobID)

			for _, want := range tt.wantLogs {
				assert.Contains(t, buf.String(), want)
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
			// A restore that sets no setting of a single policy sends none of them, for
			// the server to apply its defaults.
			name: "no setting of a single policy",
			give: &models.ServerRestore{
				Namespace: testRequestNamespace,
				BackupIDs: testRequestBackupID,
			},
			want: func() *infomodels.RequestRestore {
				return &infomodels.RequestRestore{
					RequestCommon: testRequestCommon(),
					BackupIDs:     testRequestBackupID,
				}
			},
		},
		{
			name: "cold restore",
			give: &models.ServerRestore{
				Namespace:        testRequestNamespace,
				BackupIDs:        testRequestBackupID,
				Path:             testBackupPath,
				SetList:          testRequestSetList,
				FilterExp:        testRequestFilterExp,
				NoIndexes:        true,
				NoUDFs:           true,
				ParallelWrite:    testRestoreParallelWrite,
				RecordsPerSecond: testRestoreRPS,
				ExtraTTL:         testRestoreExtraTTL,
				HydrateReplica:   new(true),

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
					BackupIDs:              testRequestBackupID,
					RestoreWorkerThreads:   testRestoreParallelWrite,
					RestoreDownloadThreads: testRestoreParallelRead,
					RestoreRPS:             testRestoreRPS,
					ExtraTTL:               testRestoreExtraTTL,
					HydrateReplica:         new(true),
				}
			},
		},
		{
			name: "fuzzy restore",
			give: &models.ServerRestore{
				Namespace:         testRequestNamespace,
				BackupIDs:         testRequestBackupID,
				Policy:            string(infomodels.RestorePolicyFuzzy),
				ParallelWrite:     testRestoreParallelWrite,
				RecordsPerSecond:  testRestoreRPS,
				MaxInflight:       new(500),
				RetryBaseInterval: new(2000),
				RetryMultiplier:   new(1.5),
				RetryMaxAttempts:  new(3),
				IgnoreRecordError: new(true),
				ApplyMetadataLast: new(true),

				ParallelRead: testRestoreParallelRead,
			},
			want: func() *infomodels.RequestRestore {
				return &infomodels.RequestRestore{
					RequestCommon:          testRequestCommon(),
					BackupIDs:              testRequestBackupID,
					RestoreWorkerThreads:   testRestoreParallelWrite,
					RestoreDownloadThreads: testRestoreParallelRead,
					RestoreRPS:             testRestoreRPS,
					Policy:                 infomodels.RestorePolicyFuzzy,
					MaxInflight:            500,
					RetryBaseIntervalMs:    new(2000),
					RetryMultiplier:        1.5,
					RetryMaxAttempts:       new(3),
					IgnoreRecordError:      new(true),
					ApplyMetadataLast:      new(true),
				}
			},
		},
		{
			// Zero is a valid retry base interval and retry count, and false a valid flag,
			// so each one given is sent as it is.
			name: "fuzzy restore with zero and false settings",
			give: &models.ServerRestore{
				Namespace:         testRequestNamespace,
				BackupIDs:         testRequestBackupID,
				Policy:            string(infomodels.RestorePolicyFuzzy),
				RetryBaseInterval: new(0),
				RetryMaxAttempts:  new(0),
				IgnoreRecordError: new(false),
			},
			want: func() *infomodels.RequestRestore {
				return &infomodels.RequestRestore{
					RequestCommon:       testRequestCommon(),
					BackupIDs:           testRequestBackupID,
					Policy:              infomodels.RestorePolicyFuzzy,
					RetryBaseIntervalMs: new(0),
					RetryMaxAttempts:    new(0),
					IgnoreRecordError:   new(false),
				}
			},
		},
		{
			// The policy is accepted in any case and sent in the lower case the server
			// expects.
			name: "policy in upper case",
			give: &models.ServerRestore{
				Namespace: testRequestNamespace,
				BackupIDs: testRequestBackupID,
				Policy:    "FUZZY",
			},
			want: func() *infomodels.RequestRestore {
				return &infomodels.RequestRestore{
					RequestCommon: testRequestCommon(),
					BackupIDs:     testRequestBackupID,
					Policy:        infomodels.RestorePolicyFuzzy,
				}
			},
		},
		{
			// The server takes the backup ids as one comma-separated parameter, so the
			// list reaches the request unchanged.
			name: "several backup ids",
			give: &models.ServerRestore{
				Namespace: testRequestNamespace,
				BackupIDs: testRequestBackupIDs,
			},
			want: func() *infomodels.RequestRestore {
				return &infomodels.RequestRestore{
					RequestCommon: testRequestCommon(),
					BackupIDs:     testRequestBackupIDs,
				}
			},
		},
		{
			name: "incremental path is normalized",
			give: &models.ServerRestore{
				Namespace:       testRequestNamespace,
				BackupIDs:       testRequestBackupIDs,
				IncrementalPath: "/" + testIncrementalPath + "/",
			},
			want: func() *infomodels.RequestRestore {
				return &infomodels.RequestRestore{
					RequestCommon:   testRequestCommon(),
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

			got, err := svc.restoreRequest()
			require.NoError(t, err)
			assert.Equal(t, tt.want(), got)
		})
	}
}

func TestServiceRestoreRequestWithoutObjectStorage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		awsS3 *models.AwsS3
	}{
		{name: "no s3 section"},
		{name: "empty s3 section", awsS3: &models.AwsS3{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			svc := &Service{restoreCfg: &config.ServerRestoreServiceConfig{
				Start: &models.ServerRestore{
					Namespace: testRequestNamespace,
					BackupIDs: testRequestBackupID,
				},
			}}
			svc.restoreCfg.AwsS3 = tt.awsS3

			got, err := svc.restoreRequest()
			require.ErrorContains(t, err, errMsgStorageNotConfigured)
			assert.Nil(t, got)
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

			md, err := svc.checkBackupExists(t.Context(), client, testBucket, tt.givePrefix, testRequestBackupID)
			require.NoError(t, err)

			assert.Equal(t, []string{tt.wantKey}, client.keys)
			assert.Equal(t, testRequestBackupID, md.BackupID)
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

			backups, err := svc.checkBackupsExist(t.Context(), client, testBucket, "", tt.giveBackupIDs)
			require.NoError(t, err)

			assert.Equal(t, tt.wantKeys, client.keys)
			assert.Len(t, backups, len(tt.giveBackupIDs))
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
		{
			// An incremental path that normalizes to nothing is not sent, so the server
			// reads the incremental backups under the path, and so does the check.
			name:                "incremental path that normalizes to nothing reads under the path",
			giveBackupIDs:       testRequestBackupIDs,
			giveIncrementalPath: "/",
			wantKeys: []string{
				testMetadataKey + testManifestSuffix,
				testBackupPath + "/" + testRequestBackupID2 + testManifestSuffix,
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

// TestServiceCheckRestoreBackupsConsistency pins down that the consistency of every
// backup of the restore is checked, the incremental ones included, before the cluster is
// asked for anything.
func TestServiceCheckRestoreBackupsConsistency(t *testing.T) {
	t.Parallel()

	manifestOf := func(t *testing.T, id string, consistency servermodels.AchievedConsistency) []byte {
		t.Helper()

		m, err := json.Marshal(servermodels.Metadata{
			BackupID:            id,
			Namespace:           testRequestNamespace,
			Status:              servermodels.BackupStatusComplete,
			AchievedConsistency: consistency,
		})
		require.NoError(t, err)

		return m
	}

	// No policy restores a fuzzy backup along with others.
	const errMsgOnItsOwn = "a fuzzy backup is restored on its own"

	tests := []struct {
		name       string
		givePolicy string
	}{
		{name: "strict policy"},
		{name: "fuzzy policy", givePolicy: string(infomodels.RestorePolicyFuzzy)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// The full backup is strict and the incremental one fuzzy, so only a check
			// that reads the incremental backup refuses the restore.
			client := &fakeManifestS3{manifestsByKey: map[string][]byte{
				testRequestBackupID + testManifestSuffix: manifestOf(
					t, testRequestBackupID, servermodels.AchievedConsistencyStrict),
				testRequestBackupID2 + testManifestSuffix: manifestOf(
					t, testRequestBackupID2, servermodels.AchievedConsistencyFuzzy),
			}}

			start := &models.ServerRestore{BackupIDs: testRequestBackupIDs}
			start.Policy = tt.givePolicy

			svc := &Service{
				logger: slog.New(slog.DiscardHandler),
				restoreCfg: &config.ServerRestoreServiceConfig{
					Start: start,
					AwsS3: testRequestS3(),
				},
			}

			err := svc.checkRestoreBackupsExist(t.Context(), client)
			require.ErrorIs(t, err, errBackupConsistency)
			require.ErrorContains(t, err, testRequestBackupID2)
			require.ErrorContains(t, err, errMsgOnItsOwn)
		})
	}
}

func TestCheckBackupsConsistency(t *testing.T) {
	t.Parallel()

	// The two refusals advise different remedies, so each is told apart by its own.
	const (
		errMsgRestoreWithFuzzy = "restore it with policy fuzzy"
		errMsgFuzzyOnItsOwn    = "a fuzzy backup is restored on its own"
	)

	strictBackup := servermodels.Metadata{
		BackupID:            testRequestBackupID,
		AchievedConsistency: servermodels.AchievedConsistencyStrict,
	}
	fuzzyBackup := servermodels.Metadata{
		BackupID:            testRequestBackupID2,
		AchievedConsistency: servermodels.AchievedConsistencyFuzzy,
	}
	// unknownBackup was written before the metadata recorded its consistency.
	unknownBackup := servermodels.Metadata{BackupID: testRequestBackupID}

	tests := []struct {
		name        string
		givePolicy  infomodels.RestorePolicy
		giveBackups []servermodels.Metadata
		wantErrMsg  string
	}{
		{
			name:        "strict restore of strict backups",
			givePolicy:  infomodels.RestorePolicyStrict,
			giveBackups: []servermodels.Metadata{strictBackup, strictBackup},
		},
		{
			name:        "default policy of a strict backup",
			giveBackups: []servermodels.Metadata{strictBackup},
		},
		{
			name:        "strict restore of a fuzzy backup",
			givePolicy:  infomodels.RestorePolicyStrict,
			giveBackups: []servermodels.Metadata{fuzzyBackup},
			wantErrMsg:  errMsgRestoreWithFuzzy,
		},
		{
			// An empty policy is the server default, strict.
			name:        "default policy of a fuzzy backup",
			giveBackups: []servermodels.Metadata{fuzzyBackup},
			wantErrMsg:  errMsgRestoreWithFuzzy,
		},
		{
			name:        "fuzzy restore of a single fuzzy backup",
			givePolicy:  infomodels.RestorePolicyFuzzy,
			giveBackups: []servermodels.Metadata{fuzzyBackup},
		},
		{
			name:        "fuzzy restore of strict backups",
			givePolicy:  infomodels.RestorePolicyFuzzy,
			giveBackups: []servermodels.Metadata{strictBackup, strictBackup},
		},
		{
			name:        "fuzzy restore of a fuzzy backup in a list",
			givePolicy:  infomodels.RestorePolicyFuzzy,
			giveBackups: []servermodels.Metadata{strictBackup, fuzzyBackup},
			wantErrMsg:  errMsgFuzzyOnItsOwn,
		},
		{
			// Switching the policy would not help a list, so that is not advised.
			name:        "strict restore of a fuzzy backup in a list",
			givePolicy:  infomodels.RestorePolicyStrict,
			giveBackups: []servermodels.Metadata{strictBackup, fuzzyBackup},
			wantErrMsg:  errMsgFuzzyOnItsOwn,
		},
		{
			// The cluster checks a backup whose metadata does not tell its consistency.
			name:        "unknown consistency",
			givePolicy:  infomodels.RestorePolicyStrict,
			giveBackups: []servermodels.Metadata{unknownBackup},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := checkBackupsConsistency(tt.givePolicy, tt.giveBackups)
			if tt.wantErrMsg != "" {
				require.ErrorIs(t, err, errBackupConsistency)
				require.ErrorContains(t, err, testRequestBackupID2)
				require.ErrorContains(t, err, tt.wantErrMsg)

				return
			}

			require.NoError(t, err)
		})
	}
}
