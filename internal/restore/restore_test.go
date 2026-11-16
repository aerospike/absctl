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

package restore

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	appBackup "github.com/aerospike/absctl/internal/backup"
	"github.com/aerospike/absctl/internal/config"
	"github.com/aerospike/absctl/internal/models"
	"github.com/aerospike/absctl/internal/storage"
	"github.com/aerospike/absctl/internal/testutil"
	"github.com/aerospike/aerospike-client-go/v8"
	atypes "github.com/aerospike/aerospike-client-go/v8/types"
	"github.com/aerospike/backup-go"
	"github.com/aerospike/backup-go/io/encoding/asb"
	bgmodels "github.com/aerospike/backup-go/models"
	"github.com/aerospike/tools-common-go/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testNamespace       = "test"
	testSet             = "test"
	testASLoginPassword = "admin"
	testHost            = "127.0.0.1"
	testPort            = 3000
)

func testHostPort() *client.HostTLSPort {
	return &client.HostTLSPort{
		Host: testHost,
		Port: testPort,
	}
}

// Test_BackupRestore one test for both so we can restore from just backed-up files.
func Test_BackupRestore(t *testing.T) {
	t.Parallel()
	testutil.RequireIntegration(t, testutil.ServiceAerospike)

	ctx := t.Context()
	dir := t.TempDir()
	hostPort := testHostPort()

	asbParams := &config.BackupServiceConfig{
		App: &models.App{},
		ClientConfig: &client.AerospikeConfig{
			Seeds: client.HostTLSPortSlice{
				hostPort,
			},
			User:     testASLoginPassword,
			Password: testASLoginPassword,
		},
		ClientPolicy: &models.ClientPolicy{
			Timeout:      1000,
			IdleTimeout:  1000,
			LoginTimeout: 1000,
		},
		Compression: &models.Compression{
			Mode: backup.CompressNone,
		},
		Backup: &models.Backup{
			Directory:                     dir,
			Namespace:                     testNamespace,
			Parallel:                      1,
			InfoMaxRetries:                3,
			InfoRetriesMultiplier:         1,
			InfoRetryIntervalMilliseconds: 1000,
		},
	}

	err := createRecords(t, asbParams.ClientConfig, asbParams.ClientPolicy, testNamespace, testSet)
	require.NoError(t, err)

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	asb, err := appBackup.NewService(ctx, asbParams, logger)
	require.NoError(t, err)

	err = asb.Run(ctx)
	require.NoError(t, err)

	asrParams := &config.RestoreServiceConfig{
		App: &models.App{},
		ClientConfig: &client.AerospikeConfig{
			Seeds: client.HostTLSPortSlice{
				hostPort,
			},
			User:     testASLoginPassword,
			Password: testASLoginPassword,
		},
		ClientPolicy: &models.ClientPolicy{
			Timeout:      1000,
			IdleTimeout:  1000,
			LoginTimeout: 1000,
		},
		Compression: &models.Compression{
			Mode: backup.CompressNone,
		},
		AwsS3: &models.AwsS3{
			RestorePollDuration: 1000,
		},
		AzureBlob: &models.AzureBlob{
			RestorePollDuration: 1000,
		},
		Restore: &models.Restore{
			BatchSize:                     1,
			MaxAsyncBatches:               1,
			Directory:                     dir,
			Namespace:                     testNamespace,
			Parallel:                      1,
			InfoMaxRetries:                3,
			InfoRetriesMultiplier:         1,
			InfoRetryIntervalMilliseconds: 1000,
		},
	}

	asr, err := NewService(ctx, asrParams, logger)
	require.NoError(t, err)

	err = asr.Run(ctx)
	require.NoError(t, err)
}

func createRecords(t *testing.T, cfg *client.AerospikeConfig, cp *models.ClientPolicy, namespace, set string) error {
	t.Helper()

	aerospikeClient, err := storage.NewAerospikeClient(cfg, cp, nil, 0, slog.Default())
	if err != nil {
		return fmt.Errorf("failed to create aerospike client: %w", err)
	}

	wp := aerospike.NewWritePolicy(0, 0)

	for i := range 10 {
		key, err := aerospike.NewKey(namespace, set, fmt.Sprintf("map-key-%d", i))
		if err != nil {
			return fmt.Errorf("failed to create aerospike key: %w", err)
		}

		bin := aerospike.NewBin("time", time.Now().Unix())

		if err = aerospikeClient.PutBins(wp, key, bin); err != nil {
			return fmt.Errorf("failed to create aerospike key: %w", err)
		}
	}

	return nil
}

func quietLogger() *slog.Logger {
	return slog.New(slog.DiscardHandler)
}

func newRestoreCfg(r *models.Restore) *config.RestoreServiceConfig {
	return &config.RestoreServiceConfig{
		App: &models.App{},
		ClientConfig: &client.AerospikeConfig{
			Seeds:    client.HostTLSPortSlice{testHostPort()},
			User:     testASLoginPassword,
			Password: testASLoginPassword,
		},
		ClientPolicy: &models.ClientPolicy{
			Timeout:      1000,
			IdleTimeout:  1000,
			LoginTimeout: 1000,
		},
		Compression: &models.Compression{Mode: backup.CompressNone},
		Restore:     r,
	}
}

func Test_NewService_ValidateOnly_EmptyDir(t *testing.T) {
	t.Parallel()

	cfg := newRestoreCfg(&models.Restore{
		ValidateOnly: true,
		Directory:    t.TempDir(),
		Namespace:    testNamespace,
		Parallel:     1,
	})

	svc, err := NewService(t.Context(), cfg, quietLogger())
	require.Error(t, err)
	require.Nil(t, svc)
	require.Contains(t, err.Error(), "restore reader")
}

const (
	keySmall = "small"
	keyBig   = "big"
	binName  = "b"
)

var rejectedTestKeys = []string{keySmall, keyBig}

// TestRestore_BatchRejectedRecordFails checks that a record rejected by the cluster
// fails a batch restore unless IgnoreRecordError is set.
func TestRestore_BatchRejectedRecordFails(t *testing.T) {
	t.Parallel()
	testutil.RequireIntegration(t, testutil.ServiceAerospike)

	tests := []struct {
		name              string
		giveSet           string
		ignoreRecordError bool
		wantErr           bool
		wantIgnored       uint64
	}{
		{
			name:    "fails without ignore record error",
			giveSet: "rejected_fail",
			wantErr: true,
		},
		{
			name:              "ignored with ignore record error",
			giveSet:           "rejected_ignore",
			ignoreRecordError: true,
			wantIgnored:       1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			writeRejectedRecordBackup(t, dir, tt.giveSet)
			// Records left by a previous run would be counted as fresher, not inserted.
			deleteRecords(t, tt.giveSet, rejectedTestKeys...)

			cfg := newRestoreCfg(&models.Restore{
				Directory:                     dir,
				Namespace:                     testNamespace,
				Parallel:                      1,
				BatchSize:                     10,
				MaxAsyncBatches:               1,
				IgnoreRecordError:             tt.ignoreRecordError,
				InfoMaxRetries:                1,
				InfoRetriesMultiplier:         1,
				InfoRetryIntervalMilliseconds: 10,
			})

			svc, err := NewService(t.Context(), cfg, quietLogger())
			require.NoError(t, err)

			if tt.wantErr {
				// Run is what the command executes; it must return the rejection.
				err = svc.Run(t.Context())

				var aerr aerospike.Error
				require.ErrorAs(t, err, &aerr)
				assert.True(t, aerr.Matches(atypes.RECORD_TOO_BIG), "unexpected error: %v", err)

				return
			}

			// Run does not expose stats, so start the restore directly to check counters.
			h, err := svc.backupClient.Restore(t.Context(), svc.config, svc.reader)
			require.NoError(t, err)

			require.NoError(t, h.Wait(t.Context()))
			assert.Equal(t, tt.wantIgnored, h.GetStats().GetRecordsIgnored())
			assert.Equal(t, uint64(1), h.GetStats().GetRecordsInserted())
		})
	}
}

// writeRejectedRecordBackup writes an .asb file with one small record and one
// record bigger than the server max record size into dir.
func writeRejectedRecordBackup(t *testing.T, dir, set string) {
	t.Helper()

	const bigBinSize = 9 << 20 // Above the 8 MiB server limit.

	newRecord := func(userKey string, bins aerospike.BinMap) *bgmodels.Record {
		key, err := aerospike.NewKey(testNamespace, set, userKey)
		require.NoError(t, err)

		return &bgmodels.Record{Record: &aerospike.Record{Key: key, Bins: bins, Generation: 1}}
	}

	records := []*bgmodels.Record{
		newRecord(keySmall, aerospike.BinMap{binName: 1}),
		newRecord(keyBig, aerospike.BinMap{binName: strings.Repeat("x", bigBinSize)}),
	}

	encoder := asb.NewEncoder(asb.NewEncoderConfig(testNamespace, false, false, bgmodels.SIndexInfo{}))
	data := encoder.GetHeader(true)

	for _, r := range records {
		var err error

		data, err = encoder.EncodeToken(bgmodels.NewRecordToken(r, 0, nil), data)
		require.NoError(t, err)
	}

	require.NoError(t, os.WriteFile(filepath.Join(dir, "rejected"+asb.Extension), data, 0o600))
}

// deleteRecords removes the records with the given user keys from set.
func deleteRecords(t *testing.T, set string, userKeys ...string) {
	t.Helper()

	cfg := newRestoreCfg(&models.Restore{})

	aerospikeClient, err := storage.NewAerospikeClient(cfg.ClientConfig, cfg.ClientPolicy, nil, 0, quietLogger())
	require.NoError(t, err)

	defer aerospikeClient.Close()

	for _, userKey := range userKeys {
		key, err := aerospike.NewKey(testNamespace, set, userKey)
		require.NoError(t, err)

		_, err = aerospikeClient.Delete(nil, key)
		require.NoError(t, err)
	}
}

func TestGetWarmUp(t *testing.T) {
	tests := []struct {
		name            string
		warmUp          int
		maxAsyncBatches int
		expected        int
	}{
		{
			name:            "Default case with zero warmUp",
			warmUp:          0,
			maxAsyncBatches: 10,
			expected:        11,
		},
		{
			name:            "Custom warmUp value",
			warmUp:          5,
			maxAsyncBatches: 10,
			expected:        5,
		},
		{
			name:            "Zero maxAsyncBatches with zero warmUp",
			warmUp:          0,
			maxAsyncBatches: 0,
			expected:        1,
		},
		{
			name:            "Large maxAsyncBatches with zero warmUp",
			warmUp:          0,
			maxAsyncBatches: 1000,
			expected:        1001,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GetWarmUp(tt.warmUp, tt.maxAsyncBatches)
			if result != tt.expected {
				t.Errorf("getWarmUp(%d, %d) = %d, want %d",
					tt.warmUp, tt.maxAsyncBatches, result, tt.expected)
			}
		})
	}
}
