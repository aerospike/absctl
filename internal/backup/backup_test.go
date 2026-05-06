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

package backup

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path"
	"testing"
	"time"

	"github.com/aerospike/absctl/internal/config"
	"github.com/aerospike/absctl/internal/models"
	"github.com/aerospike/absctl/internal/storage"
	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/backup-go"
	"github.com/aerospike/tools-common-go/client"
	"github.com/stretchr/testify/require"
)

const (
	testNamespace       = "test"
	testSet             = "test"
	testSetXDR          = "test-xdr"
	testStateFile       = "state"
	testASLoginPassword = "admin"
	testDC              = "dc1"
	testXDRHost         = "172.17.0.1"
	testXDRPort         = 8066
	testAckQueueSize    = 256
	testResultQueueSize = 256
	testRewind          = "all"
	testHost            = "127.0.0.1"
	testPort            = 3000
)

func testHostPort() *client.HostTLSPort {
	return &client.HostTLSPort{
		Host: testHost,
		Port: testPort,
	}
}

func Test_BackupWithState(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	dir := path.Join(t.TempDir(), "plain")
	hostPort := testHostPort()

	asbParams := &config.BackupServiceConfig{
		ServiceConfigCommon: config.ServiceConfigCommon{
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
		},
		Backup: &models.Backup{
			StateFileDst: testStateFile,
			ScanPageSize: 10,
			FileLimit:    100000,
			Common: models.Common{
				Directory:                     dir,
				Namespace:                     testNamespace,
				Parallel:                      1,
				InfoMaxRetries:                3,
				InfoRetriesMultiplier:         1,
				InfoRetryIntervalMilliseconds: 1000,
			},
		},
	}

	err := createRecords(t, asbParams.ClientConfig, asbParams.ClientPolicy, testNamespace, testSet)
	require.NoError(t, err)

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	asb, err := NewService(ctx, asbParams, logger)
	require.NoError(t, err)

	err = asb.Run(ctx)
	require.NoError(t, err)
}

func Test_BackupXDR(t *testing.T) {
	// Do not parallel this test. We have multiply xdr tests, so they should be executed sequentially.
	ctx := t.Context()
	dir := path.Join(t.TempDir(), "xdr")
	hostPort := testHostPort()

	asbParams := &config.BackupServiceConfig{
		ServiceConfigCommon: config.ServiceConfigCommon{
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
		},
		BackupXDR: &models.BackupXDR{
			FileLimit:                     100000,
			InfoMaxRetries:                3,
			InfoRetriesMultiplier:         1,
			InfoRetryIntervalMilliseconds: 1000,
			Directory:                     dir,
			Namespace:                     testNamespace,
			DC:                            testDC,
			LocalAddress:                  testXDRHost,
			LocalPort:                     testXDRPort,
			MaxConnections:                10,
			Rewind:                        testRewind,
			InfoPolingPeriodMilliseconds:  100,
			ReadTimeoutMilliseconds:       10000,
			WriteTimeoutMilliseconds:      10000,
			ResultQueueSize:               testAckQueueSize,
			AckQueueSize:                  testResultQueueSize,
			StartTimeoutMilliseconds:      10000,
		},
	}

	err := createRecords(t, asbParams.ClientConfig, asbParams.ClientPolicy, testNamespace, testSetXDR)
	require.NoError(t, err)

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	asb, err := NewService(ctx, asbParams, logger)
	require.NoError(t, err)

	err = asb.Run(ctx)
	require.NoError(t, err)
}

func Test_BackupEstimates(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	hostPort := testHostPort()

	asbParams := &config.BackupServiceConfig{
		ServiceConfigCommon: config.ServiceConfigCommon{
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
			Encryption:  nil,
			SecretAgent: nil,
			AwsS3:       nil,
			GcpStorage:  nil,
			AzureBlob:   nil,
			Local:       nil,
		},
		Backup: &models.Backup{
			FileLimit: 100000,
			Common: models.Common{
				Namespace:                     testNamespace,
				Parallel:                      1,
				InfoMaxRetries:                3,
				InfoRetriesMultiplier:         1,
				InfoRetryIntervalMilliseconds: 1000,
			},
			Estimate:        true,
			EstimateSamples: 100,
		},
	}

	err := createRecords(t, asbParams.ClientConfig, asbParams.ClientPolicy, testNamespace, testSet)
	require.NoError(t, err)

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	asb, err := NewService(ctx, asbParams, logger)
	require.NoError(t, err)

	err = asb.Run(ctx)
	require.NoError(t, err)
}

func quietLogger() *slog.Logger {
	return slog.New(slog.DiscardHandler)
}

func newBackupCfg(b *models.Backup) *config.BackupServiceConfig {
	return &config.BackupServiceConfig{
		ServiceConfigCommon: config.ServiceConfigCommon{
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
		},
		Backup: b,
	}
}

func Test_RunNilService(t *testing.T) {
	t.Parallel()

	var s *Service
	require.NoError(t, s.Run(t.Context()))
}

func Test_ErrHumanize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		in        error
		want      error
		wantExact bool
	}{
		{
			name:      "node not found exact",
			in:        errors.New(models.ErrNodeNotFoundText),
			want:      models.ErrNodeNotFound,
			wantExact: true,
		},
		{
			name:      "node not found wrapped",
			in:        fmt.Errorf("rpc failure: %s: details", models.ErrNodeNotFoundText),
			want:      models.ErrNodeNotFound,
			wantExact: true,
		},
		{
			name: "unrelated error is passed through",
			in:   errors.New("some other failure"),
			want: errors.New("some other failure"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := errHumanize(tt.in)
			require.Error(t, got)

			if tt.wantExact {
				require.ErrorIs(t, got, models.ErrNodeNotFound)
			} else {
				require.Equal(t, tt.want.Error(), got.Error())
			}
		})
	}
}

func Test_GetInfoPolicies_BackupScan(t *testing.T) {
	t.Parallel()

	cfg := &config.BackupServiceConfig{
		Backup: &models.Backup{
			Common: models.Common{
				InfoTimeout:                   1500,
				InfoMaxRetries:                4,
				InfoRetriesMultiplier:         2.0,
				InfoRetryIntervalMilliseconds: 250,
			},
		},
	}

	info, retry := getInfoPolicies(cfg)
	require.NotNil(t, info)
	require.NotNil(t, retry)
	require.Equal(t, 1500*time.Millisecond, info.Timeout)
	require.Equal(t, 250*time.Millisecond, retry.BaseTimeout)
	require.InDelta(t, 2.0, retry.Multiplier, 0.0001)
	require.EqualValues(t, 4, retry.MaxRetries)
}

func Test_GetInfoPolicies_BackupXDR(t *testing.T) {
	t.Parallel()

	cfg := &config.BackupServiceConfig{
		BackupXDR: &models.BackupXDR{
			InfoTimeout:                   5000,
			InfoMaxRetries:                3,
			InfoRetriesMultiplier:         1.5,
			InfoRetryIntervalMilliseconds: 200,
		},
	}

	info, retry := getInfoPolicies(cfg)
	require.NotNil(t, info)
	require.NotNil(t, retry)
	require.Equal(t, 5*time.Second, info.Timeout)
	require.Equal(t, 200*time.Millisecond, retry.BaseTimeout)
	require.InDelta(t, 1.5, retry.Multiplier, 0.0001)
	require.EqualValues(t, 3, retry.MaxRetries)
}

func Test_GetInfoPolicies_XDRTakesPrecedence(t *testing.T) {
	t.Parallel()

	cfg := &config.BackupServiceConfig{
		Backup: &models.Backup{
			Common: models.Common{InfoTimeout: 1000},
		},
		BackupXDR: &models.BackupXDR{InfoTimeout: 7000},
	}

	info, _ := getInfoPolicies(cfg)
	require.Equal(t, 7*time.Second, info.Timeout,
		"BackupXDR config must be used when both Backup and BackupXDR are set")
}

func Test_NewService_InvalidRackList(t *testing.T) {
	t.Parallel()

	cfg := newBackupCfg(&models.Backup{
		FileLimit: 1,
		RackList:  "not-a-number",
		Common: models.Common{
			Namespace: testNamespace,
			Parallel:  1,
		},
	})

	svc, err := NewService(t.Context(), cfg, quietLogger())
	require.Error(t, err)
	require.Nil(t, svc)
	require.Contains(t, err.Error(), "rack")
}

func Test_NewService_NegativeRackID(t *testing.T) {
	t.Parallel()

	cfg := newBackupCfg(&models.Backup{
		FileLimit: 1,
		RackList:  "-1",
		Common: models.Common{
			Namespace: testNamespace,
			Parallel:  1,
		},
	})

	svc, err := NewService(t.Context(), cfg, quietLogger())
	require.Error(t, err)
	require.Nil(t, svc)
}

func Test_NewService_InvalidPartitionList(t *testing.T) {
	t.Parallel()

	cfg := newBackupCfg(&models.Backup{
		FileLimit:     1,
		PartitionList: "not-a-partition",
		Common: models.Common{
			Namespace: testNamespace,
			Parallel:  1,
		},
	})

	svc, err := NewService(t.Context(), cfg, quietLogger())
	require.Error(t, err)
	require.Nil(t, svc)
	require.Contains(t, err.Error(), "partition")
}

func Test_NewService_InvalidFilterExpression(t *testing.T) {
	t.Parallel()

	cfg := newBackupCfg(&models.Backup{
		FileLimit:        1,
		FilterExpression: "!!!not-base64!!!",
		Common: models.Common{
			Namespace: testNamespace,
			Parallel:  1,
		},
	})

	svc, err := NewService(t.Context(), cfg, quietLogger())
	require.Error(t, err)
	require.Nil(t, svc)
	require.Contains(t, err.Error(), "filter expression")
}

func Test_NewService_InvalidModifiedBefore(t *testing.T) {
	t.Parallel()

	cfg := newBackupCfg(&models.Backup{
		FileLimit:      1,
		ModifiedBefore: "not-a-date",
		Common: models.Common{
			Namespace: testNamespace,
			Parallel:  1,
		},
	})

	svc, err := NewService(t.Context(), cfg, quietLogger())
	require.Error(t, err)
	require.Nil(t, svc)
	require.Contains(t, err.Error(), "modified before")
}

func Test_NewService_InvalidModifiedAfter(t *testing.T) {
	t.Parallel()

	cfg := newBackupCfg(&models.Backup{
		FileLimit:     1,
		ModifiedAfter: "not-a-date",
		Common: models.Common{
			Namespace: testNamespace,
			Parallel:  1,
		},
	})

	svc, err := NewService(t.Context(), cfg, quietLogger())
	require.Error(t, err)
	require.Nil(t, svc)
	require.Contains(t, err.Error(), "modified after")
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
