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

package backup

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path"
	"testing"
	"time"

	"github.com/aerospike/absctl/internal/config"
	"github.com/aerospike/absctl/internal/models"
	"github.com/aerospike/absctl/internal/storage"
	"github.com/aerospike/absctl/internal/testutil"
	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/backup-go"
	"github.com/aerospike/backup-go/io/encoding/asb"
	"github.com/aerospike/tools-common-go/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testNamespace       = "test"
	testSet             = "test"
	testStateFile       = "state"
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

func Test_BackupWithState(t *testing.T) {
	t.Parallel()
	testutil.RequireIntegration(t, testutil.ServiceAerospike)

	ctx := t.Context()
	dir := path.Join(t.TempDir(), "plain")
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
			StateFileDst:                  testStateFile,
			ScanPageSize:                  10,
			FileLimit:                     100000,
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

	asb, err := NewService(ctx, asbParams, logger)
	require.NoError(t, err)

	t.Cleanup(asb.Close)

	err = asb.Run(ctx)
	require.NoError(t, err)
}

func Test_BackupEstimates(t *testing.T) {
	t.Parallel()
	testutil.RequireIntegration(t, testutil.ServiceAerospike)

	ctx := t.Context()
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
		Encryption:  nil,
		SecretAgent: nil,
		AwsS3:       nil,
		GcpStorage:  nil,
		AzureBlob:   nil,
		Local:       nil,
		Backup: &models.Backup{
			FileLimit:                     100000,
			Namespace:                     testNamespace,
			Parallel:                      1,
			InfoMaxRetries:                3,
			InfoRetriesMultiplier:         1,
			InfoRetryIntervalMilliseconds: 1000,
			Estimate:                      true,
			EstimateSamples:               100,
		},
	}

	err := createRecords(t, asbParams.ClientConfig, asbParams.ClientPolicy, testNamespace, testSet)
	require.NoError(t, err)

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	asb, err := NewService(ctx, asbParams, logger)
	require.NoError(t, err)

	t.Cleanup(asb.Close)

	err = asb.Run(ctx)
	require.NoError(t, err)
}

// TestService_CloseReleasesClient checks that Close after Run leaves no goroutine of
// the Aerospike client running.
func TestService_CloseReleasesClient(t *testing.T) {
	// Not parallel: VerifyNoGoroutineLeaks needs the goroutines of this test alone.
	testutil.RequireIntegration(t, testutil.ServiceAerospike)
	testutil.VerifyNoGoroutineLeaks(t)

	cfg := newBackupCfg(&models.Backup{
		FileLimit:                     100000,
		Directory:                     t.TempDir(),
		Namespace:                     testNamespace,
		Parallel:                      1,
		InfoMaxRetries:                1,
		InfoRetriesMultiplier:         1,
		InfoRetryIntervalMilliseconds: 10,
	})

	svc, err := NewService(t.Context(), cfg, quietLogger())
	require.NoError(t, err)

	// Runs before the leak check, which was registered first, even if Run fails.
	t.Cleanup(svc.Close)

	require.NoError(t, svc.Run(t.Context()))
}

// TestService_CloseIsSafe checks that Close is a no-op where there is no client to release.
func TestService_CloseIsSafe(t *testing.T) {
	t.Parallel()

	var nilSvc *Service

	assert.NotPanics(t, nilSvc.Close)
	assert.NotPanics(t, (&Service{}).Close)
}

// TestNewService_ErrorReleasesStorage checks that NewService releases the storage
// clients of the writer and of the state reader it created when a later step fails.
func TestNewService_ErrorReleasesStorage(t *testing.T) {
	// Not parallel: VerifyNoGoroutineLeaks needs the goroutines of each subtest alone.
	const (
		bucket = "backup-bucket"
		dir    = "backup"
	)

	tests := []struct {
		name string
		// giveContinue makes NewService create a state reader as well.
		giveContinue string
		giveObjects  map[string][]byte
	}{
		{
			name: "writer",
		},
		{
			name:         "writer and state reader",
			giveContinue: testStateFile,
			giveObjects: map[string][]byte{
				dir + "/data" + asb.Extension: testutil.NewBackupFile(t, testNamespace, testSet),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Started before the leak check, so that the server outlives it and cannot
			// close the client connections itself.
			fakeS3 := testutil.NewFakeS3(t, bucket, tt.giveObjects)

			testutil.VerifyNoGoroutineLeaks(t)
			testutil.IsolateAWSConfig(t)

			cfg := newBackupCfg(&models.Backup{
				FileLimit: 1,
				Directory: dir,
				Namespace: testNamespace,
				Parallel:  1,
				Continue:  tt.giveContinue,
			})
			// The storage is set up first; the Aerospike connect then fails.
			cfg.ClientConfig.Seeds = client.HostTLSPortSlice{closedHostPort(t)}
			cfg.ClientConfig.User = ""
			cfg.ClientConfig.Password = ""
			cfg.AwsS3 = &models.AwsS3{
				BucketName:        bucket,
				Region:            "us-east-1",
				Endpoint:          fakeS3.URL,
				AccessKeyID:       "fake-key-id",
				SecretAccessKey:   "fake-secret",
				ChunkSize:         5,
				UploadConcurrency: 1,
			}

			svc, err := NewService(t.Context(), cfg, quietLogger())
			require.ErrorContains(t, err, "failed to create aerospike client")
			require.Nil(t, svc)
		})
	}
}

// closedHostPort returns a local address nothing listens on, so a connect to it is refused.
func closedHostPort(t *testing.T) *client.HostTLSPort {
	t.Helper()

	var lc net.ListenConfig

	ln, err := lc.Listen(t.Context(), "tcp", net.JoinHostPort(testHost, "0"))
	require.NoError(t, err)

	addr, ok := ln.Addr().(*net.TCPAddr)
	require.True(t, ok)

	require.NoError(t, ln.Close())

	return &client.HostTLSPort{Host: testHost, Port: addr.Port}
}

func quietLogger() *slog.Logger {
	return slog.New(slog.DiscardHandler)
}

func newBackupCfg(b *models.Backup) *config.BackupServiceConfig {
	return &config.BackupServiceConfig{
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
		Backup:      b,
	}
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

func Test_BackupInfoPolicies(t *testing.T) {
	t.Parallel()

	cfg := &config.BackupServiceConfig{
		Backup: &models.Backup{
			InfoTimeout:                   1500,
			InfoMaxRetries:                4,
			InfoRetriesMultiplier:         2.0,
			InfoRetryIntervalMilliseconds: 250,
		},
	}

	info, retry := cfg.Backup.InfoPolicy(), cfg.Backup.InfoRetryPolicy()
	require.NotNil(t, info)
	require.NotNil(t, retry)
	require.Equal(t, 1500*time.Millisecond, info.Timeout)
	require.Equal(t, 250*time.Millisecond, retry.BaseTimeout)
	require.InDelta(t, 2.0, retry.Multiplier, 0.0001)
	require.EqualValues(t, 4, retry.MaxRetries)
}

func Test_NewService_InvalidRackList(t *testing.T) {
	t.Parallel()

	cfg := newBackupCfg(&models.Backup{
		FileLimit: 1,
		RackList:  "not-a-number",
		Namespace: testNamespace,
		Parallel:  1,
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
		Namespace: testNamespace,
		Parallel:  1,
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
		Namespace:     testNamespace,
		Parallel:      1,
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
		Namespace:        testNamespace,
		Parallel:         1,
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
		Namespace:      testNamespace,
		Parallel:       1,
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
		Namespace:     testNamespace,
		Parallel:      1,
	})

	svc, err := NewService(t.Context(), cfg, quietLogger())
	require.Error(t, err)
	require.Nil(t, svc)
	require.Contains(t, err.Error(), "modified after")
}

func createRecords(t *testing.T, cfg *client.AerospikeConfig, cp *models.ClientPolicy, namespace, set string) error {
	t.Helper()

	aerospikeClient, err := storage.NewAerospikeClient(t.Context(), cfg, cp, nil, 0, slog.Default())
	if err != nil {
		return fmt.Errorf("failed to create aerospike client: %w", err)
	}

	defer aerospikeClient.Close()

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
