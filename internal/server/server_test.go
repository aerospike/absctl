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
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/aerospike/absctl/internal/config"
	"github.com/aerospike/absctl/internal/models"
	backupmodels "github.com/aerospike/backup-go/models"
	"github.com/aerospike/backup-go/pkg/asinfo"
	infomodels "github.com/aerospike/backup-go/pkg/asinfo/models"
	"github.com/aerospike/backup-go/pkg/server/lister"
	servermodels "github.com/aerospike/backup-go/pkg/server/lister/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testBucket      = "asbackup"
	logBackupDone   = "backup complete"
	logNoRunningJob = "no running backup found"
	logBackupEntry  = "backup entry"
)

// errBoom stands for any failure that is not a missing job or a missing manifest.
var errBoom = errors.New("boom")

// statusAnswer is a single canned answer of fakeStatusGetter.
type statusAnswer struct {
	status *infomodels.ResponseBackupState
	err    error
}

// fakeStatusGetter replays canned answers, one per call.
type fakeStatusGetter struct {
	answers []statusAnswer
	calls   int
}

func (f *fakeStatusGetter) GetBackupStatus(
	_ context.Context, _ string,
) (*infomodels.ResponseBackupState, error) {
	if f.calls >= len(f.answers) {
		return nil, asinfo.ErrNotFound
	}

	answer := f.answers[f.calls]
	f.calls++

	return answer.status, answer.err
}

// fakeMetadataGetter answers "not found" a given number of times before it either
// returns the manifest or fails permanently.
type fakeMetadataGetter struct {
	notFound int
	md       servermodels.Metadata
	err      error
	calls    int
}

func (f *fakeMetadataGetter) GetMetadata(_ context.Context, _ string) (servermodels.Metadata, error) {
	f.calls++

	if f.calls <= f.notFound {
		return servermodels.Metadata{}, lister.ErrMetadataNotFound
	}

	if f.err != nil {
		return servermodels.Metadata{}, f.err
	}

	return f.md, nil
}

// newTestService builds a service that logs into the returned buffer and retries the
// manifest fast enough not to slow the tests down.
func newTestService(t *testing.T, watch bool) (*Service, *bytes.Buffer) {
	t.Helper()

	buf := &bytes.Buffer{}

	return &Service{
		backupCfg: &config.ServerBackupServiceConfig{
			Progress: &models.ServerBackupProgress{JobID: testJobID, Watch: watch},
			App:      &models.App{LogJSON: true},
			AwsS3:    &models.AwsS3{BucketName: testBucket},
		},
		logger:               slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelInfo})),
		metadataRetryPolicy:  backupmodels.NewRetryPolicy(time.Millisecond, 1.0, 3),
		vanishedPollInterval: time.Millisecond,
	}, buf
}

func testStatus(state infomodels.BackupState, pct float64) *infomodels.ResponseBackupState {
	return &infomodels.ResponseBackupState{
		JobID:       testJobID,
		Namespace:   testNamespace,
		State:       state,
		ProgressPct: pct,
		StartTime:   time.Now().Add(-time.Minute),
	}
}

func TestServiceReportBackupProgress(t *testing.T) {
	t.Parallel()

	md := servermodels.Metadata{BackupID: testJobID, Namespace: testNamespace}

	tests := []struct {
		name        string
		watch       bool
		answers     []statusAnswer
		metadata    *fakeMetadataGetter
		wantErr     error
		wantErrMsg  string
		wantCalls   int
		wantLogs    []string
		notWantLogs []string
	}{
		{
			name:      "reports a completed backup",
			watch:     true,
			answers:   []statusAnswer{{status: testStatus(infomodels.BackupStateComplete, 100)}},
			wantCalls: 1,
			wantLogs:  []string{logBackupDone, logBackupEntry},
		},
		{
			// The cluster drops the job state right after finishing the backup. The job
			// was seen running, so this is a completion and not a missing backup.
			name:  "treats a vanished job as a completed backup",
			watch: true,
			answers: []statusAnswer{
				{status: testStatus(infomodels.BackupStateIncrScanActive, 95)},
				{err: asinfo.ErrNotFound},
				{err: asinfo.ErrNotFound},
				{err: asinfo.ErrNotFound},
				{err: asinfo.ErrNotFound},
				{err: asinfo.ErrNotFound},
			},
			wantCalls:   6,
			wantLogs:    []string{logBackupDone, logBackupEntry},
			notWantLogs: []string{logNoRunningJob},
		},
		{
			// The status of a running job can be unavailable for a moment. Reporting
			// that gap as a completed backup is the bug this case guards against.
			name:  "keeps watching after a transient status gap",
			watch: true,
			answers: []statusAnswer{
				{status: testStatus(infomodels.BackupStateBaseScanActive, 3)},
				{err: asinfo.ErrNotFound},
				{status: testStatus(infomodels.BackupStateBaseScanActive, 47)},
				{status: testStatus(infomodels.BackupStateComplete, 100)},
			},
			wantCalls: 4,
			wantLogs:  []string{logBackupDone, logBackupEntry},
		},
		{
			// A job that is gone and left no manifest behind is reported as a failure.
			name:  "fails when a vanished job left no manifest",
			watch: true,
			answers: []statusAnswer{
				{status: testStatus(infomodels.BackupStateBaseScanActive, 3)},
				{err: asinfo.ErrNotFound},
				{err: asinfo.ErrNotFound},
				{err: asinfo.ErrNotFound},
				{err: asinfo.ErrNotFound},
				{err: asinfo.ErrNotFound},
			},
			metadata:   &fakeMetadataGetter{notFound: 10},
			wantErrMsg: "timed out waiting for backup metadata",
			wantCalls:  6,
		},
		{
			name:        "reports a job that was never running",
			watch:       true,
			answers:     []statusAnswer{{err: asinfo.ErrNotFound}},
			wantCalls:   1,
			wantLogs:    []string{logNoRunningJob, logBackupEntry},
			notWantLogs: []string{logBackupDone},
		},
		{
			name:      "fails on a failed backup",
			watch:     true,
			answers:   []statusAnswer{{status: testStatus(infomodels.BackupStateFailed, 42)}},
			wantErr:   errBackupFailed,
			wantCalls: 1,
		},
		{
			name:      "propagates an unexpected status error",
			watch:     true,
			answers:   []statusAnswer{{err: errBoom}},
			wantErr:   errBoom,
			wantCalls: 1,
		},
		{
			name:        "prints once when watching is disabled",
			watch:       false,
			answers:     []statusAnswer{{status: testStatus(infomodels.BackupStateBaseScanActive, 5)}},
			wantCalls:   1,
			wantLogs:    []string{"backup progress"},
			notWantLogs: []string{logBackupDone, logBackupEntry},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			svc, buf := newTestService(t, tt.watch)
			status := &fakeStatusGetter{answers: tt.answers}

			metadata := tt.metadata
			if metadata == nil {
				metadata = &fakeMetadataGetter{md: md}
			}

			err := svc.reportBackupProgress(t.Context(), status, metadata)

			switch {
			case tt.wantErr != nil:
				require.ErrorIs(t, err, tt.wantErr)
			case tt.wantErrMsg != "":
				require.ErrorContains(t, err, tt.wantErrMsg)
			default:
				require.NoError(t, err)
			}

			assert.Equal(t, tt.wantCalls, status.calls)

			for _, want := range tt.wantLogs {
				assert.Contains(t, buf.String(), want)
			}

			for _, notWant := range tt.notWantLogs {
				assert.NotContains(t, buf.String(), notWant)
			}
		})
	}
}

func TestServiceFetchMetadata(t *testing.T) {
	t.Parallel()

	md := servermodels.Metadata{BackupID: testJobID, Namespace: testNamespace}

	tests := []struct {
		name      string
		wait      bool
		getter    *fakeMetadataGetter
		wantErr   string
		wantCalls int
	}{
		{
			name:      "returns the manifest right away",
			wait:      true,
			getter:    &fakeMetadataGetter{md: md},
			wantCalls: 1,
		},
		{
			name:      "waits until the manifest is uploaded",
			wait:      true,
			getter:    &fakeMetadataGetter{notFound: 2, md: md},
			wantCalls: 3,
		},
		{
			name:      "gives up after the last attempt",
			wait:      true,
			getter:    &fakeMetadataGetter{notFound: 10, md: md},
			wantErr:   "timed out waiting for backup metadata",
			wantCalls: 3,
		},
		{
			name:      "does not retry a permanent failure",
			wait:      true,
			getter:    &fakeMetadataGetter{err: errBoom},
			wantErr:   "failed to get backup metadata",
			wantCalls: 1,
		},
		{
			name:      "does not wait when the job was never running",
			wait:      false,
			getter:    &fakeMetadataGetter{notFound: 1, md: md},
			wantErr:   "failed to get backup metadata",
			wantCalls: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			svc, _ := newTestService(t, true)

			got, err := svc.fetchMetadata(t.Context(), tt.getter, testJobID, tt.wait)

			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
				assert.Equal(t, md, got)
			}

			assert.Equal(t, tt.wantCalls, tt.getter.calls)
		})
	}
}

func TestServiceMetadataS3Config(t *testing.T) {
	t.Parallel()

	svc, _ := newTestService(t, false)

	cfg := svc.metadataS3Config()

	assert.Equal(t, testBucket, cfg.BucketName)
	assert.Equal(t, metadataRequestTimeout, cfg.RequestTimeout)
	// The shared configuration must stay untouched.
	assert.Equal(t, 0, svc.backupCfg.AwsS3.RequestTimeout)
}
