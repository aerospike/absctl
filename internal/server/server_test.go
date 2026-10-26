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
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/aerospike/absctl/internal/config"
	"github.com/aerospike/absctl/internal/models"
	"github.com/aerospike/backup-go/mocks"
	backupmodels "github.com/aerospike/backup-go/models"
	"github.com/aerospike/backup-go/pkg/asinfo"
	infomodels "github.com/aerospike/backup-go/pkg/asinfo/models"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testBucket     = "asbackup"
	testBackupPath = "backups/daily"
	logBackupEntry = "backup entry"
	logOpenHint    = "released with"

	// testBackupDuration is how long a finished job ran in the tests, and logDuration is
	// how the completion line reports it.
	testBackupDuration = time.Minute
	logDuration        = "duration=1m0s"

	// testErrorReason stands for the reason the cluster gives for a terminal state.
	testErrorReason = "storage unreachable"

	// testLayoutNodes is the number of nodes the cold restores of the tests are spread
	// over.
	testLayoutNodes = 3

	testJobID     = "527069025"
	testNamespace = "source-ns1"

	testRequestNamespace = "source-ns"
	testRequestStorage   = models.StorageTypeAwsS3
	// errMsgBucketRequired is the error of a server-integrated command without a bucket.
	errMsgBucketRequired = "s3-bucket-name is required"
	testRequestRegion    = "eu-central-1"
	testRequestProfile   = "default"
	testRequestEndpoint  = "http://127.0.0.1:9000"
	testRequestSetList   = "set1,set2"
	testRequestFilterExp = "kwGTUQKkYmluMQE="
	// Backup ids have the job id form, the only one a manifest is looked up by.
	testRequestBackupID  = "261025T120000-ab12"
	testRequestBackupID2 = "261025T130000-cd34"
	// testRequestBackupIDs is the list form a multi-backup restore is given.
	testRequestBackupIDs = testRequestBackupID + "," + testRequestBackupID2
	testRequestJobID     = "rst-1"
	testMetadataKey      = testBackupPath + "/" + testRequestBackupID
	testManifestSuffix   = "/metadata.json"
)

// errBoom stands for any failure that is not a missing job or a missing manifest.
var errBoom = errors.New("boom")

// statusAnswer is a single canned answer of fakeInfoClient.
type statusAnswer struct {
	status *infomodels.ResponseBackupState
	err    error
}

// fakeInfoClient replays canned answers to the status lookups, one per call, and keeps
// reporting the job as missing once they run out - unless repeatLast is set, which makes
// the last answer stand forever, for the loops that only a cancellation can end. Every call is
// counted, so that a test which expects a loop to stop also proves that it stopped
// polling.
//
// The embedded mock supplies the rest of backup.ServerBackupInfo: reaching one of those
// methods means the code under test called something it has no business calling, and the
// mock panics instead of quietly answering.
type fakeInfoClient struct {
	mocks.MockServerBackupInfo

	answers    []statusAnswer
	repeatLast bool
	calls      int

	abortErr   error
	abortJobID string
	abortCalls int
}

func (f *fakeInfoClient) GetBackupStatus(
	_ context.Context, _ string,
) (*infomodels.ResponseBackupState, error) {
	call := f.calls
	f.calls++

	if call >= len(f.answers) {
		if !f.repeatLast || len(f.answers) == 0 {
			return nil, asinfo.ErrNotFound
		}

		call = len(f.answers) - 1
	}

	return f.answers[call].status, f.answers[call].err
}

func (f *fakeInfoClient) AbortBackup(_ context.Context, jobID string) error {
	f.abortCalls++
	f.abortJobID = jobID

	return f.abortErr
}

// restoreAnswer is a single canned answer of fakeRestoreInfoClient.
type restoreAnswer struct {
	status *infomodels.ResponseRestoreState
	err    error
}

// fakeRestoreInfoClient is the restore counterpart of fakeInfoClient: it replays canned
// restore status answers, one per call, and reports the job as missing once they run
// out - unless repeatLast is set, which makes the last answer stand forever, for the
// loops that only a cancellation can end.
type fakeRestoreInfoClient struct {
	mocks.MockServerBackupInfo

	answers    []restoreAnswer
	repeatLast bool
	calls      int

	abortErr       error
	abortNamespace string
	abortJobID     string
	abortCalls     int

	openErr       error
	openNamespace string
	openJobID     string
	openCalls     int

	// clearErrs are the answers to the clear requests, one per call; the requests past
	// them succeed.
	clearErrs      []error
	clearNamespace string
	clearJobID     string
	clearCalls     int

	// unstable reports migrations in flight, so the zero value is a stable cluster.
	unstable bool

	startErr     error
	startRequest *infomodels.RequestRestore
	startCalls   int

	prepareErr     error
	prepareRequest *infomodels.RequestPrepareRestore
	prepareCalls   int
}

func (f *fakeRestoreInfoClient) PrepareRestore(_ context.Context, request *infomodels.RequestPrepareRestore) error {
	f.prepareCalls++
	f.prepareRequest = request

	return f.prepareErr
}

func (f *fakeRestoreInfoClient) GetClusterStable(_ context.Context, _ string) (bool, error) {
	return !f.unstable, nil
}

func (f *fakeRestoreInfoClient) StartRestore(_ context.Context, request *infomodels.RequestRestore) error {
	f.startCalls++
	f.startRequest = request

	return f.startErr
}

func (f *fakeRestoreInfoClient) GetRestoreStatus(
	_ context.Context, _, _ string,
) (*infomodels.ResponseRestoreState, error) {
	call := f.calls
	f.calls++

	if call >= len(f.answers) {
		if !f.repeatLast || len(f.answers) == 0 {
			return nil, asinfo.ErrNotFound
		}

		call = len(f.answers) - 1
	}

	return f.answers[call].status, f.answers[call].err
}

func (f *fakeRestoreInfoClient) AbortRestore(_ context.Context, namespace, jobID string) error {
	f.abortCalls++
	f.abortNamespace = namespace
	f.abortJobID = jobID

	return f.abortErr
}

func (f *fakeRestoreInfoClient) OpenRestore(_ context.Context, namespace, jobID string) error {
	f.openCalls++
	f.openNamespace = namespace
	f.openJobID = jobID

	return f.openErr
}

func (f *fakeRestoreInfoClient) ClearRestoreStorage(_ context.Context, namespace, jobID string) error {
	call := f.clearCalls
	f.clearCalls++
	f.clearNamespace = namespace
	f.clearJobID = jobID

	if call < len(f.clearErrs) {
		return f.clearErrs[call]
	}

	return nil
}

// fakeClusterInfo answers the two questions asked about the cluster before a job is
// requested from it.
type fakeClusterInfo struct {
	version    infomodels.AerospikeVersion
	versionErr error

	stable    bool
	stableErr error
}

func (f *fakeClusterInfo) GetVersion(_ context.Context) (infomodels.AerospikeVersion, error) {
	return f.version, f.versionErr
}

func (f *fakeClusterInfo) GetClusterStable(_ context.Context, _ string) (bool, error) {
	return f.stable, f.stableErr
}

// TestNewService covers the one thing the constructor decides: a service serves the
// backup commands or the restore commands, and it reads the cluster and storage settings
// from the configuration of those commands. A service built with neither has nothing to
// read them from, and one built with both would silently ignore one of them.
func TestNewService(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		backupCfg  *config.ServerBackupServiceConfig
		restoreCfg *config.ServerRestoreServiceConfig
		wantErr    error
	}{
		{
			name:      "serves the backup commands",
			backupCfg: &config.ServerBackupServiceConfig{},
		},
		{
			name:       "serves the restore commands",
			restoreCfg: &config.ServerRestoreServiceConfig{},
		},
		{
			name:    "rejects a service with no configuration",
			wantErr: errServiceConfig,
		},
		{
			name:       "rejects a service with both configurations",
			backupCfg:  &config.ServerBackupServiceConfig{},
			restoreCfg: &config.ServerRestoreServiceConfig{},
			wantErr:    errServiceConfig,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			svc, err := NewService(tt.backupCfg, tt.restoreCfg, slog.New(slog.DiscardHandler))

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				assert.Nil(t, svc)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, svc)
		})
	}
}

// TestServiceCheckServerVersion covers the gate every command that talks to the cluster
// passes through. The versions are derived from the requirement rather than written out,
// so that the test keeps testing the gate and not a version number.
func TestServiceCheckServerVersion(t *testing.T) {
	t.Parallel()

	required := infomodels.AerospikeVersionSupportsIntegratedBackup

	tests := []struct {
		name       string
		client     *fakeClusterInfo
		wantErr    error
		wantErrMsg string
	}{
		{
			name:   "accepts the oldest supported version",
			client: &fakeClusterInfo{version: required},
		},
		{
			name: "accepts a newer version",
			client: &fakeClusterInfo{
				version: infomodels.AerospikeVersion{Major: required.Major + 1},
			},
		},
		{
			// The version is named in the failure: an operator has to know which node
			// holds the cluster back.
			name: "rejects an older version",
			client: &fakeClusterInfo{
				version: infomodels.AerospikeVersion{
					Major: required.Major - 1,
					Minor: required.Minor,
					Patch: required.Patch,
				},
			},
			wantErr:    errVersionUnsupported,
			wantErrMsg: "server version",
		},
		{
			name:       "propagates a failed lookup",
			client:     &fakeClusterInfo{versionErr: errBoom},
			wantErr:    errBoom,
			wantErrMsg: "failed to get server version",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			svc, _ := newTestService(t)

			err := svc.checkServerVersion(t.Context(), tt.client)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				require.ErrorContains(t, err, tt.wantErrMsg)

				return
			}

			require.NoError(t, err)
		})
	}
}

// TestServiceCheckClusterStable covers the check that keeps a job that reads or writes a
// whole namespace from starting while the namespace is still migrating.
func TestServiceCheckClusterStable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		client     *fakeClusterInfo
		wantErr    error
		wantErrMsg string
	}{
		{
			name:   "accepts a stable namespace",
			client: &fakeClusterInfo{stable: true},
		},
		{
			// The namespace belongs in the failure: it is the one the job would have
			// run on.
			name:       "rejects a namespace with migrations in flight",
			client:     &fakeClusterInfo{},
			wantErr:    errClusterNotStable,
			wantErrMsg: testNamespace,
		},
		{
			// A check that could not be made says nothing about the cluster, and must
			// not be read as a stable one.
			name:       "propagates a failed check",
			client:     &fakeClusterInfo{stableErr: errBoom},
			wantErr:    errBoom,
			wantErrMsg: "failed to check cluster stability",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			svc, _ := newTestService(t)

			err := svc.checkClusterStable(t.Context(), tt.client, testNamespace)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				require.ErrorContains(t, err, tt.wantErrMsg)

				return
			}

			require.NoError(t, err)
		})
	}
}

// newTestService builds a service that logs into the returned buffer and polls and
// retries fast enough not to slow the tests down.
func newTestService(t *testing.T) (*Service, *bytes.Buffer) {
	t.Helper()

	buf := &bytes.Buffer{}

	return &Service{
		backupCfg: &config.ServerBackupServiceConfig{
			Abort: &models.ServerBackupAbort{JobID: testJobID},
			App:   &models.App{LogJSON: true},
			AwsS3: &models.AwsS3{BucketName: testBucket},
		},
		restoreCfg: &config.ServerRestoreServiceConfig{
			Prepare:      &models.ServerRestorePrepare{Namespace: testNamespace, JobID: testJobID},
			Abort:        &models.ServerRestoreAbort{Namespace: testNamespace, JobID: testJobID},
			Open:         &models.ServerRestoreOpen{Namespace: testNamespace, JobID: testJobID},
			ClearStorage: &models.ServerRestoreClearStorage{Namespace: testNamespace, JobID: testJobID},
		},
		logger:               slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelInfo})),
		metadataRetryPolicy:  backupmodels.NewRetryPolicy(time.Millisecond, 1.0, 3),
		vanishedPollInterval: time.Millisecond,
		statePollInterval:    time.Millisecond,
	}, buf
}

// repeatNotFound builds n answers that report the job as missing.
func repeatNotFound(n int) []statusAnswer {
	answers := make([]statusAnswer, n)
	for i := range answers {
		answers[i] = statusAnswer{err: asinfo.ErrNotFound}
	}

	return answers
}

func testStatus(state infomodels.BackupState, pct float64) *infomodels.ResponseBackupState {
	return &infomodels.ResponseBackupState{
		JobID:       testJobID,
		Namespace:   testNamespace,
		State:       state,
		ProgressPct: pct,
		StartTime:   time.Now().Add(-testBackupDuration),
	}
}

// testFinishedStatus reports a job that reached a terminal state, which is the only case
// in which the cluster fills in the finish time.
func testFinishedStatus(state infomodels.BackupState, pct float64) *infomodels.ResponseBackupState {
	status := testStatus(state, pct)
	status.FinishTime = status.StartTime.Add(testBackupDuration)

	return status
}

// testFailedStatus reports a job that ended in a terminal state with a reason attached.
func testFailedStatus(state infomodels.BackupState) *infomodels.ResponseBackupState {
	status := testFinishedStatus(state, 42)
	status.ErrorReason = testErrorReason

	return status
}

// testLayout stands for the active nodes of the cluster.
func testLayout() int { return testLayoutNodes }

// testColdStatus reports a cold restore job of a single backup.
func testColdStatus(state infomodels.RestoreState, nsState infomodels.NamespaceState) *infomodels.ResponseRestoreState {
	return &infomodels.ResponseRestoreState{
		JobID:         testJobID,
		Namespace:     testNamespace,
		Kind:          infomodels.RestoreKindCold,
		State:         state,
		NsState:       nsState,
		BackupEntries: 1,
		StartTime:     time.Now().Add(-testBackupDuration),
	}
}

// testFuzzyStatus reports a fuzzy restore job of a single backup.
func testFuzzyStatus(state infomodels.RestoreState) *infomodels.ResponseRestoreState {
	return &infomodels.ResponseRestoreState{
		JobID:         testJobID,
		Namespace:     testNamespace,
		Kind:          infomodels.RestoreKindFuzzy,
		State:         state,
		BackupEntries: 1,
		StartTime:     time.Now().Add(-testBackupDuration),
	}
}

// testFinishedRestore reports a job that reached a terminal state, which is the only case
// in which the cluster fills in the finish time.
func testFinishedRestore(status *infomodels.ResponseRestoreState) *infomodels.ResponseRestoreState {
	status.FinishTime = status.StartTime.Add(testBackupDuration)

	return status
}

// testFailedRestore reports a job that ended in a terminal state with a reason attached.
func testFailedRestore(status *infomodels.ResponseRestoreState) *infomodels.ResponseRestoreState {
	status.ErrorReason = testErrorReason

	return testFinishedRestore(status)
}

// repeatRestoreNotFound builds n answers that report the restore job as missing.
func repeatRestoreNotFound(n int) []restoreAnswer {
	answers := make([]restoreAnswer, n)
	for i := range answers {
		answers[i] = restoreAnswer{err: asinfo.ErrNotFound}
	}

	return answers
}

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
