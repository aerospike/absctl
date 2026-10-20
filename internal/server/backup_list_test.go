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
	"testing"

	"github.com/aerospike/backup-go/pkg/server/lister"
	servermodels "github.com/aerospike/backup-go/pkg/server/lister/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const logNoBackups = "backups not found"

// fakeBackupLister answers the two lookups listing makes: the manifests under the
// configured path, and the single manifest that path may name itself.
type fakeBackupLister struct {
	all    []servermodels.Metadata
	allErr error

	md      servermodels.Metadata
	mdErr   error
	mdCalls int
}

func (f *fakeBackupLister) FetchAllMetadata(_ context.Context) ([]servermodels.Metadata, error) {
	if f.allErr != nil {
		return nil, f.allErr
	}

	return f.all, nil
}

func (f *fakeBackupLister) GetMetadata(_ context.Context, _ string) (servermodels.Metadata, error) {
	f.mdCalls++

	if f.mdErr != nil {
		return servermodels.Metadata{}, f.mdErr
	}

	return f.md, nil
}

// TestServiceListBackups covers what a listing does when the path holds no manifests:
// the path may name a single backup rather than a prefix over several, so it is read as
// one, and only a path that holds nothing at all is reported as empty.
func TestServiceListBackups(t *testing.T) {
	t.Parallel()

	md := servermodels.Metadata{BackupID: testJobID, Namespace: testNamespace}

	tests := []struct {
		name        string
		l           *fakeBackupLister
		wantErr     error
		wantMDCalls int
		wantLogs    []string
		notWantLogs []string
	}{
		{
			name:     "prints the backups under the path",
			l:        &fakeBackupLister{all: []servermodels.Metadata{md}},
			wantLogs: []string{logBackupEntry},
		},
		{
			// A path that names one backup holds nothing under itself, so the listing
			// comes back empty and the path itself carries the manifest.
			name:        "reads a path that names a single backup",
			l:           &fakeBackupLister{md: md},
			wantMDCalls: 1,
			wantLogs:    []string{logBackupEntry},
		},
		{
			// A path with neither manifests under it nor one of its own is empty, which
			// is an answer and not a failure.
			name:        "reports a path that holds no backups",
			l:           &fakeBackupLister{mdErr: lister.ErrMetadataNotFound},
			wantMDCalls: 1,
			wantLogs:    []string{logNoBackups},
			notWantLogs: []string{logBackupEntry},
		},
		{
			name:    "propagates a failed listing",
			l:       &fakeBackupLister{allErr: errBoom},
			wantErr: errBoom,
		},
		{
			// Storage that cannot be read says nothing about whether the path names a
			// backup, and must not be reported as an empty path.
			name:        "propagates a failed read of a single backup",
			l:           &fakeBackupLister{mdErr: errBoom},
			wantErr:     errBoom,
			wantMDCalls: 1,
			notWantLogs: []string{logNoBackups},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			svc, buf := newTestService(t)

			err := svc.listBackups(t.Context(), tt.l, testBackupPath)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}

			assert.Equal(t, tt.wantMDCalls, tt.l.mdCalls)

			for _, want := range tt.wantLogs {
				assert.Contains(t, buf.String(), want)
			}

			for _, notWant := range tt.notWantLogs {
				assert.NotContains(t, buf.String(), notWant)
			}
		})
	}
}
