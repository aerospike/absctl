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
	"encoding/json"
	"log/slog"
	"testing"

	"github.com/aerospike/absctl/internal/models"
	"github.com/aerospike/backup-go/pkg/server/lister"
	servermodels "github.com/aerospike/backup-go/pkg/server/lister/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMetadataS3Config(t *testing.T) {
	t.Parallel()

	shared := &models.AwsS3{BucketName: testBucket}

	cfg := metadataS3Config(shared)

	assert.Equal(t, testBucket, cfg.BucketName)
	assert.Equal(t, metadataRequestTimeoutMillis, cfg.RequestTimeout)
	// The configuration the rest of the command reads must stay untouched.
	assert.Equal(t, 0, shared.RequestTimeout)
}

func TestMetadataGetter(t *testing.T) {
	t.Parallel()

	manifest, err := json.Marshal(servermodels.Metadata{BackupID: testRequestBackupID})
	require.NoError(t, err)

	tests := []struct {
		name       string
		givePrefix string
		wantKey    string
	}{
		{name: "no prefix", givePrefix: "", wantKey: testRequestBackupID + testManifestSuffix},
		{name: "prefix", givePrefix: testBackupPath, wantKey: testMetadataKey + testManifestSuffix},
		{
			name:       "prefix with trailing slash",
			givePrefix: testBackupPath + "/",
			wantKey:    testMetadataKey + testManifestSuffix,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := &fakeManifestS3{manifest: manifest}
			g := newMetadataGetter(client, testBucket, tt.givePrefix, slog.New(slog.DiscardHandler))

			_, err := g.GetMetadata(t.Context(), testRequestBackupID)
			require.NoError(t, err)

			assert.Equal(t, []string{tt.wantKey}, client.keys)
		})
	}
}

func TestMetadataGetterRejectsPath(t *testing.T) {
	t.Parallel()

	client := &fakeManifestS3{}
	g := newMetadataGetter(client, testBucket, "", slog.New(slog.DiscardHandler))

	// A storage path is not a backup id, so nothing is read for it.
	_, err := g.GetMetadata(t.Context(), testMetadataKey)
	require.ErrorIs(t, err, lister.ErrInvalidBackupID)
	assert.Empty(t, client.keys)
}
