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

	"github.com/aerospike/absctl/internal/models"
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
