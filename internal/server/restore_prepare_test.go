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
	"testing"

	"github.com/aerospike/absctl/internal/config"
	"github.com/aerospike/absctl/internal/models"
	infomodels "github.com/aerospike/backup-go/pkg/asinfo/models"
	"github.com/stretchr/testify/assert"
)

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
