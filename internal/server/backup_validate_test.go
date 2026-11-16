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
	"errors"
	"testing"

	sModels "github.com/aerospike/backup-go/pkg/server/segvalidator/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testValidatedBackupID = "519118324"

var errTestRestoreRefused = errors.New("backup was aborted")

func TestValidationOutcome(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		give    *sModels.ValidationReport
		name    string
	}{
		{
			name: "clean backup",
			give: &sModels.ValidationReport{CheckedSegments: 10, ValidSegments: 10},
		},
		{
			// Segments nothing recorded are reported, not counted as failures.
			name: "unrecorded segments alone",
			give: &sModels.ValidationReport{
				CheckedSegments: 10,
				ValidSegments:   10,
				Manifests:       sModels.ManifestReport{Unrecorded: 3},
			},
		},
		{
			name:    "unreadable segment",
			give:    &sModels.ValidationReport{CheckedSegments: 10, ValidSegments: 9, InvalidSegments: 1},
			wantErr: errBackupInvalid,
		},
		{
			name: "manifest problem",
			give: &sModels.ValidationReport{
				CheckedSegments: 10,
				ValidSegments:   10,
				Manifests:       sModels.ManifestReport{Problems: 1},
			},
			wantErr: errBackupInvalid,
		},
		{
			// Every segment reads back and a restore still refuses to start.
			name: "restore would refuse the backup",
			give: &sModels.ValidationReport{
				CheckedSegments: 10,
				ValidSegments:   10,
				Commit:          sModels.CommitReport{Refusal: errTestRestoreRefused},
			},
			wantErr: errBackupInvalid,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.give.BackupID = testValidatedBackupID

			err := validationOutcome(tt.give)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				assert.Contains(t, err.Error(), testValidatedBackupID)

				return
			}

			require.NoError(t, err)
		})
	}
}
