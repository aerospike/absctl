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

package models

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLocal_Validate(t *testing.T) {
	tests := []struct {
		name       string
		bufferSize int
		isBackup   bool
		wantErr    bool
		errMsg     string
	}{
		{
			name:       "backup with positive buffer size",
			bufferSize: 1024,
			isBackup:   true,
			wantErr:    false,
		},
		{
			name:       "backup with zero buffer size",
			bufferSize: 1,
			isBackup:   true,
			wantErr:    false,
		},
		{
			name:       "backup with negative buffer size",
			bufferSize: -1,
			isBackup:   true,
			wantErr:    true,
			errMsg:     "buffer size can't be less than 1",
		},
		{
			name:       "restore with positive buffer size",
			bufferSize: 1024,
			isBackup:   false,
			wantErr:    false,
		},
		{
			name:       "restore with negative buffer size",
			bufferSize: -1,
			isBackup:   false,
			wantErr:    false, // validation not applied for restore
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := &Local{
				BufferSize: tt.bufferSize,
			}

			err := l.Validate(tt.isBackup)

			if tt.wantErr {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errMsg)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
