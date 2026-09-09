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

func TestStorageCommon_Validate(t *testing.T) {
	tests := []struct {
		name                    string
		maxConnsPerHost         int
		requestTimeoutSeconds   int
		calculateChecksum       bool
		RetryReadBackoffSeconds int
		retryReadMultiplier     float64
		retryReadMaxAttempts    uint
		isBackup                bool
		wantErr                 bool
		errMsg                  string
	}{
		{
			name:                    "backup with valid values",
			maxConnsPerHost:         100,
			requestTimeoutSeconds:   300,
			calculateChecksum:       true,
			RetryReadBackoffSeconds: 0,
			retryReadMultiplier:     0,
			retryReadMaxAttempts:    0,
			isBackup:                true,
			wantErr:                 false,
		},
		{
			name:                    "restore with valid values",
			maxConnsPerHost:         100,
			requestTimeoutSeconds:   300,
			calculateChecksum:       false,
			RetryReadBackoffSeconds: 1000,
			retryReadMultiplier:     2.0,
			retryReadMaxAttempts:    5,
			isBackup:                false,
			wantErr:                 false,
		},
		{
			name:                    "restore with minimum valid retry values",
			maxConnsPerHost:         0,
			requestTimeoutSeconds:   0,
			calculateChecksum:       false,
			RetryReadBackoffSeconds: 1,
			retryReadMultiplier:     1.0,
			retryReadMaxAttempts:    1,
			isBackup:                false,
			wantErr:                 false,
		},
		{
			name:                    "negative max connections per host",
			maxConnsPerHost:         -1,
			requestTimeoutSeconds:   300,
			calculateChecksum:       true,
			RetryReadBackoffSeconds: 1000,
			retryReadMultiplier:     2.0,
			retryReadMaxAttempts:    5,
			isBackup:                true,
			wantErr:                 true,
			errMsg:                  "max connections per host must be non-negative",
		},
		{
			name:                    "negative request timeout",
			maxConnsPerHost:         100,
			requestTimeoutSeconds:   -1,
			calculateChecksum:       true,
			RetryReadBackoffSeconds: 1000,
			retryReadMultiplier:     2.0,
			retryReadMaxAttempts:    5,
			isBackup:                true,
			wantErr:                 true,
			errMsg:                  "request timeout must be non-negative",
		},
		{
			name:                    "restore with zero retry read timeout",
			maxConnsPerHost:         100,
			requestTimeoutSeconds:   300,
			calculateChecksum:       false,
			RetryReadBackoffSeconds: 0,
			retryReadMultiplier:     2.0,
			retryReadMaxAttempts:    5,
			isBackup:                false,
			wantErr:                 true,
			errMsg:                  "retry read timeout must be positive",
		},
		{
			name:                    "restore with negative retry read timeout",
			maxConnsPerHost:         100,
			requestTimeoutSeconds:   300,
			calculateChecksum:       false,
			RetryReadBackoffSeconds: -100,
			retryReadMultiplier:     2.0,
			retryReadMaxAttempts:    5,
			isBackup:                false,
			wantErr:                 true,
			errMsg:                  "retry read timeout must be positive",
		},
		{
			name:                    "restore with zero retry read multiplier",
			maxConnsPerHost:         100,
			requestTimeoutSeconds:   300,
			calculateChecksum:       false,
			RetryReadBackoffSeconds: 1000,
			retryReadMultiplier:     0,
			retryReadMaxAttempts:    5,
			isBackup:                false,
			wantErr:                 true,
			errMsg:                  "retry read multiplier must be positive",
		},
		{
			name:                    "restore with negative retry read multiplier",
			maxConnsPerHost:         100,
			requestTimeoutSeconds:   300,
			calculateChecksum:       false,
			RetryReadBackoffSeconds: 1000,
			retryReadMultiplier:     -1.5,
			retryReadMaxAttempts:    5,
			isBackup:                false,
			wantErr:                 true,
			errMsg:                  "retry read multiplier must be positive",
		},
		{
			name:                    "backup ignores invalid retry values",
			maxConnsPerHost:         100,
			requestTimeoutSeconds:   300,
			calculateChecksum:       true,
			RetryReadBackoffSeconds: 0,
			retryReadMultiplier:     0,
			retryReadMaxAttempts:    0,
			isBackup:                true,
			wantErr:                 false,
		},
		{
			name:                    "zero max connections per host",
			maxConnsPerHost:         0,
			requestTimeoutSeconds:   300,
			calculateChecksum:       true,
			RetryReadBackoffSeconds: 1000,
			retryReadMultiplier:     2.0,
			retryReadMaxAttempts:    5,
			isBackup:                true,
			wantErr:                 false,
		},
		{
			name:                    "zero request timeout",
			maxConnsPerHost:         100,
			requestTimeoutSeconds:   0,
			calculateChecksum:       true,
			RetryReadBackoffSeconds: 1000,
			retryReadMultiplier:     2.0,
			retryReadMaxAttempts:    5,
			isBackup:                true,
			wantErr:                 false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &StorageCommon{
				MaxConnsPerHost:      tt.maxConnsPerHost,
				RequestTimeout:       tt.requestTimeoutSeconds,
				CalculateChecksum:    tt.calculateChecksum,
				RetryReadBackoff:     tt.RetryReadBackoffSeconds,
				RetryReadMultiplier:  tt.retryReadMultiplier,
				RetryReadMaxAttempts: tt.retryReadMaxAttempts,
			}

			err := s.Validate(tt.isBackup)

			if tt.wantErr {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errMsg)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
