// Copyright 2024 Aerospike, Inc.
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

	"github.com/stretchr/testify/assert"
)

func TestMapCompressionPolicy_Success(t *testing.T) {
	t.Parallel()

	compressionModel := &Compression{
		Mode:  "ZSTD",
		Level: 3,
	}

	compressionPolicy := compressionModel.Policy()
	assert.NotNil(t, compressionPolicy)
	assert.Equal(t, "ZSTD", compressionPolicy.Mode)
	assert.Equal(t, 3, compressionPolicy.Level)
}

func TestMapCompressionPolicy_EmptyMode(t *testing.T) {
	t.Parallel()

	compressionModel := &Compression{}
	compressionPolicy := compressionModel.Policy()
	assert.Nil(t, compressionPolicy)
}

func TestMapCompressionPolicy_CaseInsensitiveMode(t *testing.T) {
	t.Parallel()

	compressionModel := &Compression{
		Mode:  "zstd", // Lowercase mode
		Level: 3,
	}

	compressionPolicy := compressionModel.Policy()
	assert.NotNil(t, compressionPolicy)
	assert.Equal(t, "ZSTD", compressionPolicy.Mode, "Compression mode should be converted to uppercase")
	assert.Equal(t, 3, compressionPolicy.Level)
}

func TestCompression_Validate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		comp    Compression
		wantErr string
	}{
		{
			name: "empty compression is valid",
			comp: Compression{},
		},
		{
			name: "ZSTD mode is valid",
			comp: Compression{Mode: "ZSTD"},
		},
		{
			name: "ZSTD mode with level",
			comp: Compression{Mode: "ZSTD", Level: 3},
		},
		{
			name: "lowercase zstd is valid",
			comp: Compression{Mode: "zstd"},
		},
		{
			name: "mixed case Zstd is valid",
			comp: Compression{Mode: "Zstd"},
		},
		{
			name: "NONE mode is valid",
			comp: Compression{Mode: "NONE"},
		},
		{
			name: "lowercase none is valid",
			comp: Compression{Mode: "none"},
		},
		{
			name: "mode with zero level is valid",
			comp: Compression{Mode: "ZSTD", Level: 0},
		},
		{
			name:    "invalid mode",
			comp:    Compression{Mode: "LZ4"},
			wantErr: "invalid compression mode: LZ4",
		},
		{
			name:    "another invalid mode",
			comp:    Compression{Mode: "GZIP"},
			wantErr: "invalid compression mode: GZIP",
		},
		{
			name:    "level without mode",
			comp:    Compression{Level: 5},
			wantErr: "--compress is required when --compression-level is set",
		},
		{
			name: "negative level without mode is valid",
			comp: Compression{Level: -1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.comp.Validate()
			if tt.wantErr != "" {
				assert.ErrorContains(t, err, tt.wantErr)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
