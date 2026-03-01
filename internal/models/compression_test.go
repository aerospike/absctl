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
