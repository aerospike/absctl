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

package flags

import (
	"testing"

	"github.com/aerospike/absctl/internal/models"
	"github.com/stretchr/testify/assert"
)

func TestLocal_NewFlagSet(t *testing.T) {
	t.Parallel()
	local := NewLocal(OperationBackup)

	flagSet := local.NewFlagSet()

	args := []string{
		"--local-buffer-size", "8",
	}

	err := flagSet.Parse(args)
	assert.NoError(t, err)

	result := local.GetLocal()

	assert.Equal(t, 8, result.BufferSize, "The local-buffer-size flag should be parsed correctly")
}

func TestLocal_NewFlagSet_DefaultValues(t *testing.T) {
	t.Parallel()
	local := NewLocal(OperationBackup)

	flagSet := local.NewFlagSet()

	err := flagSet.Parse([]string{})
	assert.NoError(t, err)

	result := local.GetLocal()

	assert.Equal(t, models.DefaultLocalBufferSize, result.BufferSize, "The default value for local-buffer-size should be DefaultChunkSize")
}
