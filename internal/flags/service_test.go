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
	"github.com/stretchr/testify/require"
)

func TestServiceConnection_NewFlagSet(t *testing.T) {
	t.Parallel()

	conn := NewServiceConnection()
	flagSet := conn.NewFlagSet()

	args := []string{
		"--service-host", "abs.example.com",
		"--service-port", "9090",
	}

	err := flagSet.Parse(args)
	require.NoError(t, err)

	result := conn.GetServiceConnection()

	assert.Equal(t, "abs.example.com", result.Host)
	assert.Equal(t, 9090, result.Port)
}

func TestServiceConnection_NewFlagSet_DefaultValues(t *testing.T) {
	t.Parallel()

	conn := NewServiceConnection()
	flagSet := conn.NewFlagSet()

	err := flagSet.Parse([]string{})
	require.NoError(t, err)

	result := conn.GetServiceConnection()

	assert.Equal(t, models.DefaultServiceHost, result.Host)
	assert.Equal(t, models.DefaultServicePort, result.Port)
}
