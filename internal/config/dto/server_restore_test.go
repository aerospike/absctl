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

package dto

import (
	"testing"

	"github.com/aerospike/absctl/internal/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultServerRestore(t *testing.T) {
	restore := DefaultServerRestore()

	require.NotNil(t, restore)
	require.NotNil(t, restore.App)
	require.NotNil(t, restore.Cluster)
	require.NotNil(t, restore.Restore)
	require.NotNil(t, restore.Prepare)
	require.NotNil(t, restore.Progress)
	require.NotNil(t, restore.SecretAgent)
	require.NotNil(t, restore.Aws.S3)
}

func TestDefaultServerRestoreSections(t *testing.T) {
	restore := defaultServerRestoreConfig()

	assert.Equal(t, models.DefaultCommonNamespace, derefString(restore.Namespace))
	assert.Equal(t, models.DefaultServerBackupObjectStorageType, derefString(restore.StorageType))
	assert.Equal(t, models.DefaultServerBackupJobID, derefString(restore.JobID))
	assert.Equal(t, models.DefaultServerBackupPath, derefString(restore.Path))
	assert.Equal(t, models.DefaultServerRestoreFuzzyRestore, derefBool(restore.FuzzyRestore))

	prepare := defaultServerRestorePrepareConfig()
	assert.Equal(t, models.DefaultCommonNamespace, derefString(prepare.Namespace))
	assert.Equal(t, models.DefaultServerBackupJobID, derefString(prepare.JobID))

	progress := defaultServerRestoreProgressConfig()
	assert.Equal(t, models.DefaultCommonNamespace, derefString(progress.Namespace))
}

func TestServerRestoreToModels(t *testing.T) {
	t.Parallel()

	namespace := "ns1"
	storageType := "aws-s3"
	jobID := "bkp-1"
	path := "some/prefix"
	fuzzy := true
	prepareNamespace := "ns2"
	progressNamespace := "ns3"

	restore := &ServerRestore{
		Restore: ServerRestoreConfig{
			Namespace:    &namespace,
			StorageType:  &storageType,
			JobID:        &jobID,
			Path:         &path,
			FuzzyRestore: &fuzzy,
		},
		Prepare:  ServerRestorePrepareConfig{Namespace: &prepareNamespace, JobID: &jobID},
		Progress: ServerRestoreProgressConfig{Namespace: &progressNamespace},
	}

	start := restore.ToModelServerRestore()
	require.NotNil(t, start)
	assert.Equal(t, namespace, start.Namespace)
	assert.Equal(t, storageType, start.StorageType)
	assert.Equal(t, jobID, start.JobID)
	assert.Equal(t, path, start.Path)
	assert.True(t, start.FuzzyRestore)

	prepare := restore.ToModelServerRestorePrepare()
	require.NotNil(t, prepare)
	assert.Equal(t, prepareNamespace, prepare.Namespace)
	assert.Equal(t, jobID, prepare.JobID)

	progress := restore.ToModelServerRestoreProgress()
	require.NotNil(t, progress)
	assert.Equal(t, progressNamespace, progress.Namespace)
}

func TestServerRestoreToModelsNil(t *testing.T) {
	t.Parallel()

	var restore *ServerRestore

	assert.Nil(t, restore.ToModelServerRestore())
	assert.Nil(t, restore.ToModelServerRestorePrepare())
	assert.Nil(t, restore.ToModelServerRestoreProgress())
}

func TestServerRestoreLoadSecretsNoAgent(t *testing.T) {
	t.Parallel()

	restore := DefaultServerRestore()
	reference := "secrets:resource:key"
	restore.Cluster.Password = &reference

	require.NoError(t, restore.LoadSecrets(t.Context()))
	assert.Equal(t, "secrets:resource:key", derefString(restore.Cluster.Password))
}
