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

package storage

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/aerospike/absctl/internal/config"
	"github.com/aerospike/absctl/internal/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testBucket    = "test"
	testProjectID = "test-id"
	testFileName  = "/file.bak"

	testLocalType  = "directory"
	testStdoutType = "stdout"

	testS3Endpoint = "http://localhost:9000"
	testS3Region   = "eu"
	testS3Profile  = "minio"
	testS3Type     = "s3"

	testGcpEndpoint = "http://127.0.0.1:4443/storage/v1/b"
	testGcpType     = "gcp-storage"

	testAzureEndpoint    = "http://127.0.0.1:10000/devstoreaccount1"
	testAzureAccountName = "devstoreaccount1"
	testAzureAccountKey  = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
	testAzureType        = "azure-blob"
)

func TestNewLocalWriter(t *testing.T) {
	t.Parallel()

	params := &config.BackupServiceConfig{
		Backup: &models.Backup{
			RemoveFiles: true,
			Common: models.Common{
				Directory: t.TempDir(),
			},
		},
		ServiceConfigCommon: config.ServiceConfigCommon{
			AwsS3:      &models.AwsS3{},
			GcpStorage: &models.GcpStorage{},
			AzureBlob:  &models.AzureBlob{},
			Local:      &models.Local{},
		},
	}
	ctx := t.Context()
	writer, err := newWriter(ctx, params, slog.Default())
	require.NoError(t, err)
	assert.NotNil(t, writer)
	assert.Equal(t, testLocalType, writer.GetType())

	params = &config.BackupServiceConfig{
		Backup: &models.Backup{
			OutputFile: t.TempDir() + testFileName,
		},
		ServiceConfigCommon: config.ServiceConfigCommon{
			AwsS3:      &models.AwsS3{},
			GcpStorage: &models.GcpStorage{},
			AzureBlob:  &models.AzureBlob{},
			Local:      &models.Local{},
		},
	}
	writer, err = newWriter(ctx, params, slog.Default())
	require.NoError(t, err)
	assert.NotNil(t, writer)
	assert.Equal(t, testLocalType, writer.GetType())

	params = &config.BackupServiceConfig{
		Backup: &models.Backup{},
		ServiceConfigCommon: config.ServiceConfigCommon{
			AwsS3:      &models.AwsS3{},
			GcpStorage: &models.GcpStorage{},
			AzureBlob:  &models.AzureBlob{},
			Local:      &models.Local{},
		},
	}
	writer, err = newWriter(ctx, params, slog.Default())
	require.Error(t, err)
	assert.Nil(t, writer)
}

func TestNewS3Writer(t *testing.T) {
	t.Parallel()

	err := createAwsCredentials()
	require.NoError(t, err)

	params := &config.BackupServiceConfig{
		Backup: &models.Backup{
			RemoveFiles: true,
			Common: models.Common{
				Directory: t.TempDir(),
			},
		},
		ServiceConfigCommon: config.ServiceConfigCommon{
			AwsS3: &models.AwsS3{
				BucketName:   testS3Bucket,
				Region:       testS3Region,
				Profile:      testS3Profile,
				Endpoint:     testS3Endpoint,
				StorageClass: "STANDARD",
			},
			GcpStorage: &models.GcpStorage{},
			AzureBlob:  &models.AzureBlob{},
			Local:      &models.Local{},
		},
	}

	ctx := t.Context()

	writer, err := newWriter(ctx, params, slog.Default())
	require.NoError(t, err)
	assert.NotNil(t, writer)
	assert.Equal(t, testS3Type, writer.GetType())

	params = &config.BackupServiceConfig{
		Backup: &models.Backup{
			OutputFile: t.TempDir() + testFileName,
		},
		ServiceConfigCommon: config.ServiceConfigCommon{
			AwsS3: &models.AwsS3{
				BucketName: testS3Bucket,
				Region:     testS3Region,
				Profile:    testS3Profile,
				Endpoint:   testS3Endpoint,
			},
			GcpStorage: &models.GcpStorage{},
			AzureBlob:  &models.AzureBlob{},
			Local:      &models.Local{},
		},
	}

	writer, err = newWriter(ctx, params, slog.Default())
	require.NoError(t, err)
	assert.NotNil(t, writer)
	assert.Equal(t, testS3Type, writer.GetType())
}

func createAwsCredentials() error {
	home, err := os.UserHomeDir()
	if err != nil {
		return fmt.Errorf("error getting home directory: %w", err)
	}

	awsDir := filepath.Join(home, ".aws")

	err = os.MkdirAll(awsDir, 0o700)
	if err != nil {
		return fmt.Errorf("error creating .aws directory: %w", err)
	}

	filePath := filepath.Join(awsDir, "credentials")

	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		credentialsFileBytes := []byte(`[minio]
aws_access_key_id = minioadmin
aws_secret_access_key = minioadminpassword`)

		err = os.WriteFile(filePath, credentialsFileBytes, 0o600)
		if err != nil {
			return fmt.Errorf("error writing ~/.aws/credentials file: %w", err)
		}

		fmt.Println("Credentials file created successfully!")
	}

	return nil
}

func TestGcpWriter(t *testing.T) {
	t.Parallel()

	err := createGcpBucket()
	require.NoError(t, err)

	params := &config.BackupServiceConfig{
		Backup: &models.Backup{
			RemoveFiles: true,
			Common: models.Common{
				Directory: t.TempDir(),
			},
		},
		ServiceConfigCommon: config.ServiceConfigCommon{
			GcpStorage: &models.GcpStorage{
				BucketName: testBucket,
				Endpoint:   testGcpEndpoint,
			},
			AzureBlob: &models.AzureBlob{},
			AwsS3:     &models.AwsS3{},
			Local:     &models.Local{},
		},
	}

	ctx := t.Context()

	writer, err := newWriter(ctx, params, slog.Default())
	require.NoError(t, err)
	assert.NotNil(t, writer)
	assert.Equal(t, testGcpType, writer.GetType())

	params = &config.BackupServiceConfig{
		Backup: &models.Backup{
			OutputFile: t.TempDir() + testFileName,
		},
		ServiceConfigCommon: config.ServiceConfigCommon{
			GcpStorage: &models.GcpStorage{
				BucketName: testBucket,
				Endpoint:   testGcpEndpoint,
			},
			AzureBlob: &models.AzureBlob{},
			AwsS3:     &models.AwsS3{},
			Local:     &models.Local{},
		},
	}

	writer, err = newWriter(ctx, params, slog.Default())
	require.NoError(t, err)
	assert.NotNil(t, writer)
	assert.Equal(t, testGcpType, writer.GetType())
}

func createGcpBucket() error {
	ctx := context.Background()
	cfg := &models.GcpStorage{
		BucketName: testBucket,
		Endpoint:   testGcpEndpoint,
	}

	c, err := newGcpClient(ctx, cfg)
	if err != nil {
		return err
	}

	bucket := c.Bucket(testBucket)
	_ = bucket.Create(ctx, testProjectID, nil)

	return nil
}

func TestAzureWriter(t *testing.T) {
	t.Parallel()

	err := createAzureContainer()
	require.NoError(t, err)

	params := &config.BackupServiceConfig{
		Backup: &models.Backup{
			RemoveFiles: true,
			Common: models.Common{
				Directory: t.TempDir(),
			},
		},
		ServiceConfigCommon: config.ServiceConfigCommon{
			AzureBlob: &models.AzureBlob{
				AccountName:   testAzureAccountName,
				AccountKey:    testAzureAccountKey,
				Endpoint:      testAzureEndpoint,
				ContainerName: testBucket,
				AccessTier:    "Cold",
			},
			GcpStorage: &models.GcpStorage{},
			AwsS3:      &models.AwsS3{},
			Local:      &models.Local{},
		},
	}

	ctx := t.Context()

	writer, err := newWriter(ctx, params, slog.Default())
	require.NoError(t, err)
	assert.NotNil(t, writer)
	assert.Equal(t, testAzureType, writer.GetType())

	params = &config.BackupServiceConfig{
		Backup: &models.Backup{
			OutputFile: t.TempDir() + testFileName,
		},
		ServiceConfigCommon: config.ServiceConfigCommon{
			AzureBlob: &models.AzureBlob{
				AccountName:   testAzureAccountName,
				AccountKey:    testAzureAccountKey,
				Endpoint:      testAzureEndpoint,
				ContainerName: testBucket,
			},
			GcpStorage: &models.GcpStorage{},
			AwsS3:      &models.AwsS3{},
			Local:      &models.Local{},
		},
	}

	writer, err = newWriter(ctx, params, slog.Default())
	require.NoError(t, err)
	assert.NotNil(t, writer)
	assert.Equal(t, testAzureType, writer.GetType())
}

func createAzureContainer() error {
	cfg := &models.AzureBlob{
		AccountName:   testAzureAccountName,
		AccountKey:    testAzureAccountKey,
		Endpoint:      testAzureEndpoint,
		ContainerName: testBucket,
	}

	c, err := newAzureClient(cfg)
	if err != nil {
		return err
	}

	ctx := context.Background()

	_, _ = c.CreateContainer(ctx, testBucket, nil)

	return nil
}

func TestNewBackupWriter_LocalSuccess(t *testing.T) {
	t.Parallel()

	params := &config.BackupServiceConfig{
		Backup: &models.Backup{
			RemoveFiles: true,
			Common: models.Common{
				Directory: t.TempDir(),
			},
		},
		ServiceConfigCommon: config.ServiceConfigCommon{
			AwsS3:      &models.AwsS3{},
			GcpStorage: &models.GcpStorage{},
			AzureBlob:  &models.AzureBlob{},
			Local:      &models.Local{},
		},
	}

	writer, err := NewBackupWriter(t.Context(), params, slog.Default())
	require.NoError(t, err)
	require.NotNil(t, writer)
	assert.Equal(t, testLocalType, writer.GetType())
}

func TestNewBackupWriter_RemoveArtifactsReturnsNil(t *testing.T) {
	t.Parallel()

	params := &config.BackupServiceConfig{
		Backup: &models.Backup{
			RemoveArtifacts: true,
			Common: models.Common{
				Directory: t.TempDir(),
			},
		},
		ServiceConfigCommon: config.ServiceConfigCommon{
			AwsS3:      &models.AwsS3{},
			GcpStorage: &models.GcpStorage{},
			AzureBlob:  &models.AzureBlob{},
			Local:      &models.Local{},
		},
	}

	writer, err := NewBackupWriter(t.Context(), params, slog.Default())
	require.NoError(t, err)
	assert.Nil(t, writer)
}

func TestNewBackupWriter_WrapsInnerError(t *testing.T) {
	t.Parallel()

	// Passing nil params triggers an error in newWriter; NewBackupWriter must
	// wrap it with the "failed to create backup writer" prefix.
	writer, err := NewBackupWriter(t.Context(), nil, slog.Default())
	require.Error(t, err)
	assert.Nil(t, writer)
	assert.Contains(t, err.Error(), "failed to create backup writer")
	assert.Contains(t, err.Error(), "params cannot be nil")
}

func TestNewWriter_NilParams(t *testing.T) {
	t.Parallel()

	writer, err := newWriter(t.Context(), nil, slog.Default())
	require.Error(t, err)
	assert.Nil(t, writer)
	assert.Contains(t, err.Error(), "params cannot be nil")
}

func TestNewWriter_NilLogger(t *testing.T) {
	t.Parallel()

	params := &config.BackupServiceConfig{
		Backup: &models.Backup{Common: models.Common{Directory: t.TempDir()}},
		ServiceConfigCommon: config.ServiceConfigCommon{
			AwsS3:      &models.AwsS3{},
			GcpStorage: &models.GcpStorage{},
			AzureBlob:  &models.AzureBlob{},
			Local:      &models.Local{},
		},
	}

	writer, err := newWriter(t.Context(), params, nil)
	require.Error(t, err)
	assert.Nil(t, writer)
	assert.Contains(t, err.Error(), "logger cannot be nil")
}

// TestNewWriter_XDRLocal exercises the BackupXDR branch in newWriter,
// getDirectoryOutputFile and getShouldCleanContinue, plus the isXDR branch in newWriterOpts.
func TestNewWriter_XDRLocal(t *testing.T) {
	t.Parallel()

	params := &config.BackupServiceConfig{
		BackupXDR: &models.BackupXDR{
			Directory:   t.TempDir(),
			Namespace:   "test",
			RemoveFiles: true,
		},
		ServiceConfigCommon: config.ServiceConfigCommon{
			AwsS3:      &models.AwsS3{},
			GcpStorage: &models.GcpStorage{},
			AzureBlob:  &models.AzureBlob{},
			Local:      &models.Local{},
		},
	}

	writer, err := newWriter(t.Context(), params, slog.Default())
	require.NoError(t, err)
	require.NotNil(t, writer)
	assert.Equal(t, testLocalType, writer.GetType())
}

// TestNewWriter_ContinueBackup hits the continueBackup branch in newWriterOpts.
// When Continue is set, ShouldClearTarget must be false, so only the
// continueBackup option (WithSkipDirCheck) is appended on top of the directory.
func TestNewWriter_ContinueBackup(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	params := &config.BackupServiceConfig{
		Backup: &models.Backup{
			Continue: "state-file",
			Common: models.Common{
				Directory: dir,
			},
		},
		ServiceConfigCommon: config.ServiceConfigCommon{
			AwsS3:      &models.AwsS3{},
			GcpStorage: &models.GcpStorage{},
			AzureBlob:  &models.AzureBlob{},
			Local:      &models.Local{},
		},
	}

	writer, err := newWriter(t.Context(), params, slog.Default())
	require.NoError(t, err)
	require.NotNil(t, writer)
	assert.Equal(t, testLocalType, writer.GetType())
}

func TestGetDirectoryOutputFile(t *testing.T) {
	t.Parallel()

	t.Run("backup populates directory and output file", func(t *testing.T) {
		t.Parallel()
		params := &config.BackupServiceConfig{
			Backup: &models.Backup{
				OutputFile: "out.bak",
				Common:     models.Common{Directory: "/tmp/dir"},
			},
		}
		dir, out := getDirectoryOutputFile(params)
		assert.Equal(t, "/tmp/dir", dir)
		assert.Equal(t, "out.bak", out)
	})

	t.Run("xdr falls back to xdr directory and empty output file", func(t *testing.T) {
		t.Parallel()
		params := &config.BackupServiceConfig{
			BackupXDR: &models.BackupXDR{Directory: "/tmp/xdr"},
		}
		dir, out := getDirectoryOutputFile(params)
		assert.Equal(t, "/tmp/xdr", dir)
		assert.Empty(t, out)
	})
}

func TestGetShouldCleanContinue(t *testing.T) {
	t.Parallel()

	t.Run("backup with remove-files and no continue", func(t *testing.T) {
		t.Parallel()
		params := &config.BackupServiceConfig{
			Backup: &models.Backup{RemoveFiles: true},
		}
		clean, cont := getShouldCleanContinue(params)
		assert.True(t, clean)
		assert.False(t, cont)
	})

	t.Run("backup with continue suppresses clean", func(t *testing.T) {
		t.Parallel()
		params := &config.BackupServiceConfig{
			Backup: &models.Backup{RemoveFiles: true, Continue: "abc"},
		}
		clean, cont := getShouldCleanContinue(params)
		assert.False(t, clean, "ShouldClearTarget must be false when Continue is set")
		assert.True(t, cont)
	})

	t.Run("xdr uses RemoveFiles directly and never continues", func(t *testing.T) {
		t.Parallel()
		params := &config.BackupServiceConfig{
			BackupXDR: &models.BackupXDR{RemoveFiles: true},
		}
		clean, cont := getShouldCleanContinue(params)
		assert.True(t, clean)
		assert.False(t, cont)
	})
}

func TestNewWriterOpts_OptionCount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		directory         string
		outputFile        string
		shouldClearTarget bool
		continueBackup    bool
		isXDR             bool
		// Expected minimum options: validator + logger plus the dir/file/clear/continue flags.
		minLen int
	}{
		{
			name:      "directory only",
			directory: "/tmp/d",
			minLen:    3,
		},
		{
			name:       "output file only",
			outputFile: "/tmp/d/file.bak",
			minLen:     3,
		},
		{
			name:              "directory with clear and continue and xdr",
			directory:         "/tmp/d",
			shouldClearTarget: true,
			continueBackup:    true,
			isXDR:             true,
			minLen:            5,
		},
		{
			name:   "neither directory nor output file",
			minLen: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			opts := newWriterOpts(
				tt.directory,
				tt.outputFile,
				tt.shouldClearTarget,
				tt.continueBackup,
				tt.isXDR,
				slog.Default(),
			)
			assert.GreaterOrEqual(t, len(opts), tt.minLen)
		})
	}
}

func TestNewStdWriter(t *testing.T) {
	t.Parallel()

	params := &config.BackupServiceConfig{
		Backup: &models.Backup{
			OutputFile: config.StdPlaceholder,
		},
		ServiceConfigCommon: config.ServiceConfigCommon{
			AwsS3:      &models.AwsS3{},
			GcpStorage: &models.GcpStorage{},
			AzureBlob:  &models.AzureBlob{},
			Local:      &models.Local{},
		},
	}

	writer, err := newWriter(t.Context(), params, slog.Default())
	require.NoError(t, err)
	assert.NotNil(t, writer)
	assert.Equal(t, testStdoutType, writer.GetType())
}
