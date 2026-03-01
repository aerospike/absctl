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

	"github.com/aerospike/absctl/internal/config"
	"github.com/aerospike/absctl/internal/models"
	"github.com/aerospike/backup-go"
	"github.com/aerospike/backup-go/io/encoding/asb"
	"github.com/aerospike/backup-go/io/encoding/asbx"
	"github.com/aerospike/backup-go/io/storage/aws/s3"
	"github.com/aerospike/backup-go/io/storage/azure/blob"
	"github.com/aerospike/backup-go/io/storage/gcp/storage"
	"github.com/aerospike/backup-go/io/storage/local"
	"github.com/aerospike/backup-go/io/storage/options"
	"github.com/aerospike/backup-go/io/storage/std"
)

// NewBackupWriter initializes and returns a backup.Writer
// based on the provided parameters or cleans up artifacts if required.
func NewBackupWriter(
	ctx context.Context,
	params *config.BackupServiceConfig,
	logger *slog.Logger,
) (backup.Writer, error) {
	// We initialize a writer only if output is configured.
	writer, err := newWriter(ctx, params, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create backup writer: %w", err)
	}

	// If backup was launched with --remove-artifacts, we don't need to initialize all clients.
	// We clean the folder on writer initialization and exit.
	if params.Backup != nil && params.Backup.RemoveArtifacts {
		return nil, nil
	}

	return writer, nil
}

func newWriter(
	ctx context.Context,
	params *config.BackupServiceConfig,
	logger *slog.Logger,
) (backup.Writer, error) {
	if params == nil {
		return nil, fmt.Errorf("params cannot be nil")
	}

	if logger == nil {
		return nil, fmt.Errorf("logger cannot be nil")
	}

	directory, outputFile := getDirectoryOutputFile(params)
	shouldClearTarget, continueBackup := getShouldCleanContinue(params)
	opts := newWriterOpts(directory, outputFile, shouldClearTarget, continueBackup, params.IsXDR(), logger)

	logger.Info("initializing storage for writer",
		slog.String("directory", directory),
		slog.String("output_file", outputFile),
		slog.Bool("should_clear_target", shouldClearTarget),
		slog.Bool("continue_backup", continueBackup),
	)

	switch {
	case params.AwsS3 != nil && params.AwsS3.BucketName != "":
		defer logger.Info("initialized AWS storage writer",
			slog.String("bucket", params.AwsS3.BucketName),
			slog.String("storage_class", params.AwsS3.StorageClass),
			slog.Int("chunk_size", params.AwsS3.ChunkSize),
			slog.String("endpoint", params.AwsS3.Endpoint),
		)

		return newS3Writer(ctx, params.AwsS3, opts)
	case params.GcpStorage != nil && params.GcpStorage.BucketName != "":
		defer logger.Info("initialized GCP storage writer",
			slog.String("bucket", params.GcpStorage.BucketName),
			slog.Int("chunk_size", params.GcpStorage.ChunkSize),
			slog.String("endpoint", params.GcpStorage.Endpoint),
		)

		return newGcpWriter(ctx, params.GcpStorage, opts)
	case params.AzureBlob != nil && params.AzureBlob.ContainerName != "":
		defer logger.Info("initialized Azure storage writer",
			slog.String("container", params.AzureBlob.ContainerName),
			slog.String("access_tier", params.AzureBlob.AccessTier),
			slog.Int("block_size", params.AzureBlob.BlockSize),
			slog.String("endpoint", params.AzureBlob.Endpoint),
		)

		return newAzureWriter(ctx, params.AzureBlob, opts)
	case params.IsStdout():
		defer logger.Info("initialized standard output writer")
		return newStdWriter(ctx, params.Backup.StdBufferSize)
	default:
		defer logger.Info("initialized local storage writer")
		return newLocalWriter(ctx, params.Local, opts)
	}
}

func getDirectoryOutputFile(params *config.BackupServiceConfig) (directory, outputFile string) {
	if params.Backup != nil {
		return params.Backup.Directory, params.Backup.OutputFile
	}
	// Xdr backup.
	return params.BackupXDR.Directory, ""
}

func getShouldCleanContinue(params *config.BackupServiceConfig) (shouldClearTarget, continueBackup bool) {
	if params.Backup != nil {
		return params.Backup.ShouldClearTarget(), params.Backup.Continue != ""
	}
	// Xdr backup.

	return params.BackupXDR.RemoveFiles, false
}

func newWriterOpts(
	directory,
	outputFile string,
	shouldClearTarget,
	continueBackup,
	isXDR bool,
	logger *slog.Logger,
) []options.Opt {
	opts := make([]options.Opt, 0)

	if directory != "" && outputFile == "" {
		opts = append(opts, options.WithDir(directory))
	}

	if outputFile != "" && directory == "" {
		opts = append(opts, options.WithFile(outputFile))
	}

	if shouldClearTarget {
		opts = append(opts, options.WithRemoveFiles())
	}

	if continueBackup {
		opts = append(opts, options.WithSkipDirCheck())
	}

	if isXDR {
		opts = append(opts, options.WithValidator(asbx.NewValidator()))
	} else {
		opts = append(opts, options.WithValidator(asb.NewValidator()))
	}

	opts = append(opts, options.WithLogger(logger))

	return opts
}

func newLocalWriter(ctx context.Context, l *models.Local, opts []options.Opt) (backup.Writer, error) {
	if l != nil {
		opts = append(opts, options.WithChunkSize(l.BufferSize))
	}

	return local.NewWriter(ctx, opts...)
}

func newStdWriter(ctx context.Context, bufferSizeMiB int) (backup.Writer, error) {
	bufferSizeBytes := bufferSizeMiB * 1024 * 1024
	return std.NewWriter(ctx, bufferSizeBytes)
}

func newS3Writer(
	ctx context.Context,
	a *models.AwsS3,
	opts []options.Opt,
) (backup.Writer, error) {
	client, err := newS3Client(ctx, a)
	if err != nil {
		return nil, err
	}

	if a.StorageClass != "" {
		opts = append(opts, options.WithStorageClass(a.StorageClass))
	}

	if a.CalculateChecksum {
		opts = append(opts, options.WithChecksum())
	}

	chunkSize := a.ChunkSize * 1024 * 1024
	opts = append(opts, options.WithChunkSize(chunkSize), options.WithUploadConcurrency(a.UploadConcurrency))

	return s3.NewWriter(ctx, client, a.BucketName, opts...)
}

func newGcpWriter(
	ctx context.Context,
	g *models.GcpStorage,
	opts []options.Opt,
) (backup.Writer, error) {
	client, err := newGcpClient(ctx, g)
	if err != nil {
		return nil, err
	}

	if g.CalculateChecksum {
		opts = append(opts, options.WithChecksum())
	}

	chunkSize := g.ChunkSize * 1024 * 1024
	opts = append(opts, options.WithChunkSize(chunkSize))

	return storage.NewWriter(ctx, client, g.BucketName, opts...)
}

func newAzureWriter(
	ctx context.Context,
	a *models.AzureBlob,
	opts []options.Opt,
) (backup.Writer, error) {
	client, err := newAzureClient(a)
	if err != nil {
		return nil, err
	}

	if a.AccessTier != "" {
		opts = append(opts, options.WithStorageClass(a.AccessTier))
	}

	if a.CalculateChecksum {
		opts = append(opts, options.WithChecksum())
	}

	chunkSize := a.BlockSize * 1024 * 1024
	opts = append(opts, options.WithChunkSize(chunkSize), options.WithUploadConcurrency(a.UploadConcurrency))

	return blob.NewWriter(ctx, client, a.ContainerName, opts...)
}
