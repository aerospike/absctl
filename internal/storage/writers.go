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
	"errors"
	"fmt"
	"log/slog"

	"github.com/aerospike/absctl/internal/config"
	"github.com/aerospike/absctl/internal/models"
	"github.com/aerospike/backup-go"
	"github.com/aerospike/backup-go/io/encoding/asb"
	"github.com/aerospike/backup-go/io/storage/aws/s3"
	"github.com/aerospike/backup-go/io/storage/azure/blob"
	"github.com/aerospike/backup-go/io/storage/gcp/storage"
	"github.com/aerospike/backup-go/io/storage/local"
	"github.com/aerospike/backup-go/io/storage/options"
	"github.com/aerospike/backup-go/io/storage/std"
)

// NewBackupWriter initializes and returns a backup.Writer
// based on the provided parameters or cleans up artifacts if required.
// It returns a nil writer without an error if the backup was launched
// with --remove-artifacts: the target is cleaned during writer initialization
// and no further work is needed.
func NewBackupWriter(
	ctx context.Context,
	params *config.BackupServiceConfig,
	logger *slog.Logger,
) (backup.Writer, error) {
	writer, err := newWriter(ctx, params, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create backup writer: %w", err)
	}

	// If backup was launched with --remove-artifacts, the folder is cleaned
	// on writer initialization, so we return nil to signal the caller to exit.
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
		return nil, errors.New("params cannot be nil")
	}

	if logger == nil {
		return nil, errors.New("logger cannot be nil")
	}

	directory := params.Backup.Directory
	outputFile := params.Backup.OutputFile
	shouldClearTarget := params.Backup.ShouldClearTarget()
	continueBackup := params.IsContinue()

	opts := newWriterOpts(directory, outputFile, shouldClearTarget, continueBackup, logger)

	logger.Info("initializing storage for writer",
		slog.String("directory", directory),
		slog.String("output-file", outputFile),
		slog.Bool("should-clear-target", shouldClearTarget),
		slog.Bool("continue-backup", continueBackup),
	)

	switch {
	case params.AwsS3 != nil && params.AwsS3.BucketName != "":
		defer logger.Info("initialized AWS storage writer",
			slog.String("bucket", params.AwsS3.BucketName),
			slog.String("storage-class", params.AwsS3.StorageClass),
			slog.Int("chunk-size", params.AwsS3.ChunkSize),
			slog.String("endpoint", params.AwsS3.Endpoint),
		)

		return newS3Writer(ctx, params.AwsS3, opts)
	case params.GcpStorage != nil && params.GcpStorage.BucketName != "":
		defer logger.Info("initialized GCP storage writer",
			slog.String("bucket", params.GcpStorage.BucketName),
			slog.Int("chunk-size", params.GcpStorage.ChunkSize),
			slog.String("endpoint", params.GcpStorage.Endpoint),
		)

		return newGcpWriter(ctx, params.GcpStorage, opts)
	case params.AzureBlob != nil && params.AzureBlob.ContainerName != "":
		defer logger.Info("initialized Azure storage writer",
			slog.String("container", params.AzureBlob.ContainerName),
			slog.String("access-tier", params.AzureBlob.AccessTier),
			slog.Int("block-size", params.AzureBlob.BlockSize),
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

func newWriterOpts(
	directory,
	outputFile string,
	shouldClearTarget,
	continueBackup bool,
	logger *slog.Logger,
) []options.Opt {
	opts := make([]options.Opt, 0, 6)

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

	return append(opts,
		options.WithValidator(asb.NewValidator()),
		options.WithLogger(logger),
	)
}

func newLocalWriter(ctx context.Context, l *models.Local, opts []options.Opt) (backup.Writer, error) {
	if l != nil {
		opts = append(opts, options.WithChunkSize(toBytes(l.BufferSize)))
	}

	return local.NewWriter(ctx, opts...)
}

func newStdWriter(ctx context.Context, bufferSizeMiB int) (backup.Writer, error) {
	return std.NewWriter(ctx, toBytes(bufferSizeMiB))
}

func newS3Writer(
	ctx context.Context,
	a *models.AwsS3,
	opts []options.Opt,
) (backup.Writer, error) {
	client, err := NewS3Client(ctx, a)
	if err != nil {
		return nil, err
	}

	if a.StorageClass != "" {
		opts = append(opts, options.WithStorageClass(a.StorageClass))
	}

	if a.CalculateChecksum {
		opts = append(opts, options.WithChecksum())
	}

	opts = append(opts,
		options.WithChunkSize(toBytes(a.ChunkSize)),
		options.WithUploadConcurrency(a.UploadConcurrency),
	)

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

	opts = append(opts, options.WithChunkSize(toBytes(g.ChunkSize)))

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

	opts = append(opts,
		options.WithChunkSize(toBytes(a.BlockSize)),
		options.WithUploadConcurrency(a.UploadConcurrency),
	)

	return blob.NewWriter(ctx, client, a.ContainerName, opts...)
}

func toBytes(m int) int {
	return m * 1024 * 1024
}
