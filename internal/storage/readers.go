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
	"path"
	"time"

	"github.com/aerospike/absctl/internal/config"
	"github.com/aerospike/absctl/internal/models"
	"github.com/aerospike/backup-go"
	"github.com/aerospike/backup-go/io/encoding/asb"
	"github.com/aerospike/backup-go/io/encoding/asbx"
	"github.com/aerospike/backup-go/io/storage/aws/s3"
	"github.com/aerospike/backup-go/io/storage/azure/blob"
	"github.com/aerospike/backup-go/io/storage/common"
	"github.com/aerospike/backup-go/io/storage/gcp/storage"
	"github.com/aerospike/backup-go/io/storage/local"
	"github.com/aerospike/backup-go/io/storage/options"
	"github.com/aerospike/backup-go/io/storage/std"
	bModels "github.com/aerospike/backup-go/models"
)

// NewRestoreReader creates and returns a reader based on the restore mode specified in RestoreServiceConfig.
func NewRestoreReader(
	ctx context.Context,
	params *config.RestoreServiceConfig,
	sa *backup.SecretAgentConfig,
	logger *slog.Logger,
) (reader, xdrReader backup.StreamingReader, err error) {
	switch params.Restore.Mode {
	case models.RestoreModeASB, models.RestoreModeAuto:
		reader, err = newReader(ctx, params, sa, false, logger)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create asb reader: %w", err)
		}

		return reader, nil, nil
	case models.RestoreModeASBX:
		xdrReader, err = newReader(ctx, params, sa, true, logger)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create asbx reader: %w", err)
		}

		return nil, xdrReader, nil
	default:
		reader, err = newReader(ctx, params, sa, false, logger)

		switch {
		case errors.Is(err, common.ErrEmptyStorage):
			reader = nil
		case err != nil:
			return nil, nil, fmt.Errorf("failed to create asb reader: %w", err)
		default:
		}

		xdrReader, err = newReader(ctx, params, sa, true, logger)

		switch {
		case errors.Is(err, common.ErrEmptyStorage):
			xdrReader = nil
		case err != nil:
			return nil, nil, fmt.Errorf("failed to create asbx reader: %w", err)
		default:
		}

		// If both readers are nil return an error, as no files were found.
		if reader == nil && xdrReader == nil {
			return nil, nil, err
		}

		return reader, xdrReader, nil
	}
}

// NewStateReader initialize reader for a state file.
func NewStateReader(
	ctx context.Context,
	params *config.BackupServiceConfig,
	sa *backup.SecretAgentConfig,
	logger *slog.Logger,
) (backup.StreamingReader, error) {
	if params.Backup == nil ||
		!params.Backup.ShouldSaveState() ||
		params.Backup.StateFileDst != "" {
		return nil, nil
	}

	stateFile := params.Backup.StateFileDst
	if params.Backup.Continue != "" {
		stateFile = params.Backup.Continue
	}

	restoreParams := &config.RestoreServiceConfig{
		Restore: &models.Restore{
			InputFile: stateFile,
			Common: models.Common{
				Directory: params.Backup.Directory,
			},
		},
	}

	logger.Info("initializing state file", slog.String("path", stateFile))

	return newReader(ctx, restoreParams, sa, false, logger)
}

func newReader(
	ctx context.Context,
	params *config.RestoreServiceConfig,
	sa *backup.SecretAgentConfig,
	isXdr bool,
	logger *slog.Logger,
) (backup.StreamingReader, error) {
	directory, inputFile := params.Restore.Directory, params.Restore.InputFile
	parentDirectory, directoryList := params.Restore.ParentDirectory, params.Restore.DirectoryList

	opts := newReaderOpts(directory, inputFile, parentDirectory, directoryList, isXdr, logger)

	logger.Info("initializing storage for reader",
		slog.String("directory", directory),
		slog.String("input_file", inputFile),
		slog.String("parent_directory", parentDirectory),
		slog.String("directory_list", directoryList),
	)

	switch {
	case params.AwsS3 != nil && params.AwsS3.BucketName != "":
		defer logger.Info("initialized AWS storage reader",
			slog.String("bucket", params.AwsS3.BucketName),
			slog.String("access_tier", params.AwsS3.AccessTier),
			slog.Int("chunk_size", params.AwsS3.ChunkSize),
			slog.String("endpoint", params.AwsS3.Endpoint),
		)

		if err := params.AwsS3.LoadSecrets(ctx, sa); err != nil {
			return nil, fmt.Errorf("failed to load AWS secrets: %w", err)
		}

		return newS3Reader(ctx, params.AwsS3, opts, logger)
	case params.GcpStorage != nil && params.GcpStorage.BucketName != "":
		defer logger.Info("initialized GCP storage reader",
			slog.String("bucket", params.GcpStorage.BucketName),
			slog.Int("chunk_size", params.GcpStorage.ChunkSize),
			slog.String("endpoint", params.GcpStorage.Endpoint),
		)

		if err := params.GcpStorage.LoadSecrets(ctx, sa); err != nil {
			return nil, fmt.Errorf("failed to load GCP secrets: %w", err)
		}

		return newGcpReader(ctx, params.GcpStorage, opts)
	case params.AzureBlob != nil && params.AzureBlob.ContainerName != "":
		defer logger.Info("initialized Azure storage reader",
			slog.String("container", params.AzureBlob.ContainerName),
			slog.String("access_tier", params.AzureBlob.AccessTier),
			slog.Int("block_size", params.AzureBlob.BlockSize),
			slog.String("endpoint", params.AzureBlob.Endpoint),
		)

		if err := params.AzureBlob.LoadSecrets(ctx, sa); err != nil {
			return nil, fmt.Errorf("failed to load azure secrets: %w", err)
		}

		return newAzureReader(ctx, params.AzureBlob, opts, logger)
	case params.IsStdin():
		defer logger.Info("initialized standard input reader")
		return newStdReader(ctx, params.Restore.StdBufferSize)
	default:
		defer logger.Info("initialized local storage reader")
		return newLocalReader(ctx, opts)
	}
}

func newReaderOpts(
	directory,
	inputFile,
	parentDirectory,
	directoryList string,
	isXDR bool,
	logger *slog.Logger,
) []options.Opt {
	opts := make([]options.Opt, 0)

	// As we validate this field in validation function, we can switch here.
	switch {
	case directory != "":
		opts = append(opts, options.WithDir(directory))
	case inputFile != "":
		opts = append(opts, options.WithFile(inputFile))
	case directoryList != "":
		dirList := prepareDirectoryList(parentDirectory, directoryList)
		opts = append(opts, options.WithDirList(dirList))
	}

	// Append Validator always. As it is not applied to direct file reading.
	if isXDR {
		opts = append(opts, options.WithValidator(asbx.NewValidator()), options.WithSorting())
	} else {
		opts = append(opts, options.WithValidator(asb.NewValidator()))
	}

	// options.WithCalculateTotalSize() is required for estimating total size of backup.
	// Without it, restore will hang forever.
	opts = append(opts, options.WithLogger(logger), options.WithCalculateTotalSize())

	return opts
}

func newLocalReader(ctx context.Context, opts []options.Opt) (backup.StreamingReader, error) {
	return local.NewReader(ctx, opts...)
}

func newStdReader(ctx context.Context, bufferSizeMiB int) (backup.StreamingReader, error) {
	bufferSizeBytes := bufferSizeMiB * 1024 * 1024
	return std.NewReader(ctx, bufferSizeBytes)
}

func newS3Reader(
	ctx context.Context,
	a *models.AwsS3,
	opts []options.Opt,
	logger *slog.Logger,
) (backup.StreamingReader, error) {
	client, err := newS3Client(ctx, a)
	if err != nil {
		return nil, err
	}

	if a.AccessTier != "" {
		opts = append(
			opts,
			options.WithAccessTier(a.AccessTier),
			options.WithLogger(logger),
			options.WithWarmPollDuration(time.Duration(a.RestorePollDuration)*time.Millisecond),
		)
	}

	opts = append(opts, options.WithRetryPolicy(
		bModels.NewRetryPolicy(
			time.Duration(a.RetryReadBackoff)*time.Millisecond,
			a.RetryReadMultiplier,
			a.RetryReadMaxAttempts),
	))

	return s3.NewReader(ctx, client, a.BucketName, opts...)
}

func newGcpReader(
	ctx context.Context,
	g *models.GcpStorage,
	opts []options.Opt,
) (backup.StreamingReader, error) {
	client, err := newGcpClient(ctx, g)
	if err != nil {
		return nil, err
	}

	opts = append(opts, options.WithRetryPolicy(
		bModels.NewRetryPolicy(
			time.Duration(g.RetryReadBackoff)*time.Millisecond,
			g.RetryReadMultiplier,
			g.RetryReadMaxAttempts),
	))

	return storage.NewReader(ctx, client, g.BucketName, opts...)
}

func newAzureReader(
	ctx context.Context,
	a *models.AzureBlob,
	opts []options.Opt,
	logger *slog.Logger,
) (backup.StreamingReader, error) {
	client, err := newAzureClient(a)
	if err != nil {
		return nil, err
	}

	if a.AccessTier != "" {
		opts = append(
			opts,
			options.WithAccessTier(a.AccessTier),
			options.WithLogger(logger),
			options.WithWarmPollDuration(time.Duration(a.RestorePollDuration)*time.Millisecond),
		)
	}

	opts = append(opts, options.WithRetryPolicy(
		bModels.NewRetryPolicy(
			time.Duration(a.RetryReadBackoff)*time.Millisecond,
			a.RetryReadMultiplier,
			a.RetryReadMaxAttempts),
	))

	return blob.NewReader(ctx, client, a.ContainerName, opts...)
}

// prepareDirectoryList parses command line parameters and return slice of strings.
func prepareDirectoryList(parentDir, dirList string) []string {
	result := models.SplitByComma(dirList)
	if parentDir != "" {
		for i := range result {
			result[i] = path.Join(parentDir, result[i])
		}
	}

	return result
}
