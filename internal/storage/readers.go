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
	"log/slog"
	"path"
	"time"

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
	bModels "github.com/aerospike/backup-go/models"
)

// NewRestoreReader creates and returns a reader based on the restore mode specified in RestoreServiceConfig.
func NewRestoreReader(
	ctx context.Context,
	cfg *config.RestoreServiceConfig,
	logger *slog.Logger,
) (backup.StreamingReader, error) {
	return NewReader(
		ctx,
		&cfg.ServiceConfigCommon,
		cfg.Restore.Directory,
		cfg.Restore.InputFile,
		cfg.Restore.ParentDirectory,
		cfg.Restore.DirectoryList,
		cfg.Restore.StdBufferSize,
		false,
		logger,
	)
}

// NewStateReader initializes a reader for a state file.
// It returns a nil reader without an error if the state file is not used.
func NewStateReader(
	ctx context.Context,
	cfg *config.BackupServiceConfig,
	logger *slog.Logger,
) (backup.StreamingReader, error) {
	if cfg.Backup == nil ||
		!cfg.Backup.ShouldSaveState() ||
		cfg.Backup.StateFileDst != "" {
		return nil, nil
	}

	logger.Info("initializing state file", slog.String("path", cfg.Backup.Continue))

	return NewReader(
		ctx,
		&cfg.ServiceConfigCommon,
		cfg.Backup.Directory,
		cfg.Backup.Continue,
		"",
		"",
		0,
		false,
		logger,
	)
}

// NewReader creates and returns a reader based on the provided parameters.
func NewReader(
	ctx context.Context,
	cfg *config.ServiceConfigCommon,
	directory,
	inputFile,
	parentDirectory,
	directoryList string,
	stdBufferSize int,
	skipCheck bool,
	logger *slog.Logger,
) (backup.StreamingReader, error) {
	opts := newReaderOpts(directory, inputFile, parentDirectory, directoryList, skipCheck, logger)

	logger.Info("initializing storage for reader",
		slog.String("directory", directory),
		slog.String("input-file", inputFile),
		slog.String("parent-directory", parentDirectory),
		slog.String("directory-list", directoryList),
	)

	switch {
	case cfg.AwsS3 != nil && cfg.AwsS3.BucketName != "":
		defer logger.Info("initialized AWS storage reader",
			slog.String("bucket", cfg.AwsS3.BucketName),
			slog.String("access-tier", cfg.AwsS3.AccessTier),
			slog.String("endpoint", cfg.AwsS3.Endpoint),
		)

		return newS3Reader(ctx, cfg.AwsS3, opts)
	case cfg.GcpStorage != nil && cfg.GcpStorage.BucketName != "":
		defer logger.Info("initialized GCP storage reader",
			slog.String("bucket", cfg.GcpStorage.BucketName),
			slog.String("endpoint", cfg.GcpStorage.Endpoint),
		)

		return newGcpReader(ctx, cfg.GcpStorage, opts)
	case cfg.AzureBlob != nil && cfg.AzureBlob.ContainerName != "":
		defer logger.Info("initialized Azure storage reader",
			slog.String("container", cfg.AzureBlob.ContainerName),
			slog.String("access-tier", cfg.AzureBlob.AccessTier),
			slog.String("endpoint", cfg.AzureBlob.Endpoint),
		)

		return newAzureReader(ctx, cfg.AzureBlob, opts)
	case inputFile == config.StdPlaceholder:
		defer logger.Info("initialized standard input reader")
		return newStdReader(ctx, stdBufferSize)
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
	skipCheck bool,
	logger *slog.Logger,
) []options.Opt {
	opts := make([]options.Opt, 0, 5)

	// As we validate these fields in the validation function, we can switch here.
	switch {
	case directory != "":
		opts = append(opts, options.WithDir(directory))
	case inputFile != "":
		opts = append(opts, options.WithFile(inputFile))
	case directoryList != "":
		dirList := prepareDirectoryList(parentDirectory, directoryList)
		opts = append(opts, options.WithDirList(dirList))
	}

	if skipCheck {
		opts = append(opts, options.WithSkipDirCheck(), options.WithNestedDir())
	} else {
		opts = append(opts, options.WithValidator(asb.NewValidator()))
	}

	// options.WithCalculateTotalSize() is required for estimating the total size of a backup.
	// Without it, restore will hang forever.
	return append(opts, options.WithLogger(logger), options.WithCalculateTotalSize())
}

func newLocalReader(ctx context.Context, opts []options.Opt) (backup.StreamingReader, error) {
	return local.NewReader(ctx, opts...)
}

func newStdReader(ctx context.Context, bufferSizeMiB int) (backup.StreamingReader, error) {
	return std.NewReader(ctx, toBytes(bufferSizeMiB))
}

func newS3Reader(
	ctx context.Context,
	a *models.AwsS3,
	opts []options.Opt,
) (backup.StreamingReader, error) {
	client, err := NewS3Client(ctx, a)
	if err != nil {
		return nil, err
	}

	if a.AccessTier != "" {
		opts = append(opts,
			options.WithAccessTier(a.AccessTier),
			options.WithWarmPollDuration(time.Duration(a.RestorePollDuration)*time.Millisecond),
		)
	}

	opts = append(opts, options.WithRetryPolicy(
		newReadRetryPolicy(a.RetryReadBackoff, a.RetryReadMultiplier, a.RetryReadMaxAttempts),
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
		newReadRetryPolicy(g.RetryReadBackoff, g.RetryReadMultiplier, g.RetryReadMaxAttempts),
	))

	return storage.NewReader(ctx, client, g.BucketName, opts...)
}

func newAzureReader(
	ctx context.Context,
	a *models.AzureBlob,
	opts []options.Opt,
) (backup.StreamingReader, error) {
	client, err := newAzureClient(a)
	if err != nil {
		return nil, err
	}

	if a.AccessTier != "" {
		opts = append(opts,
			options.WithAccessTier(a.AccessTier),
			options.WithWarmPollDuration(time.Duration(a.RestorePollDuration)*time.Millisecond),
		)
	}

	opts = append(opts, options.WithRetryPolicy(
		newReadRetryPolicy(a.RetryReadBackoff, a.RetryReadMultiplier, a.RetryReadMaxAttempts),
	))

	return blob.NewReader(ctx, client, a.ContainerName, opts...)
}

// newReadRetryPolicy builds a read retry policy from millisecond-based flag values.
func newReadRetryPolicy(backoffMs int, multiplier float64, maxAttempts uint) *bModels.RetryPolicy {
	return bModels.NewRetryPolicy(
		time.Duration(backoffMs)*time.Millisecond,
		multiplier,
		maxAttempts,
	)
}

// prepareDirectoryList parses command line parameters and returns a slice of directory paths.
func prepareDirectoryList(parentDir, dirList string) []string {
	result := models.SplitByComma(dirList)
	if parentDir != "" {
		for i := range result {
			result[i] = path.Join(parentDir, result[i])
		}
	}

	return result
}
