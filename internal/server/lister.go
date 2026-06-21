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

package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aerospike/absctl/internal/logging"
	"github.com/aerospike/absctl/internal/models"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"golang.org/x/sync/errgroup"
)

const (
	metadataFileName = "metadata.json"
	maxMetadataSize  = 16 << 20

	minCitrusleafTS  int64 = 157_766_400
	clockSkewSeconds int64 = 24 * 60 * 60
	citrusleafEpoch  int64 = 1262304000
)

var errMetadataNotFound = errors.New("metadata.json not found")

// S3API is an interface for the S3 client.
type S3API interface {
	ListObjectsV2(ctx context.Context, in *s3.ListObjectsV2Input, opts ...func(*s3.Options),
	) (*s3.ListObjectsV2Output, error)
	GetObject(ctx context.Context, in *s3.GetObjectInput, opts ...func(*s3.Options),
	) (*s3.GetObjectOutput, error)
	HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options),
	) (*s3.HeadObjectOutput, error)
}

// Lister provides functionality to list backups from an S3 bucket.
// As development is ongoing, this is a work-in-progress, and may change.
// It supports listing all snapshots in a given prefix, or a single snapshot.
// The concurrency level can be configured, and the output can be logged or rendered to stderr.
// The logger is used for logging, and stderr is used for rendering.
type Lister struct {
	client      S3API
	logger      *slog.Logger
	writer      io.Writer
	bucket      string
	prefix      string
	concurrency int
	// If true, the output is logged to the logger; otherwise it is rendered to stderr.
	toLog bool
}

// NewLister creates a new backup Lister.
func NewLister(client S3API, bucket, prefix string, toLog bool, writer io.Writer, logger *slog.Logger) *Lister {
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	return &Lister{
		bucket:      bucket,
		prefix:      prefix,
		client:      client,
		concurrency: runtime.NumCPU(),
		logger:      logger,
		toLog:       toLog,
		writer:      writer,
	}
}

// FetchAllMetadata lists all metadata files in the given prefix.
func (l *Lister) FetchAllMetadata(ctx context.Context) error {
	prefixes, err := l.listSnapshotPrefixes(ctx)
	if err != nil {
		return err
	}

	if len(prefixes) == 0 {
		return nil
	}

	sort.Slice(prefixes, func(i, j int) bool {
		ti, _ := l.extractTimestamp(prefixes[i])
		tj, _ := l.extractTimestamp(prefixes[j])

		return ti < tj
	})

	futures := make([]chan models.Metadata, len(prefixes))
	for i := range futures {
		futures[i] = make(chan models.Metadata, 1)
	}

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(l.concurrency)

	go func() {
		for i, p := range prefixes {
			g.Go(func() error {
				defer close(futures[i])

				if gctx.Err() != nil {
					return nil
				}

				data, err := l.fetchOne(gctx, p)
				if errors.Is(err, errMetadataNotFound) {
					l.logger.DebugContext(gctx,
						"metadata.json missing (folder doesn't contain finished backup), skipping",
						slog.String("prefix", p))

					return nil
				}

				if err != nil {
					l.logger.WarnContext(gctx, "fetch metadata failed",
						slog.String("prefix", p), slog.Any("error", err))

					return nil
				}

				md, err := readMetafile(data)
				if err != nil {
					l.logger.WarnContext(gctx, "parse metadata failed",
						slog.String("prefix", p), slog.Any("error", err))

					return nil
				}

				futures[i] <- md

				return nil
			})
		}
	}()

	if err = l.printMetadata(ctx, l.writer, futures, g); err != nil {
		return fmt.Errorf("failed to print metadata: %w", err)
	}

	return g.Wait()
}

func (l *Lister) GetMetadata(ctx context.Context, backupID string) (models.Metadata, error) {
	data, err := l.fetchOne(ctx, backupID)
	if err != nil {
		return models.Metadata{}, fmt.Errorf("failed to get manifest %s: %w", backupID, err)
	}

	md, err := readMetafile(data)
	if err != nil {
		return models.Metadata{}, fmt.Errorf("failed to parse metadata: %w", err)
	}

	return md, nil
}

func (l *Lister) printMetadata(ctx context.Context, w io.Writer, futures []chan models.Metadata, g *errgroup.Group,
) error {
	for _, f := range futures {
		select {
		case md, ok := <-f:
			if !ok {
				continue
			}

			logging.PrintMetadata(w, md, l.toLog, l.logger)
		case <-ctx.Done():
			_ = g.Wait()
			return ctx.Err()
		}
	}

	return nil
}

func (l *Lister) listSnapshotPrefixes(ctx context.Context) ([]string, error) {
	var out []string

	pg := s3.NewListObjectsV2Paginator(l.client, &s3.ListObjectsV2Input{
		Bucket:    aws.String(l.bucket),
		Prefix:    aws.String(l.prefix),
		Delimiter: aws.String("/"),
	})

	for pg.HasMorePages() {
		page, err := pg.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to read next page: %w", err)
		}

		for _, cp := range page.CommonPrefixes {
			if cp.Prefix == nil {
				continue
			}

			name := strings.TrimSuffix(strings.TrimPrefix(*cp.Prefix, l.prefix), "/")
			if !isCitrusleafTimestamp(name) {
				l.logger.DebugContext(ctx, "skipping non-snapshot prefix", slog.String("prefix", name))
				continue
			}

			out = append(out, strings.TrimSuffix(*cp.Prefix, "/"))
		}
	}

	return out, nil
}

func (l *Lister) fetchOne(ctx context.Context, prefix string) ([]byte, error) {
	key := prefix + "/" + metadataFileName

	out, err := l.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(l.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if isNotFound(err) {
			return nil, errMetadataNotFound
		}

		return nil, fmt.Errorf("failed to get %s: %w", key, err)
	}
	defer out.Body.Close()

	data, err := io.ReadAll(io.LimitReader(out.Body, maxMetadataSize))
	if err != nil {
		return nil, fmt.Errorf("failed to read %s: %w", key, err)
	}

	return data, nil
}

func (l *Lister) extractTimestamp(fullPrefix string) (int64, error) {
	name := strings.TrimPrefix(fullPrefix, l.prefix)
	return strconv.ParseInt(name, 10, 64)
}

func isCitrusleafTimestamp(s string) bool {
	ts, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return false
	}

	return ts >= minCitrusleafTS && ts <= (time.Now().Unix()-citrusleafEpoch)+clockSkewSeconds
}

func isNotFound(err error) bool {
	if _, ok := errors.AsType[*types.NoSuchKey](err); ok {
		return true
	}

	if _, ok := errors.AsType[*types.NotFound](err); ok {
		return true
	}

	if apiErr, ok := errors.AsType[smithy.APIError](err); ok {
		switch apiErr.ErrorCode() {
		case "NoSuchKey", "NotFound", "404":
			return true
		}
	}

	return false
}

func readMetafile(data []byte) (models.Metadata, error) {
	var b models.Metadata
	if err := json.Unmarshal(data, &b); err != nil {
		return models.Metadata{}, fmt.Errorf("failed to parse metadata: %w", err)
	}

	return b, nil
}
