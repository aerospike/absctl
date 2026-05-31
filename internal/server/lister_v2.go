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
	metadataFileName   = "metadata.json"
	defaultConcurrency = 32

	minCitrusleafTS  int64 = 157_766_400
	clockSkewSeconds int64 = 24 * 60 * 60
	citrusleafEpoch  int64 = 1262304000
)

var errMetadataNotFound = errors.New("metadata.json not found")

type S3API interface {
	ListObjectsV2(ctx context.Context, in *s3.ListObjectsV2Input, opts ...func(*s3.Options),
	) (*s3.ListObjectsV2Output, error)
	GetObject(ctx context.Context, in *s3.GetObjectInput, opts ...func(*s3.Options)) (*s3.GetObjectOutput, error)
}

type ListerV2 struct {
	client      S3API
	bucket      string
	prefix      string
	concurrency int
	logger      *slog.Logger
}

type MetadataResult struct {
	Prefix string
	Data   []byte
	Err    error
}

func NewListerV2(client S3API, bucket string, logger *slog.Logger) *ListerV2 {
	return &ListerV2{
		bucket:      bucket,
		client:      client,
		concurrency: defaultConcurrency,
		logger:      logger,
	}
}

func (l *ListerV2) FetchAllMetadata(ctx context.Context) error {
	prefixes, err := l.ListSnapshotPrefixes(ctx)
	if err != nil {
		return err
	}

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(l.concurrency)

	for _, p := range prefixes {
		g.Go(func() error {
			data, err := l.fetchOne(gctx, p)
			if errors.Is(err, errMetadataNotFound) {
				l.logger.Debug("metadata.json missing (that means folder doesn't contain finished backup), skipping",
					slog.String("prefix", p))

				return nil
			}

			md, err := readMetafile(data)
			if err != nil {
				return fmt.Errorf("read metadata for prefix %s: %w", p, err)
			}

			logging.PrintMetadata(nil, md, true, l.logger)

			return nil
		})
	}
	_ = g.Wait()

	return nil
}

func (l *ListerV2) ListSnapshotPrefixes(ctx context.Context) ([]string, error) {
	var out []string

	pg := s3.NewListObjectsV2Paginator(l.client, &s3.ListObjectsV2Input{
		Bucket:    aws.String(l.bucket),
		Prefix:    aws.String(l.prefix),
		Delimiter: aws.String("/"),
	})

	for pg.HasMorePages() {
		page, err := pg.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("list snapshot prefixes: %w", err)
		}

		for _, cp := range page.CommonPrefixes {
			if cp.Prefix == nil {
				continue
			}

			name := strings.TrimSuffix(strings.TrimPrefix(*cp.Prefix, l.prefix), "/")
			if !isCitrusleafTimestamp(name) {
				slog.DebugContext(ctx, "skipping non-snapshot prefix", slog.String("prefix", name))
				continue
			}

			out = append(out, strings.TrimSuffix(*cp.Prefix, "/"))
		}
	}

	return out, nil
}

func (l *ListerV2) fetchOne(ctx context.Context, prefix string) ([]byte, error) {
	key := prefix + "/" + metadataFileName

	out, err := l.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(l.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if isNotFound(err) {
			return nil, errMetadataNotFound
		}

		return nil, fmt.Errorf("get %s: %w", key, err)
	}
	defer out.Body.Close()

	const maxSize = 16 << 20

	data, err := io.ReadAll(io.LimitReader(out.Body, maxSize))
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", key, err)
	}

	return data, nil
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

// readMetafile reads the content of a BackupEntry.
func readMetafile(data []byte) (models.Metadata, error) {
	var b models.Metadata
	if err := json.Unmarshal(data, &b); err != nil {
		return models.Metadata{}, fmt.Errorf("failed to parse metadata: %w", err)
	}

	return b, nil
}
