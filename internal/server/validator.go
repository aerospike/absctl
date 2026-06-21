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
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"math/rand/v2"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/aerospike/absctl/internal/logging"
	sModels "github.com/aerospike/absctl/internal/server/models"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"golang.org/x/sync/errgroup"
)

// SampleAll can be passed as the sampleSize argument of Validate to check
// every segment of a backup instead of a random subset.
const SampleAll = 0

const (
	manifestFileName       = "manifest.json"
	checksumAlgorithmCRC32 = "crc32"
)

// Validator validates the integrity of an Aerospike backup stored in S3 by
// comparing per-segment CRC32 checksums against the values recorded in the
// backup manifests.
type Validator struct {
	client S3API
	logger *slog.Logger
	bucket string
	// If true, the output is logged to the logger; otherwise it is rendered to stderr.
	toLog bool
	// parallel bounds the number of concurrent S3 operations.
	parallel int
}

// NewValidator creates a Validator. Concurrency is derived from the number of
// available CPUs.
func NewValidator(client S3API, bucket string, toLog bool, logger *slog.Logger) *Validator {
	parallel := max(runtime.NumCPU(), 1)

	return &Validator{
		client:   client,
		logger:   logger,
		bucket:   bucket,
		toLog:    toLog,
		parallel: parallel,
	}
}

// manifest mirrors the on-disk manifest.json structure.
type manifest struct {
	BackupID          string    `json:"backup_id"`
	Namespace         string    `json:"namespace"`
	PartitionID       int       `json:"partition_id"`
	FormatVersion     int       `json:"format_version"`
	NodeID            string    `json:"node_id"`
	ChecksumAlgorithm string    `json:"checksum_algorithm"`
	IDRangeStart      int64     `json:"id_range_start"`
	IDRangeEnd        int64     `json:"id_range_end"`
	EntryCount        int       `json:"entry_count"`
	Segments          []segment `json:"segments"`
}

type segment struct {
	SegmentID   int    `json:"segment_id"`
	SegmentName string `json:"segment_name"`
	Size        int64  `json:"size"`
	Checksum    string `json:"checksum"`
}

// segmentRef is a flattened, validation-ready view of a manifest segment.
type segmentRef struct {
	namespace string
	name      string // full S3 key of the segment file
	checksum  string // expected CRC32, hex encoded
	algorithm string
}

// verifyMethod records how a segment checksum was obtained.
type verifyMethod uint8

const (
	verifyByMetadata verifyMethod = iota
	verifyByDownload
)

type segmentResult struct {
	ok     bool
	got    string
	method verifyMethod
	err    error
}

// Validate validates the backup identified by backupID.
//
// If sampleSize is SampleAll (or not less than the total number of segments),
// every segment is checked. Otherwise sampleSize segments are selected
// randomly but spread evenly across the whole backup.
func (v *Validator) Validate(ctx context.Context, backupID string, sampleSize int) (*sModels.Report, error) {
	if backupID == "" {
		return nil, errors.New("backup id must not be empty")
	}

	refs, err := v.discover(ctx, backupID)
	if err != nil {
		return nil, err
	}

	if len(refs) == 0 {
		return nil, fmt.Errorf("no segments found for backup %q", backupID)
	}

	selected := refs
	if sampleSize > 0 && sampleSize < len(refs) {
		idx := sampleIndices(len(refs), sampleSize)

		selected = make([]segmentRef, len(idx))
		for i, j := range idx {
			selected[i] = refs[j]
		}
	}

	outcome, err := v.validate(ctx, selected)
	if err != nil {
		return nil, err
	}

	report := &sModels.Report{
		BackupID:           backupID,
		TotalSegments:      len(refs),
		CheckedSegments:    len(selected),
		VerifiedByMetadata: outcome.verifiedByMetadata,
		VerifiedByDownload: outcome.verifiedByDownload,
		Issues:             outcome.issues,
	}

	return report, nil
}

// discover walks the manifest tree of a backup and returns a reference to every
// segment together with its expected checksum. Namespaces and manifests are
// processed concurrently.
func (v *Validator) discover(ctx context.Context, backupID string) ([]segmentRef, error) {
	namespaces, err := v.listNamespaces(ctx, backupID)
	if err != nil {
		return nil, fmt.Errorf("list namespaces: %w", err)
	}

	if len(namespaces) == 0 {
		return nil, fmt.Errorf("no namespaces found for backup %q", backupID)
	}

	// Collect all manifest keys across namespaces in parallel. Partitions that
	// were empty simply do not appear in the listing, so no existence check is
	// required.
	type manifestKey struct {
		namespace string
		key       string
	}

	var (
		keysMu sync.Mutex
		keys   []manifestKey
	)

	lg, lctx := errgroup.WithContext(ctx)
	lg.SetLimit(v.parallel)

	for _, ns := range namespaces {
		lg.Go(func() error {
			found, err := v.listManifests(lctx, backupID, ns)
			if err != nil {
				return fmt.Errorf("list manifests for namespace %q: %w", ns, err)
			}

			keysMu.Lock()
			for _, key := range found {
				keys = append(keys, manifestKey{namespace: ns, key: key})
			}
			keysMu.Unlock()

			return nil
		})
	}

	if err := lg.Wait(); err != nil {
		return nil, err
	}

	// Fetch and parse manifests in parallel; flatten their segments.
	var (
		refsMu sync.Mutex
		refs   []segmentRef
	)

	fg, fctx := errgroup.WithContext(ctx)
	fg.SetLimit(v.parallel)

	for _, mk := range keys {
		fg.Go(func() error {
			m, err := v.getManifest(fctx, mk.key)
			if err != nil {
				return fmt.Errorf("get manifest %q: %w", mk.key, err)
			}

			local := make([]segmentRef, 0, len(m.Segments))
			for _, s := range m.Segments {
				local = append(local, segmentRef{
					namespace: mk.namespace,
					name:      s.SegmentName,
					checksum:  s.Checksum,
					algorithm: m.ChecksumAlgorithm,
				})
			}

			refsMu.Lock()

			refs = append(refs, local...)
			refsMu.Unlock()

			return nil
		})
	}

	if err := fg.Wait(); err != nil {
		return nil, err
	}

	return refs, nil
}

// listNamespaces returns the namespaces present under the manifest tree of a
// backup.
func (v *Validator) listNamespaces(ctx context.Context, backupID string) ([]string, error) {
	prefix := backupID + "/manifest/ns/"

	pager := s3.NewListObjectsV2Paginator(v.client, &s3.ListObjectsV2Input{
		Bucket:    aws.String(v.bucket),
		Prefix:    aws.String(prefix),
		Delimiter: aws.String("/"),
	})

	var namespaces []string

	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, err
		}

		for _, cp := range page.CommonPrefixes {
			ns := strings.TrimSuffix(strings.TrimPrefix(aws.ToString(cp.Prefix), prefix), "/")
			if ns != "" {
				namespaces = append(namespaces, ns)
			}
		}
	}

	return namespaces, nil
}

// listManifests returns every manifest.json key under a namespace.
func (v *Validator) listManifests(ctx context.Context, backupID, namespace string) ([]string, error) {
	prefix := backupID + "/manifest/ns/" + namespace + "/"

	pager := s3.NewListObjectsV2Paginator(v.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(v.bucket),
		Prefix: aws.String(prefix),
	})

	var keys []string

	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, err
		}

		for _, obj := range page.Contents {
			key := aws.ToString(obj.Key)
			if strings.HasSuffix(key, "/"+manifestFileName) {
				keys = append(keys, key)
			}
		}
	}

	return keys, nil
}

// getManifest downloads and decodes a single manifest.json.
func (v *Validator) getManifest(ctx context.Context, key string) (*manifest, error) {
	out, err := v.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(v.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}
	defer out.Body.Close()

	var m manifest
	if err := json.NewDecoder(out.Body).Decode(&m); err != nil {
		return nil, fmt.Errorf("decode manifest: %w", err)
	}

	return &m, nil
}

// validationOutcome aggregates the result of validating a set of segments.
type validationOutcome struct {
	issues             []sModels.SegmentIssue
	verifiedByMetadata int
	verifiedByDownload int
}

// validate checks the given segments concurrently and collects every issue.
// Checksum mismatches and per-segment errors are recorded in the result rather
// than aborting the run; only context cancellation stops it early.
func (v *Validator) validate(ctx context.Context, refs []segmentRef) (validationOutcome, error) {
	var (
		mu     sync.Mutex
		issues []sModels.SegmentIssue

		byMetadata atomic.Int64
		byDownload atomic.Int64
	)

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(v.parallel)

	for _, ref := range refs {
		g.Go(func() error {
			if err := gctx.Err(); err != nil {
				return err
			}

			res := v.validateSegment(gctx, ref)

			if res.err == nil {
				switch res.method {
				case verifyByMetadata:
					byMetadata.Add(1)
				case verifyByDownload:
					byDownload.Add(1)
				}
			}

			if res.ok {
				return nil
			}

			mu.Lock()

			issues = append(issues, sModels.SegmentIssue{
				Namespace:   ref.namespace,
				SegmentName: ref.name,
				Expected:    normalizeChecksum(ref.checksum),
				Got:         res.got,
				Err:         res.err,
			})
			mu.Unlock()

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return validationOutcome{}, err
	}

	return validationOutcome{
		issues:             issues,
		verifiedByMetadata: int(byMetadata.Load()),
		verifiedByDownload: int(byDownload.Load()),
	}, nil
}

// validateSegment validates a single segment. It first asks S3 for the CRC32
// checksum stored in the object metadata to avoid downloading the body; if the
// object carries no such checksum, the whole object is streamed and its CRC32
// is computed.
func (v *Validator) validateSegment(ctx context.Context, ref segmentRef) segmentResult {
	if !strings.EqualFold(ref.algorithm, checksumAlgorithmCRC32) {
		return segmentResult{err: fmt.Errorf("unsupported checksum algorithm %q", ref.algorithm)}
	}

	expected := normalizeChecksum(ref.checksum)

	got, ok, err := v.metadataChecksum(ctx, ref.name)
	if err != nil {
		return segmentResult{err: fmt.Errorf("read checksum metadata: %w", err)}
	}

	if ok {
		return segmentResult{ok: got == expected, got: got, method: verifyByMetadata}
	}

	got, err = v.computeChecksum(ctx, ref.name)
	if err != nil {
		return segmentResult{err: fmt.Errorf("compute checksum: %w", err)}
	}

	return segmentResult{ok: got == expected, got: got, method: verifyByDownload}
}

// metadataChecksum asks S3 for the CRC32 checksum recorded in the object
// metadata (the x-amz-checksum-crc32 header). It returns the checksum as a hex
// string and ok=true when present. ok is false when the object has no CRC32
// checksum, or carries a composite (multipart) checksum that cannot be compared
// to a plain CRC32 of the payload, signaling that it must be computed from the
// body instead.
func (v *Validator) metadataChecksum(ctx context.Context, key string) (checksum string, ok bool, err error) {
	out, err := v.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket:       aws.String(v.bucket),
		Key:          aws.String(key),
		ChecksumMode: types.ChecksumModeEnabled,
	})
	if err != nil {
		return "", false, err
	}

	encoded := aws.ToString(out.ChecksumCRC32)
	if encoded == "" || strings.Contains(encoded, "-") {
		return "", false, nil
	}

	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return "", false, fmt.Errorf("decode checksum %q: %w", encoded, err)
	}

	if len(raw) != crc32.Size {
		return "", false, fmt.Errorf("unexpected checksum length %d", len(raw))
	}

	return formatChecksum(binary.BigEndian.Uint32(raw)), true, nil
}

// computeChecksum streams the whole object and returns its CRC32 (IEEE) as a
// hex string. Segments are small (<= 8 MiB), so the request stays cheap.
func (v *Validator) computeChecksum(ctx context.Context, key string) (string, error) {
	out, err := v.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(v.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return "", err
	}
	defer out.Body.Close()

	h := crc32.NewIEEE()
	if _, err := io.Copy(h, out.Body); err != nil {
		return "", err
	}

	return formatChecksum(h.Sum32()), nil
}

// sampleIndices returns n indices in [0, total) using stratified sampling: the
// range is split into n equal strata and one random index is picked from each,
// yielding an even yet randomized spread.
func sampleIndices(total, n int) []int {
	if n <= 0 || n >= total {
		idx := make([]int, total)
		for i := range idx {
			idx[i] = i
		}

		return idx
	}

	indices := make([]int, 0, n)
	for i := range n {
		lo := i * total / n

		hi := (i + 1) * total / n
		if hi <= lo {
			hi = lo + 1
		}
		//nolint:gosec // Using old rand.IntN it's not security critical.
		indices = append(indices, lo+rand.IntN(hi-lo))
	}

	return indices
}

func normalizeChecksum(s string) string {
	return strings.ToLower(strings.TrimSpace(s))
}

func formatChecksum(crc uint32) string {
	return fmt.Sprintf("%08x", crc)
}

// printReport renders the report either through the logger or to stderr.
func (v *Validator) printReport(r *sModels.Report) {
	if v.toLog {
		v.logReport(r)
		return
	}

	logging.PrintServerValidationReport(r)
}

func (v *Validator) logReport(r *sModels.Report) {
	if len(r.Issues) == 0 {
		v.logger.Info("backup validation passed",
			slog.String("backup-id", r.BackupID),
			slog.Int("total-segments", r.TotalSegments),
			slog.Int("checked-segments", r.CheckedSegments),
			slog.Int("verified-by-metadata", r.VerifiedByMetadata),
			slog.Int("verified-by-download", r.VerifiedByDownload),
		)

		return
	}

	v.logger.Error("backup validation failed",
		slog.String("backup-id", r.BackupID),
		slog.Int("total-segments", r.TotalSegments),
		slog.Int("checked-segments", r.CheckedSegments),
		slog.Int("verified-by-metadata", r.VerifiedByMetadata),
		slog.Int("verified-by-download", r.VerifiedByDownload),
		slog.Int("damaged-segments", len(r.Issues)),
	)

	for _, issue := range r.Issues {
		attrs := []any{
			slog.String("namespace", issue.Namespace),
			slog.String("segment", issue.SegmentName),
			slog.String("expected", issue.Expected),
		}
		if issue.Err != nil {
			attrs = append(attrs, slog.String("error", issue.Err.Error()))
		} else {
			attrs = append(attrs, slog.String("got", issue.Got))
		}

		v.logger.Error("damaged segment", attrs...)
	}
}
