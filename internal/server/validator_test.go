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
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/segmentio/asm/base64"
)

const (
	testBucket   = "abs-testing-bucket"
	testBackupID = "519118324"
	testNS       = "source-ns1"
)

// fakeS3 is a hand-written S3API stub. Each operation is backed by a function
// field so a test wires only the behavior it needs. The fields are set once
// before use and the handlers only read shared state, so the stub is safe for
// the concurrent calls the Validator makes.
type fakeS3 struct {
	listObjectsV2Func func(context.Context, *s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error)
	headObjectFunc    func(context.Context, *s3.HeadObjectInput) (*s3.HeadObjectOutput, error)
	getObjectFunc     func(context.Context, *s3.GetObjectInput) (*s3.GetObjectOutput, error)
}

func (f *fakeS3) ListObjectsV2(
	ctx context.Context,
	params *s3.ListObjectsV2Input,
	_ ...func(*s3.Options),
) (*s3.ListObjectsV2Output, error) {
	return f.listObjectsV2Func(ctx, params)
}

func (f *fakeS3) HeadObject(
	ctx context.Context,
	params *s3.HeadObjectInput,
	_ ...func(*s3.Options),
) (*s3.HeadObjectOutput, error) {
	return f.headObjectFunc(ctx, params)
}

func (f *fakeS3) GetObject(
	ctx context.Context,
	params *s3.GetObjectInput,
	_ ...func(*s3.Options),
) (*s3.GetObjectOutput, error) {
	return f.getObjectFunc(ctx, params)
}

// Compile-time assertion that the stub satisfies the interface.
var _ S3API = (*fakeS3)(nil)

// --- helpers shared by the tests ---

// listOutput builds a ListObjectsV2 response from common prefixes and keys.
func listOutput(commonPrefixes, keys []string) *s3.ListObjectsV2Output {
	out := &s3.ListObjectsV2Output{}
	for _, p := range commonPrefixes {
		out.CommonPrefixes = append(out.CommonPrefixes, types.CommonPrefix{Prefix: aws.String(p)})
	}
	for _, k := range keys {
		out.Contents = append(out.Contents, types.Object{Key: aws.String(k)})
	}
	return out
}

// bodyOutput builds a GetObject response with a fresh reader over b.
func bodyOutput(b []byte) *s3.GetObjectOutput {
	return &s3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader(b))}
}

// headChecksum builds a HeadObject response carrying a base64 CRC32; an empty
// string means the object has no checksum in its metadata.
func headChecksum(b64 string) *s3.HeadObjectOutput {
	out := &s3.HeadObjectOutput{}
	if b64 != "" {
		out.ChecksumCRC32 = aws.String(b64)
	}
	return out
}

// crc32Hex returns the IEEE CRC32 of b as the hex string stored in a manifest.
func crc32Hex(b []byte) string {
	return fmt.Sprintf("%08x", crc32.ChecksumIEEE(b))
}

// crc32Base64 returns the IEEE CRC32 of b as the base64 string S3 reports in
// the x-amz-checksum-crc32 header.
func crc32Base64(b []byte) string {
	buf := make([]byte, crc32.Size)
	binary.BigEndian.PutUint32(buf, crc32.ChecksumIEEE(b))
	return base64.StdEncoding.EncodeToString(buf)
}

func newTestValidator(t *testing.T, client S3API) *Validator {
	t.Helper()
	logger := slog.New(slog.DiscardHandler)
	return NewValidator(client, testBucket, true, logger)
}

// nsPrefix is the prefix the Validator lists to discover namespaces.
func nsPrefix() string {
	return testBackupID + "/manifest/ns/"
}

// manifestKey builds a manifest key for a partition/node.
func manifestKey(partition, node string) string {
	return fmt.Sprintf("%s/manifest/ns/%s/%s/%s/%s",
		testBackupID, testNS, partition, node, manifestFileName)
}

// segmentKey builds a segment key.
func segmentKey(partition, node, name string) string {
	return fmt.Sprintf("%s/segment/ns/%s/%s/%s/%s", testBackupID, testNS, partition, node, name)
}

// marshalManifest serializes a manifest to JSON bytes.
//
//nolint:gocritic // I'm passing huge manifest for testing.
func marshalManifest(t *testing.T, m manifest) []byte {
	t.Helper()
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	return b
}

// singleSegmentManifest builds a manifest with one segment.
func singleSegmentManifest(segKey, checksum string) manifest {
	return manifest{
		BackupID:          testBackupID,
		Namespace:         testNS,
		ChecksumAlgorithm: checksumAlgorithmCRC32,
		Segments: []segment{
			{SegmentID: 0, SegmentName: segKey, Size: 64, Checksum: checksum},
		},
	}
}

// listFunc routes the two kinds of list calls: with a delimiter it returns the
// namespaces, without it returns the manifest keys.
func listFunc(namespaces, manifestKeys []string) func(context.Context, *s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error) {
	prefixes := make([]string, len(namespaces))
	for i, ns := range namespaces {
		prefixes[i] = nsPrefix() + ns + "/"
	}
	return func(_ context.Context, in *s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error) {
		if in.Delimiter != nil {
			return listOutput(prefixes, nil), nil
		}
		return listOutput(nil, manifestKeys), nil
	}
}

func TestValidate_MetadataChecksumMatch(t *testing.T) {
	payload := []byte("segment payload")
	segKey := segmentKey("p1641", "BB9A3A819F05A22", "s0.seg")
	mKey := manifestKey("p1641", "BB9A3A819F05A22")
	manifestBytes := marshalManifest(t, singleSegmentManifest(segKey, crc32Hex(payload)))

	fake := &fakeS3{
		listObjectsV2Func: listFunc([]string{testNS}, []string{mKey}),
		headObjectFunc: func(_ context.Context, _ *s3.HeadObjectInput) (*s3.HeadObjectOutput, error) {
			return headChecksum(crc32Base64(payload)), nil
		},
		getObjectFunc: func(_ context.Context, in *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
			key := aws.ToString(in.Key)
			if strings.HasSuffix(key, manifestFileName) {
				return bodyOutput(manifestBytes), nil
			}
			// The metadata path must not download the segment body.
			t.Errorf("unexpected segment download for %q", key)
			return nil, errors.New("unexpected download")
		},
	}

	report, err := newTestValidator(t, fake).Validate(t.Context(), testBackupID, SampleAll)
	if err != nil {
		t.Fatalf("Validate returned error: %v", err)
	}

	if len(report.Issues) != 0 {
		t.Fatalf("expected no issues, got %d: %+v", len(report.Issues), report.Issues)
	}
	if report.TotalSegments != 1 || report.CheckedSegments != 1 {
		t.Fatalf("unexpected counts: total=%d checked=%d", report.TotalSegments, report.CheckedSegments)
	}
	if report.VerifiedByMetadata != 1 || report.VerifiedByDownload != 0 {
		t.Fatalf("unexpected verification split: metadata=%d download=%d",
			report.VerifiedByMetadata, report.VerifiedByDownload)
	}
}

func TestValidate_DownloadFallbackMatch(t *testing.T) {
	payload := []byte("another payload")
	segKey := segmentKey("p1", "NODE", "s0.seg")
	mKey := manifestKey("p1", "NODE")
	manifestBytes := marshalManifest(t, singleSegmentManifest(segKey, crc32Hex(payload)))

	fake := &fakeS3{
		listObjectsV2Func: listFunc([]string{testNS}, []string{mKey}),
		headObjectFunc: func(_ context.Context, _ *s3.HeadObjectInput) (*s3.HeadObjectOutput, error) {
			return headChecksum(""), nil // no checksum in metadata
		},
		getObjectFunc: func(_ context.Context, in *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
			if strings.HasSuffix(aws.ToString(in.Key), manifestFileName) {
				return bodyOutput(manifestBytes), nil
			}
			return bodyOutput(payload), nil
		},
	}

	report, err := newTestValidator(t, fake).Validate(t.Context(), testBackupID, SampleAll)
	if err != nil {
		t.Fatalf("Validate returned error: %v", err)
	}

	if len(report.Issues) != 0 {
		t.Fatalf("expected no issues, got %+v", report.Issues)
	}
	if report.VerifiedByMetadata != 0 || report.VerifiedByDownload != 1 {
		t.Fatalf("unexpected verification split: metadata=%d download=%d",
			report.VerifiedByMetadata, report.VerifiedByDownload)
	}
}

func TestValidate_ChecksumMismatch(t *testing.T) {
	payload := []byte("real bytes")
	segKey := segmentKey("p7", "NODE", "s0.seg")
	mKey := manifestKey("p7", "NODE")
	// The manifest records a wrong checksum.
	manifestBytes := marshalManifest(t, singleSegmentManifest(segKey, "deadbeef"))

	fake := &fakeS3{
		listObjectsV2Func: listFunc([]string{testNS}, []string{mKey}),
		headObjectFunc: func(_ context.Context, _ *s3.HeadObjectInput) (*s3.HeadObjectOutput, error) {
			return headChecksum(crc32Base64(payload)), nil
		},
		getObjectFunc: func(_ context.Context, _ *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
			return bodyOutput(manifestBytes), nil
		},
	}

	report, err := newTestValidator(t, fake).Validate(t.Context(), testBackupID, SampleAll)
	if err != nil {
		t.Fatalf("Validate returned error: %v", err)
	}

	if len(report.Issues) != 1 {
		t.Fatalf("expected one issue, got %d", len(report.Issues))
	}
	issue := report.Issues[0]
	if issue.Err != nil {
		t.Fatalf("expected a mismatch, got error: %v", issue.Err)
	}
	if issue.Expected != "deadbeef" || issue.Got != crc32Hex(payload) {
		t.Fatalf("unexpected issue values: expected=%q got=%q", issue.Expected, issue.Got)
	}
	// A mismatch still counts as a verified segment.
	if report.VerifiedByMetadata != 1 {
		t.Fatalf("expected 1 verified by metadata, got %d", report.VerifiedByMetadata)
	}
}

func TestValidate_DownloadError(t *testing.T) {
	segKey := segmentKey("p7", "NODE", "s0.seg")
	mKey := manifestKey("p7", "NODE")
	manifestBytes := marshalManifest(t, singleSegmentManifest(segKey, "60c79b77"))

	fake := &fakeS3{
		listObjectsV2Func: listFunc([]string{testNS}, []string{mKey}),
		headObjectFunc: func(_ context.Context, _ *s3.HeadObjectInput) (*s3.HeadObjectOutput, error) {
			return headChecksum(""), nil
		},
		getObjectFunc: func(_ context.Context, in *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
			if strings.HasSuffix(aws.ToString(in.Key), manifestFileName) {
				return bodyOutput(manifestBytes), nil
			}
			return nil, errors.New("network down")
		},
	}

	report, err := newTestValidator(t, fake).Validate(t.Context(), testBackupID, SampleAll)
	if err != nil {
		t.Fatalf("Validate returned error: %v", err)
	}

	if len(report.Issues) != 1 || report.Issues[0].Err == nil {
		t.Fatalf("expected one issue carrying an error, got %+v", report.Issues)
	}
	if report.VerifiedByMetadata != 0 || report.VerifiedByDownload != 0 {
		t.Fatalf("a failed segment must not be counted as verified: metadata=%d download=%d",
			report.VerifiedByMetadata, report.VerifiedByDownload)
	}
}

func TestValidate_CompositeChecksumFallsBackToDownload(t *testing.T) {
	payload := []byte("multipart object")
	segKey := segmentKey("p3", "NODE", "s0.seg")
	mKey := manifestKey("p3", "NODE")
	manifestBytes := marshalManifest(t, singleSegmentManifest(segKey, crc32Hex(payload)))

	var downloaded atomic.Bool
	fake := &fakeS3{
		listObjectsV2Func: listFunc([]string{testNS}, []string{mKey}),
		headObjectFunc: func(_ context.Context, _ *s3.HeadObjectInput) (*s3.HeadObjectOutput, error) {
			// Composite multipart checksum: not comparable to a plain CRC32.
			return headChecksum("q1bc3w==-3"), nil
		},
		getObjectFunc: func(_ context.Context, in *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
			if strings.HasSuffix(aws.ToString(in.Key), manifestFileName) {
				return bodyOutput(manifestBytes), nil
			}
			downloaded.Store(true)
			return bodyOutput(payload), nil
		},
	}

	report, err := newTestValidator(t, fake).Validate(t.Context(), testBackupID, SampleAll)
	if err != nil {
		t.Fatalf("Validate returned error: %v", err)
	}

	if !downloaded.Load() {
		t.Fatal("expected fallback to download on composite checksum")
	}
	if len(report.Issues) != 0 || report.VerifiedByDownload != 1 {
		t.Fatalf("unexpected result: issues=%+v download=%d", report.Issues, report.VerifiedByDownload)
	}
}

func TestValidate_Sampling(t *testing.T) {
	const total = 100
	const sampleSize = 10

	payload := []byte("shared payload")
	mKey := manifestKey("p1", "NODE")

	segments := make([]segment, total)
	for i := range segments {
		segments[i] = segment{
			SegmentID:   i,
			SegmentName: segmentKey("p1", "NODE", fmt.Sprintf("s%d.seg", i)),
			Size:        int64(len(payload)),
			Checksum:    crc32Hex(payload),
		}
	}
	manifestBytes := marshalManifest(t, manifest{
		BackupID:          testBackupID,
		Namespace:         testNS,
		ChecksumAlgorithm: checksumAlgorithmCRC32,
		Segments:          segments,
	})

	var headCalls atomic.Int64
	fake := &fakeS3{
		listObjectsV2Func: listFunc([]string{testNS}, []string{mKey}),
		headObjectFunc: func(_ context.Context, _ *s3.HeadObjectInput) (*s3.HeadObjectOutput, error) {
			headCalls.Add(1)
			return headChecksum(crc32Base64(payload)), nil
		},
		getObjectFunc: func(_ context.Context, in *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
			if strings.HasSuffix(aws.ToString(in.Key), manifestFileName) {
				return bodyOutput(manifestBytes), nil
			}
			t.Errorf("unexpected segment download for %q", aws.ToString(in.Key))
			return nil, errors.New("unexpected download")
		},
	}

	report, err := newTestValidator(t, fake).Validate(t.Context(), testBackupID, sampleSize)
	if err != nil {
		t.Fatalf("Validate returned error: %v", err)
	}

	if report.TotalSegments != total {
		t.Fatalf("expected total %d, got %d", total, report.TotalSegments)
	}
	if report.CheckedSegments != sampleSize {
		t.Fatalf("expected %d checked, got %d", sampleSize, report.CheckedSegments)
	}
	if got := headCalls.Load(); got != sampleSize {
		t.Fatalf("expected %d HeadObject calls, got %d", sampleSize, got)
	}
	if len(report.Issues) != 0 {
		t.Fatalf("expected no issues, got %+v", report.Issues)
	}
}

func TestValidate_EmptyBackupID(t *testing.T) {
	if _, err := newTestValidator(t, &fakeS3{}).Validate(t.Context(), "", SampleAll); err == nil {
		t.Fatal("expected error for empty backup id")
	}
}

func TestValidate_NoNamespaces(t *testing.T) {
	fake := &fakeS3{
		listObjectsV2Func: func(_ context.Context, _ *s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error) {
			return listOutput(nil, nil), nil
		},
	}
	if _, err := newTestValidator(t, fake).Validate(t.Context(), testBackupID, SampleAll); err == nil {
		t.Fatal("expected error when no namespaces are found")
	}
}

func TestSampleIndices(t *testing.T) {
	t.Run("sample all when n is zero", func(t *testing.T) {
		idx := sampleIndices(10, SampleAll)
		if len(idx) != 10 {
			t.Fatalf("expected 10 indices, got %d", len(idx))
		}
		for i, v := range idx {
			if v != i {
				t.Fatalf("expected identity index at %d, got %d", i, v)
			}
		}
	})

	t.Run("sample all when n exceeds total", func(t *testing.T) {
		if idx := sampleIndices(5, 20); len(idx) != 5 {
			t.Fatalf("expected 5 indices, got %d", len(idx))
		}
	})

	t.Run("stratified spread", func(t *testing.T) {
		const total, n = 100, 10
		idx := sampleIndices(total, n)
		if len(idx) != n {
			t.Fatalf("expected %d indices, got %d", n, len(idx))
		}
		seen := make(map[int]struct{}, n)
		for i, v := range idx {
			lo, hi := i*total/n, (i+1)*total/n
			if v < lo || v >= hi {
				t.Fatalf("index %d (%d) outside stratum [%d,%d)", i, v, lo, hi)
			}
			if _, dup := seen[v]; dup {
				t.Fatalf("duplicate index %d", v)
			}
			seen[v] = struct{}{}
		}
	})
}

func TestNormalizeChecksum(t *testing.T) {
	if got := normalizeChecksum("  60C79B77 "); got != "60c79b77" {
		t.Fatalf("unexpected normalized checksum: %q", got)
	}
}

func TestFormatChecksum(t *testing.T) {
	if got := formatChecksum(0x60c79b77); got != "60c79b77" {
		t.Fatalf("unexpected formatted checksum: %q", got)
	}
	if got := formatChecksum(0xff); got != "000000ff" {
		t.Fatalf("expected zero padding, got %q", got)
	}
}
