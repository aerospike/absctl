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

package testutil

import (
	"bytes"
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"
)

// FakeS3 is an in-memory S3 endpoint serving one bucket with fixed objects. It
// answers only what the absctl readers and writers ask while they are created and
// while they read: HeadBucket, ListObjectsV2 without pagination, HeadObject and
// GetObject with ranges. A request for another bucket or a missing key fails with
// 404 Not Found; any other request fails with 501 Not Implemented.
// It is meant for tests of client lifetime, not of S3 behavior.
type FakeS3 struct {
	// URL is the endpoint to configure the S3 client with, in path style.
	URL string

	bucket  string
	objects map[string][]byte
}

// NewFakeS3 starts a FakeS3 serving bucket with objects, keyed by object key.
// The server stops when the test ends, after the cleanups registered later.
func NewFakeS3(t *testing.T, bucket string, objects map[string][]byte) *FakeS3 {
	t.Helper()

	f := &FakeS3{bucket: bucket, objects: objects}

	srv := httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(srv.Close)

	f.URL = srv.URL

	return f
}

// listBucketResult is the ListObjectsV2 response body.
type listBucketResult struct {
	XMLName     xml.Name       `xml:"ListBucketResult"`
	Name        string         `xml:"Name"`
	Prefix      string         `xml:"Prefix"`
	KeyCount    int            `xml:"KeyCount"`
	MaxKeys     int            `xml:"MaxKeys"`
	IsTruncated bool           `xml:"IsTruncated"`
	Contents    []listedObject `xml:"Contents"`
}

// listedObject is one object of a listBucketResult.
type listedObject struct {
	Key  string `xml:"Key"`
	Size int    `xml:"Size"`
}

func (f *FakeS3) serve(w http.ResponseWriter, r *http.Request) {
	bucket, key, _ := strings.Cut(strings.TrimPrefix(r.URL.Path, "/"), "/")
	if bucket != f.bucket {
		http.Error(w, "no such bucket", http.StatusNotFound)

		return
	}

	switch {
	case key == "" && r.Method == http.MethodHead:
		w.WriteHeader(http.StatusOK)
	case key == "" && r.Method == http.MethodGet && r.URL.Query().Get("list-type") == "2":
		f.list(w, r.URL.Query().Get("prefix"))
	case key != "" && (r.Method == http.MethodGet || r.Method == http.MethodHead):
		data, ok := f.objects[key]
		if !ok {
			http.Error(w, "no such key", http.StatusNotFound)

			return
		}

		http.ServeContent(w, r, key, time.Time{}, bytes.NewReader(data))
	default:
		http.Error(w, "not implemented by FakeS3", http.StatusNotImplemented)
	}
}

func (f *FakeS3) list(w http.ResponseWriter, prefix string) {
	res := listBucketResult{Name: f.bucket, Prefix: prefix, MaxKeys: 1000}

	for key, data := range f.objects {
		if strings.HasPrefix(key, prefix) {
			res.Contents = append(res.Contents, listedObject{Key: key, Size: len(data)})
		}
	}

	// S3 lists keys in order.
	slices.SortFunc(res.Contents, func(a, b listedObject) int { return strings.Compare(a.Key, b.Key) })

	res.KeyCount = len(res.Contents)

	body, err := xml.Marshal(res)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)

		return
	}

	w.Header().Set("Content-Type", "application/xml")
	_, _ = w.Write(append([]byte(xml.Header), body...))
}
