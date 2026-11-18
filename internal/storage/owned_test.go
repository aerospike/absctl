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

package storage

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"encoding/xml"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aerospike/absctl/internal/config"
	"github.com/aerospike/absctl/internal/models"
	"github.com/aerospike/absctl/internal/testutil"
	"github.com/aerospike/backup-go/io/encoding/asb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	releaseBucket    = "release-bucket"
	releaseDir       = "backup"
	releaseEmptyDir  = releaseDir + "-empty"
	releaseNamespace = "test"
	releaseSet       = "test"
	releaseRegion    = "us-east-1"
	releaseKeyID     = "fake-key-id"
	releaseSecret    = "fake-secret"
	releaseChunkSize = 5

	// gcsAPIPath is the path of the GCS JSON API on a fake GCS server.
	gcsAPIPath = "/storage/v1/"
	// gcsTokenPath is the path of the OAuth token endpoint on a fake GCS server.
	gcsTokenPath = "/token"

	headerContentType = "Content-Type"
	queryPrefix       = "prefix"
	gcsFieldName      = "name"

	leakTestWait = 5 * time.Second
	leakTestTick = 10 * time.Millisecond
)

// TestNewOwned checks that a reader or writer releases its client exactly once,
// however many times it is closed, and that a failed creation releases at once.
func TestNewOwned(t *testing.T) {
	t.Parallel()

	var (
		errCreate  = errors.New("create failed")
		errRelease = errors.New("release failed")
	)

	ctors := []struct {
		name string
		// newOwned binds no reader or writer to release; only release is under test.
		newOwned func(err error, release releaseFunc) (io.Closer, error)
	}{
		{
			name: "writer",
			newOwned: func(err error, release releaseFunc) (io.Closer, error) {
				return newOwnedWriter(nil, err, release)
			},
		},
		{
			name: "reader",
			newOwned: func(err error, release releaseFunc) (io.Closer, error) {
				return newOwnedReader(nil, err, release)
			},
		},
	}

	tests := []struct {
		name         string
		giveErr      error
		giveRelease  error
		giveCloses   int
		wantErrs     []error
		wantNil      bool
		wantReleases int
		wantCloseErr error
	}{
		{
			name:         "closed once",
			giveCloses:   1,
			wantReleases: 1,
		},
		{
			name:         "closed twice releases once",
			giveCloses:   2,
			wantReleases: 1,
		},
		{
			name:         "close returns release error",
			giveRelease:  errRelease,
			giveCloses:   2,
			wantReleases: 1,
			wantCloseErr: errRelease,
		},
		{
			name:         "failed creation releases at once",
			giveErr:      errCreate,
			wantErrs:     []error{errCreate},
			wantNil:      true,
			wantReleases: 1,
		},
		{
			name:         "failed creation joins release error",
			giveErr:      errCreate,
			giveRelease:  errRelease,
			wantErrs:     []error{errCreate, errRelease},
			wantNil:      true,
			wantReleases: 1,
		},
	}

	for _, tt := range tests {
		for _, ctor := range ctors {
			t.Run(tt.name+"/"+ctor.name, func(t *testing.T) {
				t.Parallel()

				var releases int

				c, err := ctor.newOwned(tt.giveErr, func() error {
					releases++

					return tt.giveRelease
				})
				for _, wantErr := range tt.wantErrs {
					require.ErrorIs(t, err, wantErr)
				}

				assert.Equal(t, tt.wantNil, c == nil)

				for range tt.giveCloses {
					require.ErrorIs(t, c.Close(), tt.wantCloseErr)
				}

				assert.Equal(t, tt.wantReleases, releases)
			})
		}
	}
}

// TestStorageRelease checks that each reader and writer the services create, on each
// object storage, releases its client: no connection, and so no goroutine, is left
// behind once it is closed.
func TestStorageRelease(t *testing.T) {
	// Not parallel: VerifyNoGoroutineLeaks needs the goroutines of each subtest alone.
	backupFile := testutil.NewBackupFile(t, releaseNamespace, releaseSet)
	objects := map[string][]byte{releaseDir + "/data" + asb.Extension: backupFile}

	storages := []struct {
		name string
		// start starts a fake of the storage holding objects and returns the config
		// pointing at it. The fake stops after the cleanups registered later.
		start func(t *testing.T, objects map[string][]byte) config.ServiceConfigCommon
	}{
		{
			name: "s3",
			start: func(t *testing.T, objects map[string][]byte) config.ServiceConfigCommon {
				t.Helper()

				fake := testutil.NewFakeS3(t, releaseBucket, objects)

				return config.ServiceConfigCommon{AwsS3: &models.AwsS3{
					BucketName:        releaseBucket,
					Region:            releaseRegion,
					Endpoint:          fake.URL,
					AccessKeyID:       releaseKeyID,
					SecretAccessKey:   releaseSecret,
					ChunkSize:         releaseChunkSize,
					UploadConcurrency: 1,
				}}
			},
		},
		{
			name: "gcs",
			start: func(t *testing.T, objects map[string][]byte) config.ServiceConfigCommon {
				t.Helper()

				srv := newFakeGCS(t, objects)

				return config.ServiceConfigCommon{GcpStorage: &models.GcpStorage{
					BucketName: releaseBucket,
					Endpoint:   srv.URL + gcsAPIPath,
					ChunkSize:  releaseChunkSize,
				}}
			},
		},
		{
			name: "azure",
			start: func(t *testing.T, objects map[string][]byte) config.ServiceConfigCommon {
				t.Helper()

				srv := newFakeAzure(t, objects)

				return config.ServiceConfigCommon{AzureBlob: &models.AzureBlob{
					ContainerName:     releaseBucket,
					Endpoint:          srv.URL,
					BlockSize:         releaseChunkSize,
					UploadConcurrency: 1,
				}}
			},
		},
	}

	ops := []struct {
		name string
		// open creates the reader or writer under test, makes sure the storage is no
		// longer in use, and returns it; nil when it is released before returning.
		open func(t *testing.T, common config.ServiceConfigCommon) io.Closer
	}{
		{
			name: "backup writer",
			open: func(t *testing.T, common config.ServiceConfigCommon) io.Closer {
				t.Helper()

				w, err := NewBackupWriter(t.Context(), &config.BackupServiceConfig{
					Backup:              &models.Backup{Directory: releaseEmptyDir},
					ServiceConfigCommon: common,
				}, quietLogger())
				require.NoError(t, err)

				return w
			},
		},
		{
			name: "backup writer removing artifacts",
			open: func(t *testing.T, common config.ServiceConfigCommon) io.Closer {
				t.Helper()

				w, err := NewBackupWriter(t.Context(), &config.BackupServiceConfig{
					Backup:              &models.Backup{Directory: releaseEmptyDir, RemoveArtifacts: true},
					ServiceConfigCommon: common,
				}, quietLogger())
				require.NoError(t, err)
				require.Nil(t, w)

				return nil
			},
		},
		{
			name: "restore reader",
			open: func(t *testing.T, common config.ServiceConfigCommon) io.Closer {
				t.Helper()

				r, err := NewRestoreReader(t.Context(), &config.RestoreServiceConfig{
					Restore:             &models.Restore{Directory: releaseDir},
					ServiceConfigCommon: common,
				}, quietLogger())
				require.NoError(t, err)

				waitForTotalSize(t, r, int64(len(backupFile)))

				return r
			},
		},
		{
			name: "state reader",
			open: func(t *testing.T, common config.ServiceConfigCommon) io.Closer {
				t.Helper()

				r, err := NewStateReader(t.Context(), &config.BackupServiceConfig{
					Backup:              &models.Backup{Directory: releaseDir, Continue: "state"},
					ServiceConfigCommon: common,
				}, quietLogger())
				require.NoError(t, err)

				waitForTotalSize(t, r, int64(len(backupFile)))

				return r
			},
		},
	}

	for _, st := range storages {
		for _, op := range ops {
			t.Run(st.name+"/"+op.name, func(t *testing.T) {
				// Started before the leak check, so that the fake outlives it and cannot
				// close the client connections itself.
				common := st.start(t, objects)

				testutil.VerifyNoGoroutineLeaks(t)
				testutil.IsolateAWSConfig(t)

				if c := op.open(t, common); c != nil {
					require.NoError(t, c.Close())
				}
			})
		}
	}
}

// TestGcpAuthRelease checks that a GCP client built by newGcpClient on the
// authenticated path is released by closeGcp, where client.Close cannot reach the
// transport through the auth wrapper, including the connections its key-file token
// source opened.
func TestGcpAuthRelease(t *testing.T) {
	// Not parallel: VerifyNoGoroutineLeaks and t.Setenv need the process to themselves.

	// Started before the leak check, so that the server outlives it.
	srv := newFakeGCS(t, nil)

	testutil.VerifyNoGoroutineLeaks(t)

	// Sends the data requests of the authenticated client to the fake; the key file
	// sends its token requests there too.
	t.Setenv("STORAGE_EMULATOR_HOST", srv.URL)

	c, transport, err := newGcpClient(t.Context(), &models.GcpStorage{
		KeyFile: writeServiceAccountKey(t, srv.URL+gcsTokenPath),
	})
	require.NoError(t, err)

	// Fetch a token and the bucket through transport, leaving their connection idle
	// in the pool.
	_, err = c.Bucket(releaseBucket).Attrs(t.Context())
	require.NoError(t, err)

	require.NoError(t, closeGcp(c, transport)())
}

// newFakeGCS starts a GCS server serving releaseBucket with objects through the JSON API
// at gcsAPIPath, enough to create readers and writers, and an OAuth token endpoint at
// gcsTokenPath. It stops after the cleanups registered later.
func newFakeGCS(t *testing.T, objects map[string][]byte) *httptest.Server {
	t.Helper()

	bucketPath := gcsAPIPath + "b/" + releaseBucket

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(headerContentType, "application/json")

		switch r.URL.Path {
		case gcsTokenPath:
			writeJSON(w, map[string]any{"access_token": "fake-token", "token_type": "Bearer", "expires_in": 3600})
		case bucketPath:
			writeJSON(w, map[string]any{gcsFieldName: releaseBucket})
		case bucketPath + "/o":
			prefix := r.URL.Query().Get(queryPrefix)
			items := make([]map[string]any, 0, len(objects))

			for name, data := range objects {
				if strings.HasPrefix(name, prefix) {
					items = append(items, map[string]any{
						gcsFieldName: name, "bucket": releaseBucket, "size": strconv.Itoa(len(data)),
					})
				}
			}

			writeJSON(w, map[string]any{"kind": "storage#objects", "items": items})
		default:
			http.Error(w, "not implemented by the fake GCS", http.StatusNotImplemented)
		}
	}))
	t.Cleanup(srv.Close)

	return srv
}

// azureBlobList is the List Blobs response body.
type azureBlobList struct {
	XMLName xml.Name    `xml:"EnumerationResults"`
	Prefix  string      `xml:"Prefix"`
	Blobs   []azureBlob `xml:"Blobs>Blob"`
}

// azureBlob is one blob of an azureBlobList.
type azureBlob struct {
	Name          string `xml:"Name"`
	ContentLength int    `xml:"Properties>Content-Length"`
	BlobType      string `xml:"Properties>BlobType"`
}

// newFakeAzure starts an Azure Blob server serving the container releaseBucket with
// objects, enough to create readers and writers. It stops after the cleanups
// registered later.
func newFakeAzure(t *testing.T, objects map[string][]byte) *httptest.Server {
	t.Helper()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query()

		switch {
		case r.URL.Path != "/"+releaseBucket || query.Get("restype") != "container":
			http.Error(w, "not implemented by the fake Azure", http.StatusNotImplemented)
		case query.Get("comp") == "list":
			res := azureBlobList{Prefix: query.Get(queryPrefix)}

			for name, data := range objects {
				if strings.HasPrefix(name, res.Prefix) {
					res.Blobs = append(res.Blobs, azureBlob{
						Name: name, ContentLength: len(data), BlobType: "BlockBlob",
					})
				}
			}

			body, err := xml.Marshal(res)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)

				return
			}

			w.Header().Set(headerContentType, "application/xml")
			_, _ = w.Write(append([]byte(xml.Header), body...))
		default:
			// Container properties.
			w.Header().Set("ETag", `"0x1"`)
			w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
			w.WriteHeader(http.StatusOK)
		}
	}))
	t.Cleanup(srv.Close)

	return srv
}

// writeJSON writes v as the JSON response body.
func writeJSON(w http.ResponseWriter, v any) {
	if err := json.NewEncoder(w).Encode(v); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// writeServiceAccountKey writes a GCP service account key file with a key generated
// for the test, whose tokens are fetched from tokenURL, and returns its path.
func writeServiceAccountKey(t *testing.T, tokenURL string) string {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	der, err := x509.MarshalPKCS8PrivateKey(key)
	require.NoError(t, err)

	data, err := json.Marshal(map[string]string{
		"type":           "service_account",
		"project_id":     "release-project",
		"private_key_id": "release-key",
		"private_key":    string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})),
		"client_email":   "release@release-project.iam.gserviceaccount.com",
		"client_id":      "1",
		"token_uri":      tokenURL,
	})
	require.NoError(t, err)

	path := filepath.Join(t.TempDir(), "key.json")
	require.NoError(t, os.WriteFile(path, data, 0o600))

	return path
}

// waitForTotalSize waits for the background size calculation of r to end, so that its
// listing no longer uses a connection when r is closed, and checks it found want bytes.
// backup-go publishes the size through GetSize once the listing is read: 0 until then,
// -1 when the calculation fails.
func waitForTotalSize(t *testing.T, r Reader, want int64) {
	t.Helper()

	require.Eventually(t, func() bool { return r.GetSize() != 0 }, leakTestWait, leakTestTick,
		"the size calculation did not end")
	require.Equal(t, want, r.GetSize(), "the size calculation failed or found other files")
}

func quietLogger() *slog.Logger {
	return slog.New(slog.DiscardHandler)
}
