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
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"

	gcpStorage "cloud.google.com/go/storage"
	"github.com/aerospike/backup-go"
)

// Writer is a backup.Writer together with the storage client it writes through.
// backup-go closes the files it gets from the writer, but not the client: Close
// releases it. Call Close once the backup that uses the writer is over. Close is
// safe to call more than once.
type Writer interface {
	backup.Writer
	io.Closer
}

// Reader is a backup.StreamingReader together with the storage client it reads
// through. backup-go closes the files it gets from the reader, but not the client:
// Close releases it. Call Close once the job that uses the reader is over. Close is
// safe to call more than once.
type Reader interface {
	backup.StreamingReader
	io.Closer
}

// releaseFunc releases the storage client behind a reader or writer.
type releaseFunc func() error

// ownedWriter is a Writer that releases its storage client once.
type ownedWriter struct {
	backup.Writer

	release releaseFunc
}

// Close releases the storage client of the writer.
func (w *ownedWriter) Close() error {
	return w.release()
}

// ownedReader is a Reader that releases its storage client once.
type ownedReader struct {
	backup.StreamingReader

	release releaseFunc
}

// Close releases the storage client of the reader.
func (r *ownedReader) Close() error {
	return r.release()
}

// newOwnedWriter binds w to release, or releases at once and returns err when the
// writer could not be created.
func newOwnedWriter(w backup.Writer, err error, release releaseFunc) (Writer, error) {
	if err != nil {
		return nil, errors.Join(err, release())
	}

	return &ownedWriter{Writer: w, release: sync.OnceValue(release)}, nil
}

// newOwnedReader binds r to release, or releases at once and returns err when the
// reader could not be created.
func newOwnedReader(r backup.StreamingReader, err error, release releaseFunc) (Reader, error) {
	if err != nil {
		return nil, errors.Join(err, release())
	}

	return &ownedReader{StreamingReader: r, release: sync.OnceValue(release)}, nil
}

// noRelease is the releaseFunc of a reader or writer that holds no client,
// such as the local and standard stream ones.
func noRelease() error {
	return nil
}

// closeIdle returns a releaseFunc that closes the idle connections of transport.
// It suits the S3 and Azure clients, which have no Close of their own.
func closeIdle(transport *http.Transport) releaseFunc {
	return func() error {
		transport.CloseIdleConnections()

		return nil
	}
}

// closeGcp returns a releaseFunc that closes client and the idle connections of
// transport, the one client was built on.
func closeGcp(client *gcpStorage.Client, transport *http.Transport) releaseFunc {
	return func() error {
		transport.CloseIdleConnections()

		if err := client.Close(); err != nil {
			return fmt.Errorf("failed to close GCP client: %w", err)
		}

		return nil
	}
}
