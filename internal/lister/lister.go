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

package lister

import (
	"context"
	"fmt"
	"io"
	"path/filepath"

	"github.com/aerospike/backup-go"
	"github.com/aerospike/backup-go/models"
)

const (
	// metafileABS is the name of the metadata file for ABS.
	metafileABS = "metadata.yaml"
	// metafileSSB is the name of the metadata file for Server Side Backup.
	metafileSSB = "manifest.yaml"
)

// metafileParser parses the content of a BackupEntry.
type metafileParser interface {
	Parse(string, []byte) (*BackupEntry, error)
}

// Lister lists all backups in the given path.
type Lister struct {
	reader backup.StreamingReader
	parser metafileParser
}

// NewLister creates a new backup Lister.
func NewLister(
	reader backup.StreamingReader,
	parser metafileParser,
) *Lister {
	return &Lister{
		reader: reader,
		parser: parser,
	}
}

// ListABS lists all backups in the given path.
func (l *Lister) ListABS(ctx context.Context, path string) ([]*BackupEntry, error) {
	return l.findMetafiles(ctx, path, metafileABS)
}

// ListSSB lists all backups in the given path.
func (l *Lister) ListSSB(ctx context.Context, path string) ([]*BackupEntry, error) {
	return l.findMetafiles(ctx, path, metafileSSB)
}

// findMetafiles finds all metafiles in the given path.
func (l *Lister) findMetafiles(ctx context.Context, path, filename string) ([]*BackupEntry, error) {
	allObjects, err := l.reader.ListObjects(ctx, path)
	if err != nil {
		return nil, fmt.Errorf("failed to list objects: %w", err)
	}

	metafiles := make([]*BackupEntry, 0)

	for _, object := range allObjects {
		if filepath.Base(object) == filename {
			mf, err := l.readMetafile(ctx, object)
			if err != nil {
				return nil, fmt.Errorf("failed to read BackupEntry %s: %w", object, err)
			}

			metafiles = append(metafiles, mf)
		}
	}

	return metafiles, nil
}

// readMetafile reads the content of a BackupEntry.
func (l *Lister) readMetafile(ctx context.Context, path string) (*BackupEntry, error) {
	file, err := l.openFile(ctx, path)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", path, err)
	}
	defer file.Reader.Close()

	content, err := io.ReadAll(file.Reader)
	if err != nil {
		return nil, fmt.Errorf("failed to read file %s: %w", path, err)
	}

	return l.parser.Parse(path, content)
}

// openFile opens a file for reading.
func (l *Lister) openFile(ctx context.Context, path string) (models.File, error) {
	readersCh := make(chan models.File, 1)
	errorsCh := make(chan error, 1)

	defer close(readersCh)
	defer close(errorsCh)

	go l.reader.StreamFile(ctx, path, readersCh, errorsCh)

	select {
	case <-ctx.Done():
		return models.File{}, ctx.Err()
	case err := <-errorsCh:
		return models.File{}, err
	case file := <-readersCh:
		return file, nil
	}
}
