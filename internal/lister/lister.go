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
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"

	"github.com/aerospike/absctl/internal/logging"
	"github.com/aerospike/absctl/internal/models"
	"github.com/aerospike/backup-go"
	bModels "github.com/aerospike/backup-go/models"
)

// metafileSSB is the name of the metadata file for Server Side Backup.
const metafileSSB = "metadata.json"

// Lister lists all backups in the given path.
type Lister struct {
	reader backup.StreamingReader
}

// NewLister creates a new backup Lister.
func NewLister(
	reader backup.StreamingReader,
) *Lister {
	return &Lister{
		reader: reader,
	}
}

// ListBackups lists all backups in the given path.
func (l *Lister) ListBackups(ctx context.Context, path string) error {
	allObjects, err := l.reader.ListObjects(ctx, path)
	if err != nil {
		return fmt.Errorf("failed to list objects: %w", err)
	}

	for _, object := range allObjects {
		if filepath.Base(object) == metafileSSB {
			mf, err := l.readMetafile(ctx, object)
			if err != nil {
				return fmt.Errorf("failed to read BackupEntry %s: %w", object, err)
			}

			logging.PrintMetadata(mf)
		}
	}

	return nil
}

// readMetafile reads the content of a BackupEntry.
func (l *Lister) readMetafile(ctx context.Context, path string) (models.Metadata, error) {
	file, err := l.openFile(ctx, path)
	if err != nil {
		return models.Metadata{}, fmt.Errorf("failed to open file %s: %w", path, err)
	}
	defer file.Reader.Close()

	content, err := io.ReadAll(file.Reader)
	if err != nil {
		return models.Metadata{}, fmt.Errorf("failed to read file %s: %w", path, err)
	}

	var b models.Metadata
	if err := json.Unmarshal(content, &b); err != nil {
		return models.Metadata{}, fmt.Errorf("failed to parse metadata: %w", err)
	}

	return b, nil
}

// openFile opens a file for reading.
func (l *Lister) openFile(ctx context.Context, path string) (bModels.File, error) {
	readersCh := make(chan bModels.File, 1)
	errorsCh := make(chan error, 1)

	defer close(readersCh)
	defer close(errorsCh)

	go l.reader.StreamFile(ctx, path, readersCh, errorsCh)

	select {
	case <-ctx.Done():
		return bModels.File{}, ctx.Err()
	case err := <-errorsCh:
		return bModels.File{}, err
	case file := <-readersCh:
		return file, nil
	}
}
