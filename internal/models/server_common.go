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

package models

import (
	"errors"
	"fmt"
	"math"
	"path"
	"slices"
	"strings"

	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/backup-go/pkg/asinfo"
)

// StorageTypeAwsS3 is the object storage type of the server-integrated backup and restore.
const StorageTypeAwsS3 = "aws-s3"

// backupIDForm describes the form of a backup id in error messages: the UTC start time
// and a suffix of 4 base36 characters (0-9, a-z), with an example.
const backupIDForm = "YYMMDDThhmmss-xxxx, where xxxx is 4 characters of 0-9 or a-z, " +
	"e.g. 261025T120000-ab12"

// ServerCommon contains flags that will be mapped to ServerBackup and ServerRestore.
type ServerCommon struct {
	// Namespace is the namespace to be backed up or restored.
	Namespace string
	// Path is the key prefix the backup is written under or read from.
	Path string
	// SetList is a comma-separated list of set names.
	SetList string
	// FilterExp is a base64-encoded filter expression. A restore accepts it with the
	// strict policy only.
	FilterExp string
	NoIndexes bool
	NoUDFs    bool
	// Policy is the consistency policy of the job. The values accepted by backup and
	// restore differ and are checked by each of them. Empty leaves it to the server default.
	Policy string
	// Async returns from the start command as soon as the cluster accepts the job,
	// instead of following the job until it ends.
	Async bool

	// The settings below are checked by backup and restore against their own limits.

	// ParallelRead is the number of read threads per node: the scan threads of a backup,
	// the threads that download the backup files of a restore.
	ParallelRead int
	// ParallelWrite is the number of write threads per node: the upload threads of a
	// backup, the threads that apply the records of a restore.
	ParallelWrite int
	// RecordsPerSecond limits the records per second each node reads (backup) or
	// applies (restore). Zero means no limit.
	RecordsPerSecond int
}

// Validate checks the settings that backup and restore share and check the same way.
func (s *ServerCommon) Validate() error {
	if s.Namespace == "" {
		return errors.New("namespace is required")
	}

	if err := validatePath(s.Path); err != nil {
		return err
	}

	if err := validateFilterExp(s.FilterExp); err != nil {
		return err
	}

	if s.RecordsPerSecond < 0 || int64(s.RecordsPerSecond) > math.MaxUint32 {
		return fmt.Errorf("records-per-second must be between 0 and %d, got %d",
			uint32(math.MaxUint32), s.RecordsPerSecond)
	}

	return nil
}

// validateFilterExp checks that exp, when set, is valid base64. Only the encoding can be
// checked locally: the cluster validates the expression itself once the job is submitted.
// The decoding error is not wrapped, as it carries client result codes that say nothing
// more to the user than the message below.
func validateFilterExp(exp string) error {
	if exp == "" {
		return nil
	}

	if _, err := aerospike.ExpFromBase64(exp); err != nil {
		return fmt.Errorf("invalid filter-exp: must be a base64-encoded filter expression")
	}

	return nil
}

// validatePath rejects a path with ".." segments. The path is an S3 key prefix, where
// ".." has no meaning, and resolving it locally would write the backup somewhere other
// than the user asked for.
func validatePath(p string) error {
	if slices.Contains(strings.Split(p, "/"), "..") {
		return fmt.Errorf("invalid path %q: must not contain '..'", p)
	}

	return nil
}

// NormalizeS3Prefix returns p as the S3 key prefix it names: without empty or "." segments
// and without leading or trailing slashes. The manifest reader drops a leading slash, so
// the backup has to be written under the same normalized prefix to be found afterwards.
func NormalizeS3Prefix(p string) string {
	return strings.Trim(path.Clean("/"+p), "/")
}

// validateBackupID checks that id, given with flag, has the form of a backup id. The
// manifest of a backup is looked up by that id alone, so any other value would fail only
// after the cluster and the object storage are reached.
func validateBackupID(flag, id string) error {
	if !asinfo.IsJobID(id) {
		return fmt.Errorf("%s %q is not a backup id, expected the form %s", flag, id, backupIDForm)
	}

	return nil
}
