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
	"fmt"
	"path"
	"slices"
	"strings"

	"github.com/aerospike/aerospike-client-go/v8"
)

// StorageTypeAwsS3 is the object storage type of the server-integrated backup and restore.
const StorageTypeAwsS3 = "aws-s3"

// supportedStorageTypes lists every value accepted by --object-storage-type.
// The cluster reports an unknown type only after the job has been submitted,
// so the value is checked locally to fail fast on a typo.
var supportedStorageTypes = []string{StorageTypeAwsS3}

// ServerCommon contains flags that will be mapped to ServerBackup and ServerRestore.
type ServerCommon struct {
	Namespace   string
	StorageType string
}

func (s *ServerCommon) Validate() error {
	if s.Namespace == "" {
		return fmt.Errorf("namespace is required")
	}

	if s.StorageType == "" {
		return fmt.Errorf("storage-type is required")
	}

	if !slices.Contains(supportedStorageTypes, s.StorageType) {
		return fmt.Errorf("unsupported storage-type %q, supported types: %s",
			s.StorageType, strings.Join(supportedStorageTypes, ", "))
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
