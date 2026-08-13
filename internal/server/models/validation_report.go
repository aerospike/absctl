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

package models

// Report is the outcome of a backup validation run.
type Report struct {
	BackupID        string
	TotalSegments   int // total number of segments discovered in all manifests
	CheckedSegments int // number of segments actually validated
	// VerifiedByMetadata is the number of segments whose checksum was taken
	// from the S3 object metadata (no body download).
	VerifiedByMetadata int
	// VerifiedByDownload is the number of segments whose checksum had to be
	// computed by downloading the object body.
	VerifiedByDownload int
	Issues             []SegmentIssue
}

// SegmentIssue describes a single segment that failed validation, either
// because its checksum did not match or because it could not be validated.
type SegmentIssue struct {
	Namespace   string
	SegmentName string
	Expected    string
	Got         string
	Err         error // non-nil when the segment could not be validated at all
}
