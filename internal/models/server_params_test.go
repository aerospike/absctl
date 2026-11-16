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
	"math"
	"strings"
	"testing"
	"time"

	infomodels "github.com/aerospike/backup-go/pkg/asinfo/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testParamName          = "param"
	errMsgParamLen         = "param exceeds the maximum length of "
	errMsgParamUTF8        = "param must be valid UTF-8 with no control characters"
	errMsgParamASCII       = "param must be printable ASCII"
	errMsgParamDelim       = "param must not contain '|'"
	errMsgParamUnsafe      = "param must not contain ';' or a newline"
	errMsgParamSeparators  = "param must not contain ':', ';' or '='"
	errMsgParamSlash       = "param must not contain '/' or '..'"
	errMsgParamEmptyEntry  = "param must not contain an empty entry"
	errMsgNamespaceTooLong = "namespace exceeds the maximum length of 31"
	errMsgSetListEntry     = "set-list entry"
	errMsgRetryMultiplier  = "retry-multiplier must be at least 1.0"
)

func TestValidateServerText(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		give       string
		wantErrMsg string
	}{
		{name: "empty", give: ""},
		{name: "at the limit", give: strings.Repeat("a", 5)},
		{name: "too long", give: strings.Repeat("a", 6), wantErrMsg: errMsgParamLen + "5"},
		{name: "delimiter", give: "a|b", wantErrMsg: errMsgParamDelim},
		// A ";" or a newline would split the info command.
		{name: "semicolon", give: "a;b", wantErrMsg: errMsgParamUnsafe},
		{name: "newline", give: "a\nb", wantErrMsg: errMsgParamUnsafe},
		{name: "control character", give: "a\tb", wantErrMsg: errMsgParamASCII},
		{name: "non ASCII", give: "é", wantErrMsg: errMsgParamASCII},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := validateServerText(testParamName, tt.give, 5)
			if tt.wantErrMsg != "" {
				require.ErrorContains(t, err, tt.wantErrMsg)
				return
			}

			require.NoError(t, err)
		})
	}
}

func TestValidateServerUTF8(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		give       string
		wantErrMsg string
	}{
		{name: "non ASCII letters", give: "ключ"},
		// Lengths are in bytes: each of these letters takes two.
		{name: "too long in bytes", give: "ключик", wantErrMsg: errMsgParamLen + "10"},
		{name: "invalid UTF-8", give: "a\xffb", wantErrMsg: errMsgParamUTF8},
		{name: "C0 control", give: "a\tb", wantErrMsg: errMsgParamUTF8},
		{name: "semicolon", give: "a;b", wantErrMsg: errMsgParamUnsafe},
		{name: "DEL", give: "a\x7fb", wantErrMsg: errMsgParamUTF8},
		{name: "C1 control", give: "a\u0085b", wantErrMsg: errMsgParamUTF8},
		{name: "delimiter", give: "a|b", wantErrMsg: errMsgParamDelim},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := validateServerUTF8(testParamName, tt.give, 10)
			if tt.wantErrMsg != "" {
				require.ErrorContains(t, err, tt.wantErrMsg)
				return
			}

			require.NoError(t, err)
		})
	}
}

func TestValidateServerNameList(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		give       string
		wantErrMsg string
	}{
		{name: "unset", give: ""},
		{name: "several names", give: "set1,set2"},
		{name: "name at the limit", give: strings.Repeat("s", 4)},
		{name: "name too long", give: "set1," + strings.Repeat("s", 5), wantErrMsg: "exceeds the maximum length of 4"},
		{name: "empty entry", give: "set1,,set2", wantErrMsg: errMsgParamEmptyEntry},
		{name: "only a separator", give: ",", wantErrMsg: errMsgParamEmptyEntry},
		{name: "duplicate", give: "set1,set1", wantErrMsg: `param contains duplicate entry "set1"`},
		{name: "list too long", give: strings.Repeat("s,", 10) + "s", wantErrMsg: errMsgParamLen + "20"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := validateServerNameList(testParamName, tt.give, 20, 4)
			if tt.wantErrMsg != "" {
				require.ErrorContains(t, err, tt.wantErrMsg)
				return
			}

			require.NoError(t, err)
		})
	}
}

func TestValidateServerPath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		give       string
		wantErrMsg string
	}{
		{name: "unset", give: ""},
		{name: "several segments", give: "daily/incr"},
		// The server adds the trailing "/" and counts it.
		{name: "at the limit", give: strings.Repeat("p", maxServerPathLen-1)},
		{name: "too long", give: strings.Repeat("p", maxServerPathLen), wantErrMsg: "param exceeds the maximum length of 512"},
		// Normalized slashes do not count.
		{name: "slashes stripped before the limit", give: "/" + strings.Repeat("p", maxServerPathLen-1) + "/"},
		{name: "parent segment", give: "a/../b", wantErrMsg: "must not contain '..'"},
		{name: "control character", give: "a\tb", wantErrMsg: errMsgParamUTF8},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := validateServerPath(testParamName, tt.give)
			if tt.wantErrMsg != "" {
				require.ErrorContains(t, err, tt.wantErrMsg)
				return
			}

			require.NoError(t, err)
		})
	}
}

func TestServerCommon_ValidateServerParams(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		give       func(*ServerCommon)
		wantErrMsg string
	}{
		{name: "valid", give: func(*ServerCommon) {}},
		{
			name:       "namespace too long",
			give:       func(s *ServerCommon) { s.Namespace = strings.Repeat("n", maxServerNamespaceLen+1) },
			wantErrMsg: errMsgNamespaceTooLong,
		},
		{
			name:       "namespace with a semicolon",
			give:       func(s *ServerCommon) { s.Namespace = "test;no-config=true" },
			wantErrMsg: "namespace must not contain ';' or a newline",
		},
		{
			name:       "namespace not printable",
			give:       func(s *ServerCommon) { s.Namespace = "ns\x01" },
			wantErrMsg: "namespace must be printable ASCII",
		},
		{
			name:       "set name too long",
			give:       func(s *ServerCommon) { s.SetList = strings.Repeat("s", maxServerSetNameLen+1) },
			wantErrMsg: errMsgSetListEntry,
		},
		{
			name:       "set name with a semicolon",
			give:       func(s *ServerCommon) { s.SetList = "set1;set2" },
			wantErrMsg: "set-list must not contain ';' or a newline",
		},
		{
			name:       "duplicate set",
			give:       func(s *ServerCommon) { s.SetList = "set1,set1" },
			wantErrMsg: "set-list contains duplicate entry",
		},
		{
			name:       "filter expression with a carriage return",
			give:       func(s *ServerCommon) { s.FilterExp = "kwGTUQKkYmluMQE=\r" },
			wantErrMsg: "filter-exp must be printable ASCII",
		},
		{
			name:       "filter expression too long",
			give:       func(s *ServerCommon) { s.FilterExp = strings.Repeat("A", maxServerFilterExpLen+1) },
			wantErrMsg: "filter-exp exceeds the maximum length of 16383",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := &ServerCommon{Namespace: testServerNamespace}
			tt.give(s)

			err := s.Validate()
			if tt.wantErrMsg != "" {
				require.ErrorContains(t, err, tt.wantErrMsg)
				return
			}

			require.NoError(t, err)
		})
	}
}

func TestServerBackup_ValidateServerParams(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		give       func(*ServerBackup)
		wantErrMsg string
	}{
		{
			name:       "MRT monitor set",
			give:       func(s *ServerBackup) { s.SetList = "set1," + mrtMonitorSet },
			wantErrMsg: "set-list must not name the MRT monitor set",
		},
		{
			name: "bin names",
			give: func(s *ServerBackup) { s.BinList = "bin1,bin2" },
		},
		{
			name:       "bin name too long",
			give:       func(s *ServerBackup) { s.BinList = strings.Repeat("b", maxServerBinNameLen+1) },
			wantErrMsg: "bin-list entry",
		},
		{
			name:       "duplicate bin",
			give:       func(s *ServerBackup) { s.BinList = "bin1,bin1" },
			wantErrMsg: "bin-list contains duplicate entry",
		},
		{
			name:       "empty bin entry",
			give:       func(s *ServerBackup) { s.BinList = "bin1," },
			wantErrMsg: "bin-list must not contain an empty entry",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := validServerBackup()
			tt.give(s)

			err := s.Validate()
			if tt.wantErrMsg != "" {
				require.ErrorContains(t, err, tt.wantErrMsg)
				return
			}

			require.NoError(t, err)
		})
	}
}

func TestServerBackup_ValidateModifiedRange(t *testing.T) {
	t.Parallel()

	// now is fixed, so that "in the future" does not depend on the day the test runs.
	now := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name       string
		giveAfter  string
		giveBefore string
		wantErrMsg string
	}{
		{name: "unset"},
		{name: "valid range", giveAfter: "2026-01-01", giveBefore: "2026-02-01"},
		{name: "before in the future", giveBefore: "2030-01-01"},
		{name: "after too early", giveAfter: "2009-12-31", wantErrMsg: "modified-after must be after 2010-01-01"},
		{name: "before too early", giveBefore: "2009-12-31", wantErrMsg: "modified-before must be after 2010-01-01"},
		{name: "after in the future", giveAfter: "2026-07-01", wantErrMsg: "modified-after is in the future"},
		{name: "before too large", giveBefore: "2600-01-01", wantErrMsg: "modified-before must be at most 18446744073"},
		{
			name:       "empty range",
			giveAfter:  "2026-02-01",
			giveBefore: "2026-02-01",
			wantErrMsg: "modified-before must be strictly greater than modified-after",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := validServerBackup()
			s.ModifiedAfter = tt.giveAfter
			s.ModifiedBefore = tt.giveBefore

			err := s.validateModifiedRange(now)
			if tt.wantErrMsg != "" {
				require.ErrorContains(t, err, tt.wantErrMsg)
				return
			}

			require.NoError(t, err)
		})
	}
}

func TestValidateServerS3(t *testing.T) {
	t.Parallel()

	const (
		testS3Bucket = "bucket"
		testS3Region = "us-east-1"
	)

	tests := []struct {
		name       string
		give       *AwsS3
		wantErrMsg string
	}{
		{name: "not configured", wantErrMsg: "object storage is not configured"},
		{name: "minimal", give: &AwsS3{BucketName: testS3Bucket, Region: testS3Region}},
		{
			name: "every setting",
			give: &AwsS3{
				BucketName: testS3Bucket, Region: testS3Region, Profile: "default",
				Endpoint: "http://minio.local:9000",
			},
		},
		{name: "no bucket", give: &AwsS3{Region: testS3Region}, wantErrMsg: "s3-bucket-name is required"},
		{name: "no region", give: &AwsS3{BucketName: testS3Bucket}, wantErrMsg: "s3-region is required"},
		{
			name:       "region with a semicolon",
			give:       &AwsS3{BucketName: testS3Bucket, Region: "us-east-1;x"},
			wantErrMsg: "s3-region must not contain ';' or a newline",
		},
		{
			name: "bucket too long",
			give: &AwsS3{
				BucketName: strings.Repeat("b", maxServerS3BucketLen+1), Region: testS3Region,
			},
			wantErrMsg: "s3-bucket-name exceeds the maximum length of 255",
		},
		{
			name: "profile too long",
			give: &AwsS3{
				BucketName: testS3Bucket, Region: testS3Region,
				Profile: strings.Repeat("p", maxServerS3ProfileLen+1),
			},
			wantErrMsg: "s3-profile exceeds the maximum length of 63",
		},
		{
			name:       "endpoint looks unset",
			give:       &AwsS3{BucketName: testS3Bucket, Region: testS3Region, Endpoint: "None"},
			wantErrMsg: "looks like an unset value",
		},
		{
			name:       "endpoint without a scheme",
			give:       &AwsS3{BucketName: testS3Bucket, Region: testS3Region, Endpoint: "minio:9000"},
			wantErrMsg: "scheme must be http:// or https://",
		},
		{
			name:       "endpoint with an upper case scheme",
			give:       &AwsS3{BucketName: testS3Bucket, Region: testS3Region, Endpoint: "HTTPS://minio:9000"},
			wantErrMsg: "scheme must be lowercase",
		},
		{
			name:       "endpoint without a host",
			give:       &AwsS3{BucketName: testS3Bucket, Region: testS3Region, Endpoint: "https://"},
			wantErrMsg: "has no host",
		},
		{
			name:       "endpoint with whitespace",
			give:       &AwsS3{BucketName: testS3Bucket, Region: testS3Region, Endpoint: "https://a b"},
			wantErrMsg: "must not contain whitespace",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := ValidateServerS3(tt.give)
			if tt.wantErrMsg != "" {
				require.ErrorContains(t, err, tt.wantErrMsg)
				return
			}

			assert.NoError(t, err)
		})
	}
}

func TestValidateServerJobID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		give       string
		wantErrMsg string
	}{
		{name: "generated form", give: "261025T120000-ab12"},
		{name: "free form", give: "full-1104"},
		{name: "at the limit", give: strings.Repeat("j", maxServerJobIDLen)},
		{name: "empty", give: "", wantErrMsg: "param is required"},
		{name: "too long", give: strings.Repeat("j", maxServerJobIDLen+1), wantErrMsg: errMsgParamLen + "20"},
		{name: "not printable", give: "job\x01", wantErrMsg: errMsgParamASCII},
		{name: "delimiter", give: "a|b", wantErrMsg: errMsgParamDelim},
		{name: "colon", give: "a:b", wantErrMsg: errMsgParamSeparators},
		// A ";" is refused before the id rules, as it would split the info command.
		{name: "semicolon", give: "a;b", wantErrMsg: errMsgParamUnsafe},
		{name: "equals", give: "a=b", wantErrMsg: errMsgParamSeparators},
		{name: "slash", give: "a/b", wantErrMsg: errMsgParamSlash},
		{name: "parent", give: "a..b", wantErrMsg: errMsgParamSlash},
		{name: "space", give: "a b", wantErrMsg: "param must not contain a space"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := validateServerJobID(testParamName, tt.give)
			if tt.wantErrMsg != "" {
				require.ErrorContains(t, err, tt.wantErrMsg)
				return
			}

			require.NoError(t, err)
		})
	}
}

func TestServerRestore_ValidateServerParams(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		give       func(*ServerRestore)
		wantErrMsg string
	}{
		{
			name: "incremental path too long",
			give: func(s *ServerRestore) {
				s.BackupIDs = testServerBackupIDs
				s.IncrementalPath = strings.Repeat("p", maxServerPathLen)
			},
			wantErrMsg: "incremental-path exceeds the maximum length of 512",
		},
		{
			// An incremental path that normalizes to nothing is not sent, so the server
			// reads the incremental backups under the path anyway.
			name: "incremental path normalizing to the path root",
			give: func(s *ServerRestore) {
				s.BackupIDs = testServerBackupIDs
				s.IncrementalPath = "/"
			},
		},
		{
			// It is not sent either, so it is not a second backup id's path.
			name: "incremental path normalizing to nothing with a single backup",
			give: func(s *ServerRestore) { s.IncrementalPath = "./" },
		},
		{
			// The bucket root cannot be sent while path names a prefix.
			name: "incremental path at the bucket root under a path prefix",
			give: func(s *ServerRestore) {
				s.BackupIDs = testServerBackupIDs
				s.Path = testServerPath
				s.IncrementalPath = "/"
			},
			wantErrMsg: "names the bucket root",
		},
		{
			name:       "namespace too long",
			give:       func(s *ServerRestore) { s.Namespace = strings.Repeat("n", maxServerNamespaceLen+1) },
			wantErrMsg: errMsgNamespaceTooLong,
		},
		{
			name:       "set name too long",
			give:       func(s *ServerRestore) { s.SetList = strings.Repeat("s", maxServerSetNameLen+1) },
			wantErrMsg: errMsgSetListEntry,
		},
		{
			name: "retry multiplier NaN",
			give: func(s *ServerRestore) {
				s.Policy = string(infomodels.RestorePolicyFuzzy)
				s.RetryMultiplier = new(math.NaN())
			},
			wantErrMsg: errMsgRetryMultiplier,
		},
		{
			name: "retry multiplier infinite",
			give: func(s *ServerRestore) {
				s.Policy = string(infomodels.RestorePolicyFuzzy)
				s.RetryMultiplier = new(math.Inf(1))
			},
			wantErrMsg: errMsgRetryMultiplier,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := validServerRestore()
			tt.give(s)

			err := s.Validate()
			if tt.wantErrMsg != "" {
				require.ErrorContains(t, err, tt.wantErrMsg)
				return
			}

			require.NoError(t, err)
		})
	}
}

func TestValidateRestoreJob(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		giveNamespace string
		giveJobID     string
		wantErrMsg    string
	}{
		{name: "valid", giveNamespace: testServerNamespace, giveJobID: testServerRestoreJobID},
		{name: "no job id", giveNamespace: testServerNamespace, wantErrMsg: "job-id is required"},
		{name: "no namespace", giveJobID: testServerRestoreJobID, wantErrMsg: "namespace is required"},
		{
			name:          "job id with a space",
			giveNamespace: testServerNamespace,
			giveJobID:     "restore 1",
			wantErrMsg:    "job-id must not contain a space",
		},
		{
			name:          "namespace too long",
			giveNamespace: strings.Repeat("n", maxServerNamespaceLen+1),
			giveJobID:     testServerRestoreJobID,
			wantErrMsg:    "namespace exceeds the maximum length of 31",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := validateRestoreJob(tt.giveNamespace, tt.giveJobID)
			if tt.wantErrMsg != "" {
				require.ErrorContains(t, err, tt.wantErrMsg)
				return
			}

			require.NoError(t, err)
		})
	}
}
