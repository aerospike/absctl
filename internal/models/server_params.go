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
	"strings"
	"unicode/utf8"
)

// Limits and rules the server applies to the parameters of the backup and restore info
// commands. Lengths are in bytes, as the server counts them.
const (
	maxServerNamespaceLen = 31
	// maxServerPathLen is the longest object key prefix, the trailing "/" the server
	// adds included: the prefix shares the 1024 byte S3 key limit with the key the
	// server generates under it.
	maxServerPathLen      = 512
	maxServerSetListLen   = 262_143
	maxServerSetNameLen   = 63
	maxServerBinListLen   = 1_048_575
	maxServerBinNameLen   = 15
	maxServerFilterExpLen = 16_383
	maxServerJobIDLen     = 20

	// serverInfoDelimiter separates the values of an info command and its reply, so no
	// value may contain it.
	serverInfoDelimiter = "|"
	// serverCmdUnsafeChars split an info command: ";" separates its parameters and a
	// newline ends it, so no value may contain either. backup-go refuses them too.
	serverCmdUnsafeChars = ";\n"
	// serverNameListSeparator separates the names of a set or bin list.
	serverNameListSeparator = ","
	// mrtMonitorSet is the set of the multi-record transaction monitor records, which a
	// backup must not name.
	mrtMonitorSet = "<ERO~MRT"
)

// validateServerText checks a value the server reads as printable ASCII text: at most
// maxLen bytes, without the info delimiter.
func validateServerText(name, value string, maxLen int) error {
	if err := validateServerLen(name, value, maxLen); err != nil {
		return err
	}

	for i := range len(value) {
		if value[i] < 0x20 || value[i] > 0x7e {
			return fmt.Errorf("%s must be printable ASCII", name)
		}
	}

	return nil
}

// validateServerUTF8 checks a value the server reads as UTF-8 text: at most maxLen
// bytes, valid UTF-8 with no C0, DEL or C1 control characters, without the info
// delimiter.
func validateServerUTF8(name, value string, maxLen int) error {
	if err := validateServerLen(name, value, maxLen); err != nil {
		return err
	}

	if !utf8.ValidString(value) {
		return fmt.Errorf("%s must be valid UTF-8 with no control characters", name)
	}

	for _, r := range value {
		if r < 0x20 || (r >= 0x7f && r <= 0x9f) {
			return fmt.Errorf("%s must be valid UTF-8 with no control characters", name)
		}
	}

	return nil
}

// validateServerLen checks the length of a value in bytes, and that it holds neither the
// info delimiter nor a character that would split the info command.
func validateServerLen(name, value string, maxLen int) error {
	if len(value) > maxLen {
		return fmt.Errorf("%s exceeds the maximum length of %d bytes", name, maxLen)
	}

	if strings.Contains(value, serverInfoDelimiter) {
		return fmt.Errorf("%s must not contain '%s'", name, serverInfoDelimiter)
	}

	if strings.ContainsAny(value, serverCmdUnsafeChars) {
		return fmt.Errorf("%s must not contain ';' or a newline", name)
	}

	return nil
}

// validateServerNameList checks a comma separated list of set or bin names, when it is
// set: the whole list as UTF-8 text of at most maxLen bytes, and every name between 1
// and maxNameLen bytes long and named once.
func validateServerNameList(name, list string, maxLen, maxNameLen int) error {
	if list == "" {
		return nil
	}

	if err := validateServerUTF8(name, list, maxLen); err != nil {
		return err
	}

	names := strings.Split(list, serverNameListSeparator)
	seen := make(map[string]struct{}, len(names))

	for _, n := range names {
		switch {
		case n == "":
			return fmt.Errorf("%s must not contain an empty entry", name)
		case len(n) > maxNameLen:
			return fmt.Errorf("%s entry %q exceeds the maximum length of %d bytes",
				name, n, maxNameLen)
		}

		if _, ok := seen[n]; ok {
			return fmt.Errorf("%s contains duplicate entry %q", name, n)
		}

		seen[n] = struct{}{}
	}

	return nil
}

// validateServerPath checks a key prefix the way the server does once it is normalized
// as it is sent: UTF-8 text without control characters and no longer than the S3 prefix
// limit, the trailing "/" the server adds included.
func validateServerPath(name, p string) error {
	if err := validatePath(p); err != nil {
		return err
	}

	prefix := NormalizeS3Prefix(p)
	if prefix == "" {
		return nil
	}

	// The server appends the "/" itself and counts it.
	return validateServerUTF8(name, prefix+"/", maxServerPathLen)
}

// validateServerJobID checks a job id the way the server does: 1 to 20 bytes of
// printable ASCII, without the characters that separate the fields of a status record
// or the segments of a storage path, and without spaces.
func validateServerJobID(name, id string) error {
	if id == "" {
		return fmt.Errorf("%s is required", name)
	}

	if err := validateServerText(name, id, maxServerJobIDLen); err != nil {
		return err
	}

	switch {
	case strings.ContainsAny(id, ":;="):
		return fmt.Errorf("%s must not contain ':', ';' or '='", name)
	case strings.Contains(id, "/") || strings.Contains(id, ".."):
		return fmt.Errorf("%s must not contain '/' or '..'", name)
	case strings.Contains(id, " "):
		return fmt.Errorf("%s must not contain a space", name)
	}

	return nil
}
