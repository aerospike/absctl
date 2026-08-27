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

package logging

import (
	"fmt"
	"io"
	"os"
	"strings"
)

const (
	metricIndent = 30
)

// outWriter is a package-level reference to stderr that tests may override
// when capturing pretty-printed output is impractical via os.Pipe.
var outWriter io.Writer = os.Stderr

// SetOutWriter replaces the package-level output writer used by all
// pretty-printers. It returns the previous writer so callers (typically
// tests) can restore it. Production code does not call this function.
func SetOutWriter(w io.Writer) io.Writer {
	old := outWriter
	outWriter = w

	return old
}

// printToOutWriter prints the string to the configured output writer (stdout by default).
func printToOutWriter(s string) {
	_, _ = fmt.Fprintln(outWriter, s)
}

func printMetric(key string, value any) {
	_, _ = fmt.Fprintf(outWriter, "%s%v\n", indent(key), value)
}

func indent(key string) string {
	return fmt.Sprintf("%s:%s", key, strings.Repeat(" ", metricIndent-len(key)))
}
