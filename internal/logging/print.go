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
	"text/tabwriter"
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

// printSection writes a header followed by a dashed underline of the same length.
// Use it to delimit logical sections in non-JSON output.
func printSection(header string) {
	printToOutWriter("")
	printToOutWriter(header)
	printToOutWriter(strings.Repeat("-", len(header)))
}

// printSubSection prints a sub-header inside a section without a dashed
// underline; reserved for grouped key/value blocks.
func printSubSection(header string) {
	printToOutWriter("")
	printToOutWriter("::" + header + "::")
}

// newTabWriter returns a tabwriter writing to outWriter. The format is tuned
// for compact, left-aligned columns separated by two spaces.
func newTabWriter() *tabwriter.Writer {
	return tabwriter.NewWriter(outWriter, 0, 0, 2, ' ', 0)
}

// writeRow writes a single row into the tabwriter. Cells are joined with the
// tab separator that tabwriter uses for column alignment.
func writeRow(tw *tabwriter.Writer, cells ...string) {
	_, _ = fmt.Fprintln(tw, strings.Join(cells, "\t"))
}

// strVal safely dereferences a *string, returning an empty placeholder when nil.
func strVal(p *string) string {
	if p == nil {
		return "-"
	}

	return *p
}

// intVal safely dereferences a *int, returning 0 when nil.
func intVal(p *int) int {
	if p == nil {
		return 0
	}

	return *p
}

// int64Val safely dereferences a *int64, returning 0 when nil.
func int64Val(p *int64) int64 {
	if p == nil {
		return 0
	}

	return *p
}

// boolVal safely dereferences a *bool, returning false when nil.
func boolVal(p *bool) bool {
	if p == nil {
		return false
	}

	return *p
}

// printToOutWriter prints the string to the configured output writer (stdout by default).
func printToOutWriter(s string) {
	_, _ = fmt.Fprintln(outWriter, s)
}

func printMetric(key string, value any) {
	_, _ = fmt.Fprintf(outWriter, "%s%v\n", indent(key), value)
}

func indent(key string) string {
	return fmt.Sprintf("%s:%s", key, strings.Repeat(" ", 21-len(key)))
}
