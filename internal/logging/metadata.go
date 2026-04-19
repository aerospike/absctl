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
	"os"
	"strings"
	"text/tabwriter"

	"github.com/aerospike/absctl/internal/models"
)

// PrintMetadata displays the node data in a formatted table.
func PrintMetadata(data models.Metadata) {
	header := fmt.Sprintf("BACKUP ID: %s | NAMESPACE: %s | VERSION: %d",
		data.BackupID, data.Namespace, data.FormatVersion)
	line := strings.Repeat("-", len(header))

	fmt.Println()
	fmt.Println(header)
	fmt.Println(line)

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', tabwriter.Debug)

	// Print Table Header
	fmt.Fprintln(w, "NODE ID\tRECORDS\tBYTES\tPARTITIONS\tCREATED\tFINISHED")

	// Print Rows
	for _, node := range data.Nodes {
		fmt.Fprintf(w, "%s\t%d\t%d\t%d\t%s\t%s\n",
			node.NodeID,
			node.RecordCount,
			node.ByteCount,
			node.PartitionCount,
			node.Created.Format("2006-01-02 15:04:05"),
			node.Finished.Format("2006-01-02 15:04:05"),
		)
	}

	// Flush the writer to output the buffered table
	w.Flush()
}
