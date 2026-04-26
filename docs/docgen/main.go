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

// docgen generates README.md files for the backup and restore commands.
// Flag descriptions and default values are pulled from the flag definitions
// in internal/flags, and the YAML configuration schema is built from the
// DTO structs in internal/config/dto with comments derived from flag usage text.
//
// Usage:
//
//	go run ./cmd/docgen
//	make docs-generate
package main

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/aerospike/absctl/internal/config/dto"
	"github.com/aerospike/absctl/internal/flags"
	asFlags "github.com/aerospike/tools-common-go/flags"
	"github.com/spf13/pflag"
	"gopkg.in/yaml.v3"
)

var flagRefRe = regexp.MustCompile(`--([a-z])`)

const (
	// sectionSupportedFlags is used to split the existing README into a static header
	// (preserved) and dynamic content (regenerated).
	sectionSupportedFlags = "\n## Supported flags\n"
	// sectionConfigSchema is used to split the existing README into a static header.
	sectionConfigSchema = "\n## Configuration file schema with example values\n"

	// Path where to save docs.
	docPath = "./docs"

	opBackup  = "backup"
	opRestore = "restore"
)

// docSection binds a markdown section, its flag set, and its YAML mapping logic together.
type docSection struct {
	TextContent string         // Markdown text/header to print before flags
	FS          *pflag.FlagSet // Flags to print (if nil, just prints text)
	YAMLPrefix  string         // Base prefix for YAML paths
	// FlagToYAMLPath maps a flag to its full YAML dot-path.
	// If nil, defaults to YAMLPrefix + "." + flag.Name.
	// If it returns an empty string, the flag is skipped in YAML.
	FlagToYAMLPath func(prefix string, f *pflag.Flag) string
}

func main() {
	for _, op := range []string{opBackup, opRestore} {
		if err := generate(op); err != nil {
			log.Fatalf("failed to generate %s readme: %v", op, err)
		}
	}
}

// generate reads the existing README, preserves the static header, and regenerates all dynamic sections.
func generate(operation string) error {
	readmePath := filepath.Join(docPath, operation, "readme.md")

	existing, err := os.ReadFile(readmePath)
	if err != nil {
		return fmt.Errorf("read %s: %w", readmePath, err)
	}

	idx := strings.Index(string(existing), sectionSupportedFlags)
	if idx == -1 {
		return fmt.Errorf("marker %q not found in %s", sectionSupportedFlags, readmePath)
	}

	// Keep everything before the marker, plus its leading newline.
	staticHeader := string(existing[:idx+1])

	opID := flags.OperationBackup
	if operation == opRestore {
		opID = flags.OperationRestore
	}

	// Build shared context for this operation
	sections := buildSections(operation, opID)

	var sb strings.Builder
	sb.WriteString(staticHeader)
	sb.WriteString(sectionSupportedFlags)

	if operation == opBackup {
		sb.WriteString("```bash\n")
	} else {
		sb.WriteString("```\n")
	}

	sb.WriteString(generateFlagsContent(sections))
	sb.WriteString("```\n")

	// Unsupported flags (backup only)
	if operation == opBackup {
		sb.WriteString(unsupportedFlagsSection())
	}

	// YAML configuration schema
	yamlContent, err := generateYAMLContent(operation, sections)
	if err != nil {
		return fmt.Errorf("generate YAML: %w", err)
	}

	sb.WriteString(sectionConfigSchema)
	sb.WriteString("```yaml\n")
	sb.WriteString(yamlContent)
	sb.WriteString("```\n")

	return os.WriteFile(readmePath, []byte(sb.String()), 0o644)
}

// buildSections constructs all FlagSets and associates them with their markdown headers and YAML mappings.
func buildSections(operation string, opID flags.Operation) []docSection {
	var sections []docSection

	// 1. Usage Text
	usageText := flags.SectionTextUsageRestore
	if operation == opBackup {
		usageText = flags.SectionTextUsageBackup
	}
	sections = append(sections, docSection{TextContent: usageText})

	// 2. App Flags
	sections = append(sections, docSection{
		TextContent: flags.SectionTextGeneral,
		FS:          flags.NewApp().NewFlagSet(),
		YAMLPrefix:  "app",
		FlagToYAMLPath: func(prefix string, f *pflag.Flag) string {
			if f.Name == "config" || f.Name == "help" {
				return ""
			}
			return prefix + "." + f.Name
		},
	})

	// 3. Aerospike Client Flags
	aeroFlags := asFlags.NewDefaultAerospikeFlags()
	aeroFS := aeroFlags.NewFlagSet(asFlags.DefaultWrapHelpString)
	flags.WrapFlagsForSecrets(aeroFS)
	sections = append(sections, docSection{
		TextContent: flags.SectionTextAerospike,
		FS:          aeroFS,
		YAMLPrefix:  "cluster",
		FlagToYAMLPath: func(prefix string, f *pflag.Flag) string {
			if f.Name == "host" || f.Name == "port" {
				return "" // handled by cluster.seeds
			}
			if strings.HasPrefix(f.Name, "tls-") {
				return prefix + ".tls." + strings.TrimPrefix(f.Name, "tls-")
			}
			return prefix + "." + f.Name
		},
	})

	// 4. Client Policy Flags (Appended under Aerospike, no new text content)
	sections = append(sections, docSection{
		FS:         flags.NewClientPolicy().NewFlagSet(),
		YAMLPrefix: "cluster",
	})

	// 5. Common & Specific Operation Flags
	var commonFS, specificFS *pflag.FlagSet
	opText := flags.SectionTextRestore
	if operation == opBackup {
		opText = flags.SectionTextBackup
		b := flags.NewBackup()
		c := flags.NewCommon(&b.Common, flags.OperationBackup)
		commonFS, specificFS = c.NewFlagSet(), b.NewFlagSet()
	} else {
		r := flags.NewRestore()
		c := flags.NewCommon(&r.Common, flags.OperationRestore)
		commonFS, specificFS = c.NewFlagSet(), r.NewFlagSet()
	}

	if f := commonFS.Lookup("nice"); f != nil {
		f.Deprecated = "use --bandwidth instead"
		f.Hidden = false
	}

	sections = append(sections, docSection{
		TextContent: opText,
		FS:          commonFS,
		YAMLPrefix:  operation,
		FlagToYAMLPath: func(prefix string, f *pflag.Flag) string {
			if f.Name == "nice" {
				return ""
			}
			return prefix + "." + f.Name
		},
	})
	sections = append(sections, docSection{
		FS:         specificFS,
		YAMLPrefix: operation,
	})

	// 6. Compression Flags
	sections = append(sections, docSection{
		TextContent: flags.SectionTextCompression,
		FS:          flags.NewCompression(opID).NewFlagSet(),
		YAMLPrefix:  "compression",
		FlagToYAMLPath: func(prefix string, f *pflag.Flag) string {
			if f.Name == "compress" || f.Name == "compression-level" {
				return prefix + "." + strings.TrimPrefix(f.Name, "compression-")
			}
			return ""
		},
	})

	// 7. Encryption Flags
	sections = append(sections, docSection{
		TextContent: flags.SectionTextEncryption,
		FS:          flags.NewEncryption(opID).NewFlagSet(),
		YAMLPrefix:  "encryption",
		FlagToYAMLPath: func(prefix string, f *pflag.Flag) string {
			if f.Name == "encrypt" {
				return "encryption.encrypt"
			}
			return prefix + "." + strings.TrimPrefix(f.Name, "encryption-")
		},
	})

	// 8. Secret Agent Flags
	saText := flags.SectionTextSecretAgentRestore
	if operation == opBackup {
		saText = flags.SectionTextSecretAgentBackup
	}
	sections = append(sections, docSection{
		TextContent: saText,
		FS:          flags.NewSecretAgent().NewFlagSet(),
		YAMLPrefix:  "secret-agent",
		FlagToYAMLPath: func(prefix string, f *pflag.Flag) string {
			return prefix + "." + strings.TrimPrefix(f.Name, "sa-")
		},
	})

	// 9. Local Disk Flags (Backup only)
	if operation == opBackup {
		sections = append(sections, docSection{
			TextContent: flags.SectionTextLocal,
			FS:          flags.NewLocal(opID).NewFlagSet(),
			YAMLPrefix:  "local.disk",
			FlagToYAMLPath: func(prefix string, f *pflag.Flag) string {
				return prefix + "." + strings.TrimPrefix(f.Name, "local-")
			},
		})
	}

	// 10. AWS S3 Flags
	sections = append(sections, docSection{
		TextContent: flags.SectionTextAWS,
		FS:          flags.NewAwsS3(opID).NewFlagSet(),
		YAMLPrefix:  "aws.s3",
		FlagToYAMLPath: func(prefix string, f *pflag.Flag) string {
			return prefix + "." + strings.TrimPrefix(f.Name, "s3-")
		},
	})

	// 11. GCP Storage Flags
	sections = append(sections, docSection{
		TextContent: flags.SectionTextGCP,
		FS:          flags.NewGcpStorage(opID).NewFlagSet(),
		YAMLPrefix:  "gcp.storage",
		FlagToYAMLPath: func(prefix string, f *pflag.Flag) string {
			return prefix + "." + strings.TrimPrefix(f.Name, "gcp-")
		},
	})

	// 12. Azure Blob Flags
	sections = append(sections, docSection{
		TextContent: flags.SectionTextAzure,
		FS:          flags.NewAzureBlob(opID).NewFlagSet(),
		YAMLPrefix:  "azure.blob",
		FlagToYAMLPath: func(prefix string, f *pflag.Flag) string {
			return prefix + "." + strings.TrimPrefix(f.Name, "azure-")
		},
	})

	return sections
}

// generateFlagsContent generates the markdown flags section by iterating through all docSections.
func generateFlagsContent(sections []docSection) string {
	var sb strings.Builder

	for _, sec := range sections {
		if sec.TextContent != "" {
			sb.WriteString(sec.TextContent)
			sb.WriteString("\n")
		}
		if sec.FS != nil {
			sb.WriteString(fsStr(sec.FS))
		}
	}

	return sb.String()
}

func fsStr(fs *pflag.FlagSet) string {
	var buf bytes.Buffer
	fs.SetOutput(&buf)
	fs.PrintDefaults()
	return buf.String()
}

// unsupportedFlagsSection generates a string containing details about unsupported command-line flags.
func unsupportedFlagsSection() string {
	return "\n## Unsupported flags\n" +
		"```bash\n" +
		"--machine           Output machine-readable status updates to the given path, typically a FIFO.\n" +
		"\n" +
		"--no-config-file    Do not read any config file. Default: disabled\n" +
		"\n" +
		"--instance          Section with these instance is read. e.g in case instance `a` is specified\n" +
		"                    sections cluster_a, asbackup_a is read.\n" +
		"\n" +
		"--only-config-file  Read only this configuration file.\n" +
		"\n" +
		"--s3-max-async-downloads    The maximum number of simultaneous download requests from S3.\n" +
		"                            The default is 32.\n" +
		"\n" +
		"--s3-max-async-uploads      The maximum number of simultaneous upload requests from S3.\n" +
		"                            The default is 16.\n" +
		"\n" +
		"--s3-log-level              The log level of the AWS S3 C++ SDK. The possible levels are,\n" +
		"                            from least to most granular:\n" +
		"                             - Off\n" +
		"                             - Fatal\n" +
		"                             - Error\n" +
		"                             - Warn\n" +
		"                             - Info\n" +
		"                             - Debug\n" +
		"                             - Trace\n" +
		"                            The default is Fatal.\n" +
		"\n" +
		"--s3-connect-timeout        The AWS S3 client's connection timeout (in ms).\n" +
		"                            This is equivalent to cli-connect-timeout in the AWS CLI,\n" +
		"                            or connectTimeoutMS in the aws-sdk-cpp client configuration.\n" +
		"```\n"
}

// generateYAMLContent generates YAML content for a specified operation with annotated comments.
func generateYAMLContent(operation string, sections []docSection) (string, error) {
	var exampleDTO interface{}

	switch operation {
	case opBackup:
		exampleDTO = backupExampleDTO()
	case opRestore:
		exampleDTO = restoreExampleDTO()
	}

	var doc yaml.Node
	if err := doc.Encode(exampleDTO); err != nil {
		return "", fmt.Errorf("encode DTO: %w", err)
	}

	comments := buildCommentMap(sections)
	addComments(&doc, "", comments)

	// Clean out empty keys for specific cloud integrations
	cloudPrefixes := []string{"aws.s3", "gcp.storage", "azure.blob"}
	removeUncommentedLeaves(&doc, "", cloudPrefixes)

	var buf bytes.Buffer
	enc := yaml.NewEncoder(&buf)
	enc.SetIndent(2)

	if err := enc.Encode(&doc); err != nil {
		return "", fmt.Errorf("marshal YAML: %w", err)
	}
	enc.Close()

	return buf.String(), nil
}

// backupExampleDTO returns a Backup DTO populated with example values.
func backupExampleDTO() *dto.Backup {
	b := dto.DefaultBackup()

	dir := "backup_dir"
	ns := "source-ns1"
	b.Backup.Directory = &dir
	b.Backup.Namespace = &ns
	b.Backup.SetList = []string{"set1", "set2"}
	b.Backup.BinList = []string{"bin1", "bin2"}
	b.Backup.NodeList = []string{"127.0.0.1:3000", "127.0.0.1:3005"}
	b.Backup.PartitionList = []string{"0-1000"}
	b.Backup.PreferRacks = []string{"1"}
	b.Backup.RackList = []string{"1"}

	user := "db_user"
	pass := "db_password"
	b.Cluster.User = &user
	b.Cluster.Password = &pass

	idleTimeout := int64(60000)
	b.Cluster.ClientIdleTimeout = &idleTimeout

	tlsEnabled := true
	b.Cluster.TLS.Enable = &tlsEnabled

	return b
}

// restoreExampleDTO returns a Restore DTO populated with example values.
func restoreExampleDTO() *dto.Restore {
	r := dto.DefaultRestore()

	dir := "backup_dir"
	ns := "source-ns1"
	r.Restore.Directory = &dir
	r.Restore.Namespace = &ns
	r.Restore.SetList = []string{"set1", "set2"}
	r.Restore.BinList = []string{"bin1", "bin2"}
	r.Restore.DirectoryList = []string{"dir1", "dir2"}

	par := 1
	r.Restore.Parallel = &par

	user := "db_user"
	pass := "db_password"
	r.Cluster.User = &user
	r.Cluster.Password = &pass

	idleTimeout := int64(60000)
	r.Cluster.ClientIdleTimeout = &idleTimeout

	tlsEnabled := true
	r.Cluster.TLS.Enable = &tlsEnabled

	return r
}

// buildCommentMap iterates through docSections and applies formatting rules to map flags to YAML keys.
func buildCommentMap(sections []docSection) map[string]string {
	comments := make(map[string]string)

	for _, sec := range sections {
		if sec.FS == nil {
			continue // skip text-only sections
		}

		sec.FS.VisitAll(func(f *pflag.Flag) {
			var yamlPath string

			if sec.FlagToYAMLPath != nil {
				yamlPath = sec.FlagToYAMLPath(sec.YAMLPrefix, f)
			} else {
				yamlPath = sec.YAMLPrefix + "." + f.Name
			}

			if yamlPath != "" {
				comments[yamlPath] = toYAMLComment(f.Usage)
			}
		})
	}

	return comments
}

// addComments walks a yaml.Node tree and sets HeadComment on key nodes.
func addComments(node *yaml.Node, path string, comments map[string]string) {
	switch node.Kind {
	case yaml.DocumentNode:
		for _, child := range node.Content {
			addComments(child, path, comments)
		}
	case yaml.MappingNode:
		for i := 0; i < len(node.Content)-1; i += 2 {
			keyNode := node.Content[i]
			valNode := node.Content[i+1]

			var fieldPath string
			if path == "" {
				fieldPath = keyNode.Value
			} else {
				fieldPath = path + "." + keyNode.Value
			}

			if comment, ok := comments[fieldPath]; ok {
				keyNode.HeadComment = comment
			}

			if valNode.Kind == yaml.MappingNode || valNode.Kind == yaml.SequenceNode {
				addComments(valNode, fieldPath, comments)
			}
		}
	case yaml.SequenceNode:
		for _, child := range node.Content {
			if child.Kind == yaml.MappingNode {
				addComments(child, path, comments)
			}
		}
	}
}

// removeUncommentedLeaves walks the YAML tree and removes scalar fields that
// lack a head comment within targeted prefix sections (e.g. cloud storage).
func removeUncommentedLeaves(node *yaml.Node, path string, targetPrefixes []string) {
	if node.Kind == yaml.DocumentNode {
		for _, child := range node.Content {
			removeUncommentedLeaves(child, path, targetPrefixes)
		}
		return
	}

	if node.Kind != yaml.MappingNode {
		return
	}

	isFiltered := false
	for _, target := range targetPrefixes {
		if path == target {
			isFiltered = true
			break
		}
	}

	var kept []*yaml.Node

	for i := 0; i < len(node.Content)-1; i += 2 {
		keyNode := node.Content[i]
		valNode := node.Content[i+1]

		var fieldPath string
		if path == "" {
			fieldPath = keyNode.Value
		} else {
			fieldPath = path + "." + keyNode.Value
		}

		if isFiltered && keyNode.HeadComment == "" && valNode.Kind == yaml.ScalarNode {
			continue
		}

		kept = append(kept, keyNode, valNode)

		if valNode.Kind == yaml.MappingNode || valNode.Kind == yaml.SequenceNode {
			removeUncommentedLeaves(valNode, fieldPath, targetPrefixes)
		}
	}

	node.Content = kept
}

// toYAMLComment converts a flag usage string to a YAML comment body.
// It strips -- prefixes from flag references and cleans up whitespace.
func toYAMLComment(usage string) string {
	result := flagRefRe.ReplaceAllString(usage, "$1")
	lines := strings.Split(result, "\n")

	var cleaned []string
	for _, line := range lines {
		line = strings.TrimRight(line, " \t")
		if line != "" {
			cleaned = append(cleaned, line)
		}
	}

	return strings.Join(cleaned, "\n")
}
