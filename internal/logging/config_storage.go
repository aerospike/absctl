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
	"log/slog"

	"github.com/aerospike/absctl/api"
)

const (
	headerStorage    = "Storage"
	headerStorageAll = "Storage definitions"

	storageTypeLocal = "local"
	storageTypeS3    = "s3"
)

// PrintStorageList prints all configured storage entries keyed by name. When
// toLog is true the entire map is logged via slog; otherwise a tabular
// summary is rendered to stderr.
func PrintStorageList(storage map[string]api.DtoStorage, toLog bool, logger *slog.Logger) {
	if toLog {
		logger.Info("storage", slog.Any("storage", storage))
		return
	}

	printSection(headerStorageAll)
	printStorageTable(storage)
}

// PrintStorage prints a single storage definition. When toLog is true the
// DTO is logged via slog; otherwise key fields for the active provider are
// rendered as a section.
func PrintStorage(name string, storage *api.DtoStorage, toLog bool, logger *slog.Logger) {
	if toLog {
		logger.Info("storage",
			slog.String("name", name),
			slog.Any("storage", storage),
		)

		return
	}

	printStorage(name, storage)
}

func printStorageTable(storage map[string]api.DtoStorage) {
	if len(storage) == 0 {
		printToOutWriter("No storage configured.")
		return
	}

	tw := newTabWriter()
	writeRow(tw, "NAME", "TYPE", "LOCATION", "PATH")

	for _, name := range sortedKeys(storage) {
		s := storage[name]
		typ, location, path := storageSummary(&s)

		writeRow(tw, name, typ, location, path)
	}

	_ = tw.Flush()
}

func printStorage(name string, s *api.DtoStorage) {
	printSection(headerStorage)
	printMetric("Name", name)

	if s == nil {
		return
	}

	typ, location, path := storageSummary(s)
	printMetric("Type", typ)
	printMetric("Location", location)
	printMetric("Path", path)

	if s.S3Storage != nil {
		printMetric("S3 Region", s.S3Storage.S3Region)
		printMetric("S3 Endpoint", strVal(s.S3Storage.S3EndpointOverride))
		printMetric("S3 Profile", strVal(s.S3Storage.S3Profile))
	}

	if s.AzureStorage != nil {
		printMetric("Endpoint", s.AzureStorage.Endpoint)
		printMetric("Account Name", strVal(s.AzureStorage.AccountName))
	}

	if s.GcpStorage != nil {
		printMetric("Bucket", s.GcpStorage.BucketName)
	}
}

// storageSummary returns the provider type, location (bucket/container/-),
// and path for a storage entry.
func storageSummary(s *api.DtoStorage) (storageType, location, path string) {
	switch {
	case s.LocalStorage != nil:
		return storageTypeLocal, "-", s.LocalStorage.Path
	case s.S3Storage != nil:
		return storageTypeS3, s.S3Storage.Bucket, strVal(s.S3Storage.Path)
	case s.GcpStorage != nil:
		return "gcp", s.GcpStorage.BucketName, strVal(s.GcpStorage.Path)
	case s.AzureStorage != nil:
		return "azure", s.AzureStorage.ContainerName, strVal(s.AzureStorage.Path)
	default:
		return "-", "-", "-"
	}
}
