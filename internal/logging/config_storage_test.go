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
	"testing"

	"github.com/aerospike/absctl/api"
	"github.com/stretchr/testify/assert"
)

func sampleS3Storage() api.DtoStorage {
	return api.DtoStorage{
		S3Storage: &api.DtoS3Storage{
			Bucket:   "my-bucket",
			S3Region: "us-east-1",
		},
	}
}

func sampleLocalStorage() api.DtoStorage {
	return api.DtoStorage{
		LocalStorage: &api.DtoLocalStorage{
			Path: "/var/backup",
		},
	}
}

func TestPrintStorageList_Console(t *testing.T) {
	storage := map[string]api.DtoStorage{
		"local": sampleLocalStorage(),
		"s3":    sampleS3Storage(),
	}

	output := captureOutput(t, func() {
		PrintStorageList(storage, false, nil)
	})

	assert.Contains(t, output, headerStorageAll)
	assert.Contains(t, output, "NAME")
	assert.Contains(t, output, "TYPE")
	assert.Contains(t, output, "local")
	assert.Contains(t, output, "s3")
	assert.Contains(t, output, "my-bucket")
	assert.Contains(t, output, "/var/backup")
}

func TestPrintStorageList_Console_Empty(t *testing.T) {
	output := captureOutput(t, func() {
		PrintStorageList(nil, false, nil)
	})

	assert.Contains(t, output, "No storage configured")
}

func TestPrintStorage_Console_Local(t *testing.T) {
	s := sampleLocalStorage()

	output := captureOutput(t, func() {
		PrintStorage("local", &s, false, nil)
	})

	assert.Contains(t, output, headerStorage)
	assert.Contains(t, output, "local")
	assert.Contains(t, output, "/var/backup")
}

func TestPrintStorage_Console_S3(t *testing.T) {
	s := sampleS3Storage()

	output := captureOutput(t, func() {
		PrintStorage("s3", &s, false, nil)
	})

	assert.Contains(t, output, "s3")
	assert.Contains(t, output, "my-bucket")
	assert.Contains(t, output, "us-east-1")
}

func TestPrintStorageList_JSON(t *testing.T) {
	storage := map[string]api.DtoStorage{"s3": sampleS3Storage()}

	out := captureLogJSON(t, func(logger *slog.Logger) {
		PrintStorageList(storage, true, logger)
	})

	assert.Contains(t, out, `"msg":"storage"`)
	assert.Contains(t, out, `"bucket":"my-bucket"`)
}
