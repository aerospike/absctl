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

package service

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/aerospike/absctl/api"
	"github.com/aerospike/absctl/internal/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestRestoreHandler(t *testing.T, srv *httptest.Server) *RestoreHandler {
	t.Helper()

	h, err := NewRestoreHandler(srv.URL)
	require.NoError(t, err)

	return h
}

func TestNewRestoreHandler(t *testing.T) {
	t.Parallel()

	h, err := NewRestoreHandler("http://localhost:8080")
	require.NoError(t, err)
	require.NotNil(t, h)
}

func TestRestoreHandler_Cancel(t *testing.T) {
	t.Parallel()

	srv, rec := newTestServer(t, http.StatusAccepted, "")
	h := newTestRestoreHandler(t, srv)

	err := h.Cancel(t.Context(), 42)
	require.NoError(t, err)

	assert.Equal(t, http.MethodPost, rec.method)
	assert.Equal(t, "/v1/restore/cancel/42", rec.path)
}

func TestRestoreHandler_Cancel_InvalidJobID(t *testing.T) {
	t.Parallel()

	srv, _ := newTestServer(t, http.StatusAccepted, "")
	h := newTestRestoreHandler(t, srv)

	err := h.Cancel(t.Context(), 0)
	require.Error(t, err)
	require.Contains(t, err.Error(), "job-id must be a positive integer")
}

func TestRestoreHandler_Cancel_NotFound(t *testing.T) {
	t.Parallel()

	srv, _ := newTestServer(t, http.StatusNotFound, "job not found")
	h := newTestRestoreHandler(t, srv)

	err := h.Cancel(t.Context(), 99)
	require.Error(t, err)
	require.Contains(t, err.Error(), "job not found")
}

func TestRestoreHandler_Status(t *testing.T) {
	t.Parallel()

	body := `{"status":"Running","read-records":100}`
	srv, rec := newTestServer(t, http.StatusOK, body)
	h := newTestRestoreHandler(t, srv)

	data, err := h.Status(t.Context(), 42)
	require.NoError(t, err)

	assert.Equal(t, http.MethodGet, rec.method)
	assert.Equal(t, "/v1/restore/status/42", rec.path)
	require.NotNil(t, data)
	require.NotNil(t, data.ReadRecords)
	assert.Equal(t, int64(100), *data.ReadRecords)
}

func TestRestoreHandler_Status_InvalidJobID(t *testing.T) {
	t.Parallel()

	srv, _ := newTestServer(t, http.StatusOK, "{}")
	h := newTestRestoreHandler(t, srv)

	_, err := h.Status(t.Context(), -1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "job-id must be a positive integer")
}

func TestRestoreHandler_Status_NotFound(t *testing.T) {
	t.Parallel()

	srv, _ := newTestServer(t, http.StatusNotFound, `"job missing"`)
	h := newTestRestoreHandler(t, srv)

	_, err := h.Status(t.Context(), 7)
	require.Error(t, err)
	require.Contains(t, err.Error(), "job missing")
}

func TestRestoreHandler_ListJobs_NoFilters(t *testing.T) {
	t.Parallel()

	srv, rec := newTestServer(t, http.StatusOK, `{}`)
	h := newTestRestoreHandler(t, srv)

	_, err := h.ListJobs(t.Context(), 0, 0, "")
	require.NoError(t, err)

	assert.Equal(t, http.MethodGet, rec.method)
	assert.Equal(t, "/v1/restore/jobs", rec.path)
	assert.Empty(t, rec.query, "no query params expected when no filters set")
}

func TestRestoreHandler_ListJobs_WithFilters(t *testing.T) {
	t.Parallel()

	srv, rec := newTestServer(t, http.StatusOK, `{}`)
	h := newTestRestoreHandler(t, srv)

	_, err := h.ListJobs(t.Context(), 100, 200, "Running,Done")
	require.NoError(t, err)

	assert.Equal(t, "/v1/restore/jobs", rec.path)
	assert.Equal(t, "100", rec.query.Get("from"))
	assert.Equal(t, "200", rec.query.Get("to"))
	assert.Equal(t, "Running,Done", rec.query.Get("status"))
}

func TestRestoreHandler_ListJobs_BadRequest(t *testing.T) {
	t.Parallel()

	srv, _ := newTestServer(t, http.StatusBadRequest, `"invalid status filter"`)
	h := newTestRestoreHandler(t, srv)

	_, err := h.ListJobs(t.Context(), 0, 0, "Bogus")
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid status filter")
}

func TestRestoreHandler_RestoreFull_FlagsOnly(t *testing.T) {
	t.Parallel()

	srv, rec := newTestServer(t, http.StatusAccepted, "123")
	h := newTestRestoreHandler(t, srv)

	req := &models.RestoreRequest{
		BackupDataPath:  "data/backup-1",
		DestinationName: "dest",
		SourceName:      "src",
		SecretAgentName: "agent",
	}

	jobID, err := h.RestoreFull(t.Context(), req)
	require.NoError(t, err)

	assert.Equal(t, http.MethodPost, rec.method)
	assert.Equal(t, "/v1/restore/full", rec.path)
	assert.Equal(t, "123", jobID)

	var got api.DtoRestoreRequest
	require.NoError(t, json.Unmarshal(rec.body, &got))
	assert.Equal(t, "data/backup-1", got.BackupDataPath)
	require.NotNil(t, got.DestinationName)
	assert.Equal(t, "dest", *got.DestinationName)
	require.NotNil(t, got.SourceName)
	assert.Equal(t, "src", *got.SourceName)
	require.NotNil(t, got.SecretAgentName)
	assert.Equal(t, "agent", *got.SecretAgentName)
}

func TestRestoreHandler_RestoreFull_RequestFile(t *testing.T) {
	t.Parallel()

	file := writeTempJSON(t, map[string]any{
		"backup-data-path": "data/from-file",
		"destination-name": "file-dest",
		"source-name":      "file-src",
	})

	srv, rec := newTestServer(t, http.StatusAccepted, "555")
	h := newTestRestoreHandler(t, srv)

	req := &models.RestoreRequest{RequestFile: file}

	jobID, err := h.RestoreFull(t.Context(), req)
	require.NoError(t, err)

	assert.Equal(t, "555", jobID)

	var got api.DtoRestoreRequest
	require.NoError(t, json.Unmarshal(rec.body, &got))
	assert.Equal(t, "data/from-file", got.BackupDataPath)
	require.NotNil(t, got.DestinationName)
	assert.Equal(t, "file-dest", *got.DestinationName)
	require.NotNil(t, got.SourceName)
	assert.Equal(t, "file-src", *got.SourceName)
}

func TestRestoreHandler_RestoreFull_RequestFileWithFlagOverrides(t *testing.T) {
	t.Parallel()

	file := writeTempJSON(t, map[string]any{
		"backup-data-path": "data/from-file",
		"destination-name": "file-dest",
		"source-name":      "file-src",
	})

	srv, rec := newTestServer(t, http.StatusAccepted, "1")
	h := newTestRestoreHandler(t, srv)

	req := &models.RestoreRequest{
		RequestFile:     file,
		BackupDataPath:  "data/from-flag",
		DestinationName: "flag-dest",
	}

	_, err := h.RestoreFull(t.Context(), req)
	require.NoError(t, err)

	var got api.DtoRestoreRequest
	require.NoError(t, json.Unmarshal(rec.body, &got))
	assert.Equal(t, "data/from-flag", got.BackupDataPath, "flag should override file")
	require.NotNil(t, got.DestinationName)
	assert.Equal(t, "flag-dest", *got.DestinationName)
	require.NotNil(t, got.SourceName)
	assert.Equal(t, "file-src", *got.SourceName, "file value preserved when no flag override")
}

func TestRestoreHandler_RestoreFull_ValidationError(t *testing.T) {
	t.Parallel()

	srv, _ := newTestServer(t, http.StatusAccepted, "1")
	h := newTestRestoreHandler(t, srv)

	_, err := h.RestoreFull(t.Context(), &models.RestoreRequest{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "either --request-file or --backup-data-path")
}

func TestRestoreHandler_RestoreFull_MissingBackupDataPath(t *testing.T) {
	t.Parallel()

	file := writeTempJSON(t, map[string]any{
		"destination-name": "dest",
	})

	srv, _ := newTestServer(t, http.StatusAccepted, "1")
	h := newTestRestoreHandler(t, srv)

	_, err := h.RestoreFull(t.Context(), &models.RestoreRequest{RequestFile: file})
	require.Error(t, err)
	require.Contains(t, err.Error(), "backup-data-path is required")
}

func TestRestoreHandler_RestoreFull_BadRequest(t *testing.T) {
	t.Parallel()

	srv, _ := newTestServer(t, http.StatusBadRequest, "invalid request")
	h := newTestRestoreHandler(t, srv)

	req := &models.RestoreRequest{BackupDataPath: "data/x"}

	_, err := h.RestoreFull(t.Context(), req)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid request")
}

func TestRestoreHandler_RestoreIncremental_FlagsOnly(t *testing.T) {
	t.Parallel()

	srv, rec := newTestServer(t, http.StatusAccepted, "999")
	h := newTestRestoreHandler(t, srv)

	req := &models.RestoreRequest{
		BackupDataPath:  "data/incr-1",
		DestinationName: "dest",
	}

	jobID, err := h.RestoreIncremental(t.Context(), req)
	require.NoError(t, err)

	assert.Equal(t, http.MethodPost, rec.method)
	assert.Equal(t, "/v1/restore/incremental", rec.path)
	assert.Equal(t, "999", jobID)

	var got api.DtoRestoreRequest
	require.NoError(t, json.Unmarshal(rec.body, &got))
	assert.Equal(t, "data/incr-1", got.BackupDataPath)
}

func TestRestoreHandler_RestoreIncremental_ValidationError(t *testing.T) {
	t.Parallel()

	srv, _ := newTestServer(t, http.StatusAccepted, "1")
	h := newTestRestoreHandler(t, srv)

	_, err := h.RestoreIncremental(t.Context(), &models.RestoreRequest{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "either --request-file or --backup-data-path")
}

func TestRestoreHandler_RestoreTimestamp_FlagsOnly(t *testing.T) {
	t.Parallel()

	srv, rec := newTestServer(t, http.StatusAccepted, "777")
	h := newTestRestoreHandler(t, srv)

	req := &models.RestoreTimestampRequest{
		Routine:           "daily",
		Time:              1700000000000,
		DestinationName:   "dest",
		SecretAgentName:   "agent",
		DisableReordering: true,
	}

	jobID, err := h.RestoreTimestamp(t.Context(), req)
	require.NoError(t, err)

	assert.Equal(t, http.MethodPost, rec.method)
	assert.Equal(t, "/v1/restore/timestamp", rec.path)
	assert.Equal(t, "777", jobID)

	var got api.DtoRestoreTimestampRequest
	require.NoError(t, json.Unmarshal(rec.body, &got))
	assert.Equal(t, "daily", got.Routine)
	assert.Equal(t, int64(1700000000000), got.Time)
	require.NotNil(t, got.DestinationName)
	assert.Equal(t, "dest", *got.DestinationName)
	require.NotNil(t, got.SecretAgentName)
	assert.Equal(t, "agent", *got.SecretAgentName)
	require.NotNil(t, got.DisableReordering)
	assert.True(t, *got.DisableReordering)
}

func TestRestoreHandler_RestoreTimestamp_RequestFile(t *testing.T) {
	t.Parallel()

	file := writeTempJSON(t, map[string]any{
		"routine":          "weekly",
		"time":             1234567890,
		"destination-name": "file-dest",
	})

	srv, rec := newTestServer(t, http.StatusAccepted, "1")
	h := newTestRestoreHandler(t, srv)

	_, err := h.RestoreTimestamp(t.Context(), &models.RestoreTimestampRequest{RequestFile: file})
	require.NoError(t, err)

	var got api.DtoRestoreTimestampRequest
	require.NoError(t, json.Unmarshal(rec.body, &got))
	assert.Equal(t, "weekly", got.Routine)
	assert.Equal(t, int64(1234567890), got.Time)
	require.NotNil(t, got.DestinationName)
	assert.Equal(t, "file-dest", *got.DestinationName)
}

func TestRestoreHandler_RestoreTimestamp_FlagsOverrideFile(t *testing.T) {
	t.Parallel()

	file := writeTempJSON(t, map[string]any{
		"routine": "weekly",
		"time":    1000,
	})

	srv, rec := newTestServer(t, http.StatusAccepted, "1")
	h := newTestRestoreHandler(t, srv)

	req := &models.RestoreTimestampRequest{
		RequestFile: file,
		Routine:     "daily",
		Time:        2000,
	}

	_, err := h.RestoreTimestamp(t.Context(), req)
	require.NoError(t, err)

	var got api.DtoRestoreTimestampRequest
	require.NoError(t, json.Unmarshal(rec.body, &got))
	assert.Equal(t, "daily", got.Routine)
	assert.Equal(t, int64(2000), got.Time)
}

func TestRestoreHandler_RestoreTimestamp_ValidationError(t *testing.T) {
	t.Parallel()

	srv, _ := newTestServer(t, http.StatusAccepted, "1")
	h := newTestRestoreHandler(t, srv)

	_, err := h.RestoreTimestamp(t.Context(), &models.RestoreTimestampRequest{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "--routine is required")
}

func TestRestoreHandler_RestoreTimestamp_FileMissingRoutine(t *testing.T) {
	t.Parallel()

	file := writeTempJSON(t, map[string]any{"time": 1000})

	srv, _ := newTestServer(t, http.StatusAccepted, "1")
	h := newTestRestoreHandler(t, srv)

	_, err := h.RestoreTimestamp(t.Context(), &models.RestoreTimestampRequest{RequestFile: file})
	require.Error(t, err)
	require.Contains(t, err.Error(), "routine is required")
}

func TestRestoreHandler_RestoreTimestamp_FileMissingTime(t *testing.T) {
	t.Parallel()

	file := writeTempJSON(t, map[string]any{"routine": "daily"})

	srv, _ := newTestServer(t, http.StatusAccepted, "1")
	h := newTestRestoreHandler(t, srv)

	_, err := h.RestoreTimestamp(t.Context(), &models.RestoreTimestampRequest{RequestFile: file})
	require.Error(t, err)
	require.Contains(t, err.Error(), "time is required")
}

func TestRestoreHandler_RequestFile_NotFound(t *testing.T) {
	t.Parallel()

	srv, _ := newTestServer(t, http.StatusAccepted, "1")
	h := newTestRestoreHandler(t, srv)

	req := &models.RestoreRequest{RequestFile: "/no/such/file.json"}

	_, err := h.RestoreFull(t.Context(), req)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to read request file")
}

func TestRestoreHandler_RequestFile_InvalidJSON(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "bad.json")
	require.NoError(t, os.WriteFile(path, []byte("{not valid"), 0o600))

	srv, _ := newTestServer(t, http.StatusAccepted, "1")
	h := newTestRestoreHandler(t, srv)

	req := &models.RestoreRequest{RequestFile: path}

	_, err := h.RestoreFull(t.Context(), req)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to parse request file")
}

func writeTempJSON(t *testing.T, v any) string {
	t.Helper()

	dir := t.TempDir()
	path := filepath.Join(dir, "request.json")

	data, err := json.Marshal(v)
	require.NoError(t, err)

	require.NoError(t, os.WriteFile(path, data, 0o600))

	return path
}
