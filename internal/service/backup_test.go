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
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// recordedRequest captures the salient parts of an inbound HTTP request.
type recordedRequest struct {
	method string
	path   string
	query  url.Values
	body   []byte
}

// newTestServer creates an httptest server that records every incoming request
// and responds with the given status code and body.
func newTestServer(t *testing.T, status int, body string) (*httptest.Server, *recordedRequest) {
	t.Helper()

	rec := &recordedRequest{}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec.method = r.Method
		rec.path = r.URL.Path
		rec.query = r.URL.Query()

		if r.Body != nil {
			rec.body, _ = io.ReadAll(r.Body)
			_ = r.Body.Close()
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)

		if body != "" {
			_, _ = w.Write([]byte(body))
		}
	}))

	t.Cleanup(srv.Close)

	return srv, rec
}

func newTestHandler(t *testing.T, srv *httptest.Server) *BackupHandler {
	t.Helper()

	h, err := NewBackupHandler(srv.URL)
	require.NoError(t, err)

	return h
}

func TestBackupHandler_Cancel(t *testing.T) {
	t.Parallel()

	srv, rec := newTestServer(t, http.StatusAccepted, "")
	h := newTestHandler(t, srv)

	err := h.Cancel(t.Context(), "daily-backup")
	require.NoError(t, err)

	assert.Equal(t, http.MethodPost, rec.method)
	assert.Equal(t, "/v1/backups/cancel/daily-backup", rec.path)
}

func TestBackupHandler_Cancel_EmptyName(t *testing.T) {
	t.Parallel()

	srv, _ := newTestServer(t, http.StatusAccepted, "")
	h := newTestHandler(t, srv)

	err := h.Cancel(t.Context(), "")
	require.Error(t, err)
	require.Contains(t, err.Error(), "routine name is required")
}

func TestBackupHandler_Cancel_NotFound(t *testing.T) {
	t.Parallel()

	srv, _ := newTestServer(t, http.StatusNotFound, "routine not found")
	h := newTestHandler(t, srv)

	err := h.Cancel(t.Context(), "missing")
	require.Error(t, err)
	require.Contains(t, err.Error(), "routine not found")
}

func TestBackupHandler_Status(t *testing.T) {
	t.Parallel()

	body := `{"full":{"done-records":100,"duration":42}}`
	srv, rec := newTestServer(t, http.StatusOK, body)
	h := newTestHandler(t, srv)

	data, err := h.Status(t.Context(), "daily-backup")
	require.NoError(t, err)

	assert.Equal(t, http.MethodGet, rec.method)
	assert.Equal(t, "/v1/backups/currentBackup/daily-backup", rec.path)
	require.NotNil(t, data)
	require.NotNil(t, data.Full)
	require.NotNil(t, data.Full.DoneRecords)
	assert.Equal(t, 100, *data.Full.DoneRecords)
}

func TestBackupHandler_Status_EmptyName(t *testing.T) {
	t.Parallel()

	srv, _ := newTestServer(t, http.StatusOK, "{}")
	h := newTestHandler(t, srv)

	_, err := h.Status(t.Context(), "")
	require.Error(t, err)
	require.Contains(t, err.Error(), "routine name is required")
}

func TestBackupHandler_Status_BadRequest(t *testing.T) {
	t.Parallel()

	srv, _ := newTestServer(t, http.StatusBadRequest, `"invalid input"`)
	h := newTestHandler(t, srv)

	_, err := h.Status(t.Context(), "bad")
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid input")
}

func TestBackupHandler_ListFull_AllRoutines(t *testing.T) {
	t.Parallel()

	srv, rec := newTestServer(t, http.StatusOK, `{"daily":[]}`)
	h := newTestHandler(t, srv)

	_, err := h.ListFull(t.Context(), "", 0, 0)
	require.NoError(t, err)

	assert.Equal(t, http.MethodGet, rec.method)
	assert.Equal(t, "/v1/backups/full", rec.path)
	assert.Empty(t, rec.query, "no query params expected when no filters set")
}

func TestBackupHandler_ListFull_AllRoutines_WithFilters(t *testing.T) {
	t.Parallel()

	srv, rec := newTestServer(t, http.StatusOK, `{}`)
	h := newTestHandler(t, srv)

	_, err := h.ListFull(t.Context(), "", 100, 200)
	require.NoError(t, err)

	assert.Equal(t, "/v1/backups/full", rec.path)
	assert.Equal(t, "100", rec.query.Get("from"))
	assert.Equal(t, "200", rec.query.Get("to"))
}

func TestBackupHandler_ListFull_ForRoutine(t *testing.T) {
	t.Parallel()

	srv, rec := newTestServer(t, http.StatusOK, `[]`)
	h := newTestHandler(t, srv)

	_, err := h.ListFull(t.Context(), "daily", 100, 0)
	require.NoError(t, err)

	assert.Equal(t, http.MethodGet, rec.method)
	assert.Equal(t, "/v1/backups/full/daily", rec.path)
	assert.Equal(t, "100", rec.query.Get("from"))
	assert.NotContains(t, rec.query, "to", "to should not be set when 0")
}

func TestBackupHandler_ListIncremental_AllRoutines(t *testing.T) {
	t.Parallel()

	srv, rec := newTestServer(t, http.StatusOK, `{}`)
	h := newTestHandler(t, srv)

	_, err := h.ListIncremental(t.Context(), "", 0, 0)
	require.NoError(t, err)

	assert.Equal(t, http.MethodGet, rec.method)
	assert.Equal(t, "/v1/backups/incremental", rec.path)
}

func TestBackupHandler_ListIncremental_ForRoutine(t *testing.T) {
	t.Parallel()

	srv, rec := newTestServer(t, http.StatusOK, `[]`)
	h := newTestHandler(t, srv)

	_, err := h.ListIncremental(t.Context(), "hourly", 0, 500)
	require.NoError(t, err)

	assert.Equal(t, "/v1/backups/incremental/hourly", rec.path)
	assert.Equal(t, "500", rec.query.Get("to"))
	assert.NotContains(t, rec.query, "from")
}

func TestBackupHandler_TriggerFull(t *testing.T) {
	t.Parallel()

	srv, rec := newTestServer(t, http.StatusAccepted, "")
	h := newTestHandler(t, srv)

	err := h.TriggerFull(t.Context(), "daily", 0)
	require.NoError(t, err)

	assert.Equal(t, http.MethodPost, rec.method)
	assert.Equal(t, "/v1/backups/full/daily", rec.path)
	assert.Empty(t, rec.query)
}

func TestBackupHandler_TriggerFull_WithDelay(t *testing.T) {
	t.Parallel()

	srv, rec := newTestServer(t, http.StatusAccepted, "")
	h := newTestHandler(t, srv)

	err := h.TriggerFull(t.Context(), "daily", 1500)
	require.NoError(t, err)

	assert.Equal(t, "1500", rec.query.Get("delay"))
}

func TestBackupHandler_TriggerFull_EmptyName(t *testing.T) {
	t.Parallel()

	srv, _ := newTestServer(t, http.StatusAccepted, "")
	h := newTestHandler(t, srv)

	err := h.TriggerFull(t.Context(), "", 0)
	require.Error(t, err)
	require.Contains(t, err.Error(), "routine name is required")
}

func TestBackupHandler_TriggerFull_NotFound(t *testing.T) {
	t.Parallel()

	srv, _ := newTestServer(t, http.StatusNotFound, "routine missing")
	h := newTestHandler(t, srv)

	err := h.TriggerFull(t.Context(), "missing", 0)
	require.Error(t, err)
	require.Contains(t, err.Error(), "routine missing")
}

func TestBackupHandler_TriggerIncremental(t *testing.T) {
	t.Parallel()

	srv, rec := newTestServer(t, http.StatusAccepted, "")
	h := newTestHandler(t, srv)

	err := h.TriggerIncremental(t.Context(), "hourly", 250)
	require.NoError(t, err)

	assert.Equal(t, http.MethodPost, rec.method)
	assert.Equal(t, "/v1/backups/incremental/hourly", rec.path)
	assert.Equal(t, "250", rec.query.Get("delay"))
}

func TestBackupHandler_TriggerIncremental_EmptyName(t *testing.T) {
	t.Parallel()

	srv, _ := newTestServer(t, http.StatusAccepted, "")
	h := newTestHandler(t, srv)

	err := h.TriggerIncremental(t.Context(), "", 0)
	require.Error(t, err)
	require.Contains(t, err.Error(), "routine name is required")
}

func TestNewBackupHandler(t *testing.T) {
	t.Parallel()

	h, err := NewBackupHandler("http://localhost:8080")
	require.NoError(t, err)
	require.NotNil(t, h)
}
