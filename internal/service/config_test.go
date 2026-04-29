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

func newTestConfigHandler(t *testing.T, srv *httptest.Server) *ConfigHandler {
	t.Helper()

	logger, _ := newSilentLogger()
	h, err := NewConfigHandler(srv.URL, logger, true)
	require.NoError(t, err)

	return h
}

func writeJSONFile(t *testing.T, v any) string {
	t.Helper()

	dir := t.TempDir()
	path := filepath.Join(dir, "body.json")

	data, err := json.Marshal(v)
	require.NoError(t, err)

	require.NoError(t, os.WriteFile(path, data, 0o600))

	return path
}

func TestNewConfigHandler(t *testing.T) {
	t.Parallel()

	logger, _ := newSilentLogger()
	h, err := NewConfigHandler("http://localhost:8080", logger, false)
	require.NoError(t, err)
	require.NotNil(t, h)
}

func TestConfigHandler_Read(t *testing.T) {
	t.Parallel()

	body := `{"service":{"http":{"address":"127.0.0.1"}}}`
	srv, rec := newTestServer(t, http.StatusOK, body)

	logger, buf := newSilentLogger()
	h, err := NewConfigHandler(srv.URL, logger, true)
	require.NoError(t, err)

	require.NoError(t, h.Read(t.Context()))

	assert.Equal(t, http.MethodGet, rec.method)
	assert.Equal(t, "/v1/config", rec.path)
	assert.Contains(t, buf.String(), `"address":"127.0.0.1"`)
}

func TestConfigHandler_Read_BadStatus(t *testing.T) {
	t.Parallel()

	srv, _ := newTestServer(t, http.StatusInternalServerError, "boom")
	h := newTestConfigHandler(t, srv)

	err := h.Read(t.Context())
	require.Error(t, err)
	require.Contains(t, err.Error(), "boom")
}

func TestConfigHandler_Update(t *testing.T) {
	t.Parallel()

	file := writeJSONFile(t, map[string]any{"service": map[string]any{"http": map[string]any{"address": "0.0.0.0"}}})

	srv, rec := newTestServer(t, http.StatusOK, "")
	h := newTestConfigHandler(t, srv)

	err := h.Update(t.Context(), file)
	require.NoError(t, err)

	assert.Equal(t, http.MethodPut, rec.method)
	assert.Equal(t, "/v1/config", rec.path)

	var got api.DtoConfig
	require.NoError(t, json.Unmarshal(rec.body, &got))
}

func TestConfigHandler_Update_MissingFile(t *testing.T) {
	t.Parallel()

	srv, _ := newTestServer(t, http.StatusOK, "")
	h := newTestConfigHandler(t, srv)

	err := h.Update(t.Context(), "")
	require.Error(t, err)
	require.Contains(t, err.Error(), "--file is required")
}

func TestConfigHandler_Update_FileNotFound(t *testing.T) {
	t.Parallel()

	srv, _ := newTestServer(t, http.StatusOK, "")
	h := newTestConfigHandler(t, srv)

	err := h.Update(t.Context(), "/no/such/file.json")
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to read request file")
}

func TestConfigHandler_Apply(t *testing.T) {
	t.Parallel()

	srv, rec := newTestServer(t, http.StatusOK, "")
	h := newTestConfigHandler(t, srv)

	err := h.Apply(t.Context())
	require.NoError(t, err)

	assert.Equal(t, http.MethodPost, rec.method)
	assert.Equal(t, "/v1/config/apply", rec.path)
}

func TestConfigHandler_Apply_BadRequest(t *testing.T) {
	t.Parallel()

	srv, _ := newTestServer(t, http.StatusBadRequest, "invalid")
	h := newTestConfigHandler(t, srv)

	err := h.Apply(t.Context())
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid")
}

// Resource cases — clusters, policies, routines, storage all share the same code
// path through generic handler methods. The table-driven cases below cover one
// representative request per HTTP verb for each resource.

func TestConfigHandler_Lists(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		path string
		fn   func(h *ConfigHandler) error
	}{
		{
			name: "clusters",
			path: "/v1/config/clusters",
			fn: func(h *ConfigHandler) error {
				return h.ListClusters(t.Context())
			},
		},
		{
			name: "policies",
			path: "/v1/config/policies",
			fn: func(h *ConfigHandler) error {
				return h.ListPolicies(t.Context())
			},
		},
		{
			name: "routines",
			path: "/v1/config/routines",
			fn: func(h *ConfigHandler) error {
				return h.ListRoutines(t.Context())
			},
		},
		{
			name: "storage",
			path: "/v1/config/storage",
			fn: func(h *ConfigHandler) error {
				return h.ListStorage(t.Context())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			srv, rec := newTestServer(t, http.StatusOK, "{}")
			h := newTestConfigHandler(t, srv)

			require.NoError(t, tt.fn(h))
			assert.Equal(t, http.MethodGet, rec.method)
			assert.Equal(t, tt.path, rec.path)
		})
	}
}

func TestConfigHandler_Reads(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		path string
		fn   func(h *ConfigHandler) error
	}{
		{
			name: "cluster",
			path: "/v1/config/clusters/primary",
			fn: func(h *ConfigHandler) error {
				return h.ReadCluster(t.Context(), "primary")
			},
		},
		{
			name: "policy",
			path: "/v1/config/policies/daily",
			fn: func(h *ConfigHandler) error {
				return h.ReadPolicy(t.Context(), "daily")
			},
		},
		{
			name: "routine",
			path: "/v1/config/routines/hourly",
			fn: func(h *ConfigHandler) error {
				return h.ReadRoutine(t.Context(), "hourly")
			},
		},
		{
			name: "storage",
			path: "/v1/config/storage/local",
			fn: func(h *ConfigHandler) error {
				return h.ReadStorage(t.Context(), "local")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			srv, rec := newTestServer(t, http.StatusOK, "{}")
			h := newTestConfigHandler(t, srv)

			require.NoError(t, tt.fn(h))
			assert.Equal(t, http.MethodGet, rec.method)
			assert.Equal(t, tt.path, rec.path)
		})
	}
}

func TestConfigHandler_Reads_EmptyName(t *testing.T) {
	t.Parallel()

	srv, _ := newTestServer(t, http.StatusOK, "{}")
	h := newTestConfigHandler(t, srv)

	err := h.ReadCluster(t.Context(), "")
	require.Error(t, err)
	require.Contains(t, err.Error(), "--name is required")
}

func TestConfigHandler_Reads_NotFound(t *testing.T) {
	t.Parallel()

	srv, _ := newTestServer(t, http.StatusNotFound, `"missing"`)
	h := newTestConfigHandler(t, srv)

	err := h.ReadPolicy(t.Context(), "missing")
	require.Error(t, err)
	require.Contains(t, err.Error(), "missing")
}

func TestConfigHandler_Adds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		path        string
		expectBytes []string
		fn          func(h *ConfigHandler) error
	}{
		{
			name:        "cluster",
			path:        "/v1/config/clusters/primary",
			expectBytes: []string{`"host-name":"127.0.0.1"`, `"port":3000`, `"label":"primary"`},
			fn: func(h *ConfigHandler) error {
				return h.AddCluster(t.Context(), &models.ConfigClusterFields{
					Name:      "primary",
					Label:     "primary",
					SeedNodes: []string{"127.0.0.1:3000"},
				})
			},
		},
		{
			name:        "policy",
			path:        "/v1/config/policies/daily",
			expectBytes: []string{`"parallel":4`},
			fn: func(h *ConfigHandler) error {
				return h.AddPolicy(t.Context(), &models.ConfigPolicyFields{
					Name:     "daily",
					Parallel: 4,
				})
			},
		},
		{
			name:        "routine",
			path:        "/v1/config/routines/hourly",
			expectBytes: []string{`"source-cluster":"primary"`, `"namespaces":["test"]`},
			fn: func(h *ConfigHandler) error {
				return h.AddRoutine(t.Context(), &models.ConfigRoutineFields{
					Name:          "hourly",
					SourceCluster: "primary",
					Storage:       "local",
					IntervalCron:  "0 * * * *",
					Namespaces:    []string{"test"},
				})
			},
		},
		{
			name:        "storage",
			path:        "/v1/config/storage/local",
			expectBytes: []string{`"local-storage":`, `"path":"/tmp/backup"`},
			fn: func(h *ConfigHandler) error {
				return h.AddStorage(t.Context(), &models.ConfigStorageFields{
					Name:      "local",
					LocalPath: "/tmp/backup",
				})
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			srv, rec := newTestServer(t, http.StatusCreated, "")
			h := newTestConfigHandler(t, srv)

			require.NoError(t, tt.fn(h))
			assert.Equal(t, http.MethodPost, rec.method)
			assert.Equal(t, tt.path, rec.path)

			require.NotEmpty(t, rec.body)

			var sent map[string]any
			require.NoError(t, json.Unmarshal(rec.body, &sent),
				"body should be valid JSON: %s", string(rec.body))

			for _, want := range tt.expectBytes {
				assert.Contains(t, string(rec.body), want)
			}
		})
	}
}

func TestConfigHandler_Adds_FlagsOverrideFile(t *testing.T) {
	t.Parallel()

	file := writeJSONFile(t, map[string]any{
		"label":      "loaded-from-file",
		"seed-nodes": []map[string]any{{"host-name": "10.0.0.1", "port": 3000}},
	})

	srv, rec := newTestServer(t, http.StatusCreated, "")
	h := newTestConfigHandler(t, srv)

	err := h.AddCluster(t.Context(), &models.ConfigClusterFields{
		Name:  "primary",
		File:  file,
		Label: "from-flag",
	})
	require.NoError(t, err)

	body := string(rec.body)
	assert.Contains(t, body, `"label":"from-flag"`)
	assert.Contains(t, body, `"host-name":"10.0.0.1"`,
		"unrelated fields from --file should remain")
}

func TestConfigHandler_Adds_ValidationErrors(t *testing.T) {
	t.Parallel()

	srv, _ := newTestServer(t, http.StatusCreated, "")
	h := newTestConfigHandler(t, srv)

	err := h.AddCluster(t.Context(), &models.ConfigClusterFields{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "--name is required")

	err = h.AddPolicy(t.Context(), &models.ConfigPolicyFields{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "--name is required")
}

func TestConfigHandler_Adds_BadStatus(t *testing.T) {
	t.Parallel()

	srv, _ := newTestServer(t, http.StatusBadRequest, "invalid cluster")
	h := newTestConfigHandler(t, srv)

	err := h.AddCluster(t.Context(), &models.ConfigClusterFields{
		Name:      "primary",
		SeedNodes: []string{"127.0.0.1:3000"},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid cluster")
}

func TestConfigHandler_Updates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		path string
		fn   func(h *ConfigHandler) error
	}{
		{
			name: "cluster",
			path: "/v1/config/clusters/primary",
			fn: func(h *ConfigHandler) error {
				return h.UpdateCluster(t.Context(), &models.ConfigClusterFields{
					Name:      "primary",
					SeedNodes: []string{"127.0.0.1:3000"},
				})
			},
		},
		{
			name: "policy",
			path: "/v1/config/policies/daily",
			fn: func(h *ConfigHandler) error {
				return h.UpdatePolicy(t.Context(), &models.ConfigPolicyFields{
					Name:     "daily",
					Parallel: 8,
				})
			},
		},
		{
			name: "routine",
			path: "/v1/config/routines/hourly",
			fn: func(h *ConfigHandler) error {
				return h.UpdateRoutine(t.Context(), &models.ConfigRoutineFields{
					Name:          "hourly",
					SourceCluster: "primary",
					Storage:       "local",
					IntervalCron:  "*/30 * * * *",
					Namespaces:    []string{"test"},
				})
			},
		},
		{
			name: "storage",
			path: "/v1/config/storage/local",
			fn: func(h *ConfigHandler) error {
				return h.UpdateStorage(t.Context(), &models.ConfigStorageFields{
					Name:      "local",
					LocalPath: "/tmp/backup",
				})
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			srv, rec := newTestServer(t, http.StatusOK, "")
			h := newTestConfigHandler(t, srv)

			require.NoError(t, tt.fn(h))
			assert.Equal(t, http.MethodPut, rec.method)
			assert.Equal(t, tt.path, rec.path)
		})
	}
}

func TestConfigHandler_Deletes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		path string
		fn   func(h *ConfigHandler) error
	}{
		{
			name: "cluster",
			path: "/v1/config/clusters/primary",
			fn: func(h *ConfigHandler) error {
				return h.DeleteCluster(t.Context(), "primary")
			},
		},
		{
			name: "policy",
			path: "/v1/config/policies/daily",
			fn: func(h *ConfigHandler) error {
				return h.DeletePolicy(t.Context(), "daily")
			},
		},
		{
			name: "routine",
			path: "/v1/config/routines/hourly",
			fn: func(h *ConfigHandler) error {
				return h.DeleteRoutine(t.Context(), "hourly")
			},
		},
		{
			name: "storage",
			path: "/v1/config/storage/local",
			fn: func(h *ConfigHandler) error {
				return h.DeleteStorage(t.Context(), "local")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			srv, rec := newTestServer(t, http.StatusNoContent, "")
			h := newTestConfigHandler(t, srv)

			require.NoError(t, tt.fn(h))
			assert.Equal(t, http.MethodDelete, rec.method)
			assert.Equal(t, tt.path, rec.path)
		})
	}
}

func TestConfigHandler_Deletes_EmptyName(t *testing.T) {
	t.Parallel()

	srv, _ := newTestServer(t, http.StatusNoContent, "")
	h := newTestConfigHandler(t, srv)

	err := h.DeleteCluster(t.Context(), "")
	require.Error(t, err)
	require.Contains(t, err.Error(), "--name is required")
}

func TestConfigHandler_EnableRoutine(t *testing.T) {
	t.Parallel()

	srv, rec := newTestServer(t, http.StatusNoContent, "")
	h := newTestConfigHandler(t, srv)

	require.NoError(t, h.EnableRoutine(t.Context(), "hourly"))

	assert.Equal(t, http.MethodPut, rec.method)
	assert.Equal(t, "/v1/config/routines/hourly/enable", rec.path)
}

func TestConfigHandler_DisableRoutine(t *testing.T) {
	t.Parallel()

	srv, rec := newTestServer(t, http.StatusNoContent, "")
	h := newTestConfigHandler(t, srv)

	require.NoError(t, h.DisableRoutine(t.Context(), "hourly"))

	assert.Equal(t, http.MethodPut, rec.method)
	assert.Equal(t, "/v1/config/routines/hourly/disable", rec.path)
}

func TestConfigHandler_EnableRoutine_EmptyName(t *testing.T) {
	t.Parallel()

	srv, _ := newTestServer(t, http.StatusNoContent, "")
	h := newTestConfigHandler(t, srv)

	err := h.EnableRoutine(t.Context(), "")
	require.Error(t, err)
	require.Contains(t, err.Error(), "--name is required")
}

func TestConfigHandler_EnableRoutine_NotFound(t *testing.T) {
	t.Parallel()

	srv, _ := newTestServer(t, http.StatusNotFound, "routine missing")
	h := newTestConfigHandler(t, srv)

	err := h.EnableRoutine(t.Context(), "missing")
	require.Error(t, err)
	require.Contains(t, err.Error(), "routine missing")
}
