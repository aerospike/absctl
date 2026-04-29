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
	"context"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/aerospike/absctl/api"
	"github.com/aerospike/absctl/internal/logging"
	"github.com/aerospike/absctl/internal/models"
)

// ConfigHandler handles configuration-related REST API calls under
// /v1/config and renders responses either as human-readable
// tables/sections or as structured slog entries depending on the JSON
// output flag.
type ConfigHandler struct {
	client *api.ClientWithResponses
	logger *slog.Logger
	json   bool
}

// NewConfigHandler creates a new ConfigHandler. The logger and json flag are
// forwarded to the logging package which selects the appropriate renderer.
func NewConfigHandler(serverURL string, logger *slog.Logger, json bool) (*ConfigHandler, error) {
	client, err := api.NewClientWithResponses(serverURL)
	if err != nil {
		return nil, fmt.Errorf("failed to create API client: %w", err)
	}

	return &ConfigHandler{
		client: client,
		logger: logger,
		json:   json,
	}, nil
}

// Read fetches the full service configuration and prints it.
func (h *ConfigHandler) Read(ctx context.Context) error {
	resp, err := h.client.ReadConfigWithResponse(ctx)
	if err != nil {
		return fmt.Errorf("read config request failed: %w", err)
	}

	if err := ensureStatus("read config", resp.StatusCode(), http.StatusOK, resp.Body); err != nil {
		return err
	}

	logging.PrintConfig(resp.JSON200, h.json, h.logger)

	return nil
}

// Update replaces the full service configuration with the body loaded from file.
func (h *ConfigHandler) Update(ctx context.Context, file string) error {
	if file == "" {
		return fmt.Errorf("--file is required")
	}

	var body api.UpdateConfigJSONRequestBody
	if err := loadJSONFile(file, &body); err != nil {
		return err
	}

	resp, err := h.client.UpdateConfigWithResponse(ctx, body)
	if err != nil {
		return fmt.Errorf("update config request failed: %w", err)
	}

	return ensureStatus("update config", resp.StatusCode(), http.StatusOK, resp.Body)
}

// Apply reloads the configuration from the configuration file on the server.
func (h *ConfigHandler) Apply(ctx context.Context) error {
	resp, err := h.client.ApplyConfigWithResponse(ctx)
	if err != nil {
		return fmt.Errorf("apply config request failed: %w", err)
	}

	return ensureStatus("apply config", resp.StatusCode(), http.StatusOK, resp.Body)
}

// Clusters

// ListClusters fetches all configured Aerospike clusters and prints them.
func (h *ConfigHandler) ListClusters(ctx context.Context) error {
	resp, err := h.client.ReadAllClustersWithResponse(ctx)
	if err != nil {
		return fmt.Errorf("list clusters request failed: %w", err)
	}

	if err := ensureStatus("list clusters", resp.StatusCode(), http.StatusOK, resp.Body); err != nil {
		return err
	}

	logging.PrintClusters(derefMap(resp.JSON200), h.json, h.logger)

	return nil
}

// ReadCluster fetches a single configured cluster by name and prints it.
func (h *ConfigHandler) ReadCluster(ctx context.Context, name string) error {
	if name == "" {
		return fmt.Errorf("--name is required")
	}

	resp, err := h.client.ReadClusterWithResponse(ctx, name)
	if err != nil {
		return fmt.Errorf("read cluster request failed: %w", err)
	}

	if err := ensureStatus("read cluster", resp.StatusCode(), http.StatusOK, resp.Body); err != nil {
		return err
	}

	logging.PrintCluster(name, resp.JSON200, h.json, h.logger)

	return nil
}

// AddCluster adds a new cluster definition built from individual flag fields
// (and optionally a JSON base file).
func (h *ConfigHandler) AddCluster(ctx context.Context, fields *models.ConfigClusterFields) error {
	if err := fields.Validate(); err != nil {
		return err
	}

	body, err := buildClusterBody(fields)
	if err != nil {
		return err
	}

	resp, err := h.client.AddClusterWithResponse(ctx, fields.Name, *body)
	if err != nil {
		return fmt.Errorf("add cluster request failed: %w", err)
	}

	return ensureStatus("add cluster", resp.StatusCode(), http.StatusCreated, resp.Body)
}

// UpdateCluster replaces an existing cluster definition with a body built from
// individual flag fields (and optionally a JSON base file).
func (h *ConfigHandler) UpdateCluster(ctx context.Context, fields *models.ConfigClusterFields) error {
	if err := fields.Validate(); err != nil {
		return err
	}

	body, err := buildClusterBody(fields)
	if err != nil {
		return err
	}

	resp, err := h.client.UpdateClusterWithResponse(ctx, fields.Name, *body)
	if err != nil {
		return fmt.Errorf("update cluster request failed: %w", err)
	}

	return ensureStatus("update cluster", resp.StatusCode(), http.StatusOK, resp.Body)
}

// DeleteCluster removes a cluster definition by name.
func (h *ConfigHandler) DeleteCluster(ctx context.Context, name string) error {
	if name == "" {
		return fmt.Errorf("--name is required")
	}

	resp, err := h.client.DeleteClusterWithResponse(ctx, name)
	if err != nil {
		return fmt.Errorf("delete cluster request failed: %w", err)
	}

	return ensureStatus("delete cluster", resp.StatusCode(), http.StatusNoContent, resp.Body)
}

// Policies

// ListPolicies fetches all configured backup policies and prints them.
func (h *ConfigHandler) ListPolicies(ctx context.Context) error {
	resp, err := h.client.ReadPoliciesWithResponse(ctx)
	if err != nil {
		return fmt.Errorf("list policies request failed: %w", err)
	}

	if err := ensureStatus("list policies", resp.StatusCode(), http.StatusOK, resp.Body); err != nil {
		return err
	}

	logging.PrintPolicies(derefMap(resp.JSON200), h.json, h.logger)

	return nil
}

// ReadPolicy fetches a single backup policy by name and prints it.
func (h *ConfigHandler) ReadPolicy(ctx context.Context, name string) error {
	if name == "" {
		return fmt.Errorf("--name is required")
	}

	resp, err := h.client.ReadPolicyWithResponse(ctx, name)
	if err != nil {
		return fmt.Errorf("read policy request failed: %w", err)
	}

	if err := ensureStatus("read policy", resp.StatusCode(), http.StatusOK, resp.Body); err != nil {
		return err
	}

	logging.PrintPolicy(name, resp.JSON200, h.json, h.logger)

	return nil
}

// AddPolicy adds a new backup policy built from individual flag fields.
func (h *ConfigHandler) AddPolicy(ctx context.Context, fields *models.ConfigPolicyFields) error {
	if err := fields.Validate(); err != nil {
		return err
	}

	body, err := buildPolicyBody(fields)
	if err != nil {
		return err
	}

	resp, err := h.client.AddPolicyWithResponse(ctx, fields.Name, *body)
	if err != nil {
		return fmt.Errorf("add policy request failed: %w", err)
	}

	return ensureStatus("add policy", resp.StatusCode(), http.StatusCreated, resp.Body)
}

// UpdatePolicy replaces an existing backup policy with a body built from
// individual flag fields.
func (h *ConfigHandler) UpdatePolicy(ctx context.Context, fields *models.ConfigPolicyFields) error {
	if err := fields.Validate(); err != nil {
		return err
	}

	body, err := buildPolicyBody(fields)
	if err != nil {
		return err
	}

	resp, err := h.client.UpdatePolicyWithResponse(ctx, fields.Name, *body)
	if err != nil {
		return fmt.Errorf("update policy request failed: %w", err)
	}

	return ensureStatus("update policy", resp.StatusCode(), http.StatusOK, resp.Body)
}

// DeletePolicy removes a backup policy by name.
func (h *ConfigHandler) DeletePolicy(ctx context.Context, name string) error {
	if name == "" {
		return fmt.Errorf("--name is required")
	}

	resp, err := h.client.DeletePolicyWithResponse(ctx, name)
	if err != nil {
		return fmt.Errorf("delete policy request failed: %w", err)
	}

	return ensureStatus("delete policy", resp.StatusCode(), http.StatusNoContent, resp.Body)
}

// Routines

// ListRoutines fetches all configured backup routines and prints them.
func (h *ConfigHandler) ListRoutines(ctx context.Context) error {
	resp, err := h.client.ReadRoutinesWithResponse(ctx)
	if err != nil {
		return fmt.Errorf("list routines request failed: %w", err)
	}

	if err := ensureStatus("list routines", resp.StatusCode(), http.StatusOK, resp.Body); err != nil {
		return err
	}

	logging.PrintRoutines(derefMap(resp.JSON200), h.json, h.logger)

	return nil
}

// ReadRoutine fetches a single backup routine by name and prints it.
func (h *ConfigHandler) ReadRoutine(ctx context.Context, name string) error {
	if name == "" {
		return fmt.Errorf("--name is required")
	}

	resp, err := h.client.ReadRoutineWithResponse(ctx, name)
	if err != nil {
		return fmt.Errorf("read routine request failed: %w", err)
	}

	if err := ensureStatus("read routine", resp.StatusCode(), http.StatusOK, resp.Body); err != nil {
		return err
	}

	logging.PrintRoutine(name, resp.JSON200, h.json, h.logger)

	return nil
}

// AddRoutine adds a new backup routine built from individual flag fields.
func (h *ConfigHandler) AddRoutine(ctx context.Context, fields *models.ConfigRoutineFields) error {
	if err := fields.Validate(); err != nil {
		return err
	}

	body, err := buildRoutineBody(fields)
	if err != nil {
		return err
	}

	resp, err := h.client.AddRoutineWithResponse(ctx, fields.Name, *body)
	if err != nil {
		return fmt.Errorf("add routine request failed: %w", err)
	}

	return ensureStatus("add routine", resp.StatusCode(), http.StatusCreated, resp.Body)
}

// UpdateRoutine replaces an existing backup routine with a body built from
// individual flag fields.
func (h *ConfigHandler) UpdateRoutine(ctx context.Context, fields *models.ConfigRoutineFields) error {
	if err := fields.Validate(); err != nil {
		return err
	}

	body, err := buildRoutineBody(fields)
	if err != nil {
		return err
	}

	resp, err := h.client.UpdateRoutineWithResponse(ctx, fields.Name, *body)
	if err != nil {
		return fmt.Errorf("update routine request failed: %w", err)
	}

	return ensureStatus("update routine", resp.StatusCode(), http.StatusOK, resp.Body)
}

// DeleteRoutine removes a backup routine by name.
func (h *ConfigHandler) DeleteRoutine(ctx context.Context, name string) error {
	if name == "" {
		return fmt.Errorf("--name is required")
	}

	resp, err := h.client.DeleteRoutineWithResponse(ctx, name)
	if err != nil {
		return fmt.Errorf("delete routine request failed: %w", err)
	}

	return ensureStatus("delete routine", resp.StatusCode(), http.StatusNoContent, resp.Body)
}

// EnableRoutine enables a backup routine by name.
func (h *ConfigHandler) EnableRoutine(ctx context.Context, name string) error {
	if name == "" {
		return fmt.Errorf("--name is required")
	}

	resp, err := h.client.EnableRoutineWithResponse(ctx, name)
	if err != nil {
		return fmt.Errorf("enable routine request failed: %w", err)
	}

	return ensureStatus("enable routine", resp.StatusCode(), http.StatusNoContent, resp.Body)
}

// DisableRoutine disables a backup routine by name.
func (h *ConfigHandler) DisableRoutine(ctx context.Context, name string) error {
	if name == "" {
		return fmt.Errorf("--name is required")
	}

	resp, err := h.client.DisableRoutineWithResponse(ctx, name)
	if err != nil {
		return fmt.Errorf("disable routine request failed: %w", err)
	}

	return ensureStatus("disable routine", resp.StatusCode(), http.StatusNoContent, resp.Body)
}

// Storage

// ListStorage fetches all configured storage entries and prints them.
func (h *ConfigHandler) ListStorage(ctx context.Context) error {
	resp, err := h.client.ReadAllStorageWithResponse(ctx)
	if err != nil {
		return fmt.Errorf("list storage request failed: %w", err)
	}

	if err := ensureStatus("list storage", resp.StatusCode(), http.StatusOK, resp.Body); err != nil {
		return err
	}

	logging.PrintStorageList(derefMap(resp.JSON200), h.json, h.logger)

	return nil
}

// ReadStorage fetches a single storage entry by name and prints it.
func (h *ConfigHandler) ReadStorage(ctx context.Context, name string) error {
	if name == "" {
		return fmt.Errorf("--name is required")
	}

	resp, err := h.client.ReadStorageWithResponse(ctx, name)
	if err != nil {
		return fmt.Errorf("read storage request failed: %w", err)
	}

	if err := ensureStatus("read storage", resp.StatusCode(), http.StatusOK, resp.Body); err != nil {
		return err
	}

	logging.PrintStorage(name, resp.JSON200, h.json, h.logger)

	return nil
}

// AddStorage adds a new storage entry built from individual flag fields.
func (h *ConfigHandler) AddStorage(ctx context.Context, fields *models.ConfigStorageFields) error {
	if err := fields.Validate(); err != nil {
		return err
	}

	body, err := buildStorageBody(fields)
	if err != nil {
		return err
	}

	resp, err := h.client.AddStorageWithResponse(ctx, fields.Name, *body)
	if err != nil {
		return fmt.Errorf("add storage request failed: %w", err)
	}

	return ensureStatus("add storage", resp.StatusCode(), http.StatusCreated, resp.Body)
}

// UpdateStorage replaces an existing storage entry with a body built from
// individual flag fields.
func (h *ConfigHandler) UpdateStorage(ctx context.Context, fields *models.ConfigStorageFields) error {
	if err := fields.Validate(); err != nil {
		return err
	}

	body, err := buildStorageBody(fields)
	if err != nil {
		return err
	}

	resp, err := h.client.UpdateStorageWithResponse(ctx, fields.Name, *body)
	if err != nil {
		return fmt.Errorf("update storage request failed: %w", err)
	}

	return ensureStatus("update storage", resp.StatusCode(), http.StatusOK, resp.Body)
}

// DeleteStorage removes a storage entry by name.
func (h *ConfigHandler) DeleteStorage(ctx context.Context, name string) error {
	if name == "" {
		return fmt.Errorf("--name is required")
	}

	resp, err := h.client.DeleteStorageWithResponse(ctx, name)
	if err != nil {
		return fmt.Errorf("delete storage request failed: %w", err)
	}

	return ensureStatus("delete storage", resp.StatusCode(), http.StatusNoContent, resp.Body)
}

// ensureStatus returns a descriptive error if the actual HTTP status differs from
// the expected one. The body is included in the error message when present.
func ensureStatus(op string, got, want int, body []byte) error {
	if got == want {
		return nil
	}

	return fmt.Errorf("%s failed: %s", op, extractErrorBody(body))
}

// derefMap safely dereferences a map pointer returned by the generated client,
// returning a nil map when the pointer itself is nil. The pointer parameter
// type is dictated by oapi-codegen: it always emits *map[...] for inline map
// response schemas, so we accept that shape and normalize for callers.
//
//nolint:gocritic // *map parameter type is required by the generated client signatures.
func derefMap[K comparable, V any](m *map[K]V) map[K]V {
	if m == nil {
		return nil
	}

	return *m
}

// derefSlice safely dereferences a slice pointer returned by the generated
// client, returning a nil slice when the pointer is nil. Same rationale as
// derefMap: oapi-codegen emits *[]T for inline slice response schemas.
func derefSlice[T any](s *[]T) []T {
	if s == nil {
		return nil
	}

	return *s
}
