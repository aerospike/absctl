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
	"encoding/json"
	"fmt"
	"net/http"
	"os"

	"github.com/aerospike/absctl/api"
	"github.com/aerospike/absctl/internal/models"
)

// RestoreHandler handles restore-related REST API calls.
type RestoreHandler struct {
	client *api.ClientWithResponses
}

// NewRestoreHandler creates a new RestoreHandler for the given server URL.
func NewRestoreHandler(serverURL string) (*RestoreHandler, error) {
	client, err := api.NewClientWithResponses(serverURL)
	if err != nil {
		return nil, fmt.Errorf("failed to create API client: %w", err)
	}

	return &RestoreHandler{client: client}, nil
}

// Cancel cancels a running restore operation by job ID.
func (h *RestoreHandler) Cancel(ctx context.Context, jobID int64) error {
	if jobID <= 0 {
		return fmt.Errorf("job-id must be a positive integer")
	}

	resp, err := h.client.CancelRestoreWithResponse(ctx, jobID)
	if err != nil {
		return fmt.Errorf("cancel restore request failed: %w", err)
	}

	if resp.StatusCode() != http.StatusAccepted {
		return fmt.Errorf("cancel restore failed: %s", extractErrorBody(resp.Body))
	}

	return nil
}

// Status returns the status of a restore job by ID.
func (h *RestoreHandler) Status(ctx context.Context, jobID int64) (*api.DtoRestoreJobStatus, error) {
	if jobID <= 0 {
		return nil, fmt.Errorf("job-id must be a positive integer")
	}

	resp, err := h.client.RestoreStatusWithResponse(ctx, jobID)
	if err != nil {
		return nil, fmt.Errorf("get restore status request failed: %w", err)
	}

	if resp.StatusCode() != http.StatusOK {
		return nil, fmt.Errorf("get restore status failed: %s", extractErrorBody(resp.Body))
	}

	return resp.JSON200, nil
}

// ListJobs returns restore jobs, optionally filtered by time range and status.
func (h *RestoreHandler) ListJobs(
	ctx context.Context, from, to int64, status string,
) (map[string]api.DtoRestoreJobStatus, error) {
	params := &api.RetrieveRestoreJobsParams{}

	if from > 0 {
		params.From = &from
	}

	if to > 0 {
		params.To = &to
	}

	if status != "" {
		params.Status = &status
	}

	resp, err := h.client.RetrieveRestoreJobsWithResponse(ctx, params)
	if err != nil {
		return nil, fmt.Errorf("list restore jobs request failed: %w", err)
	}

	if resp.StatusCode() != http.StatusOK {
		return nil, fmt.Errorf("list restore jobs failed: %s", extractErrorBody(resp.Body))
	}

	return *resp.JSON200, nil
}

// RestoreFull triggers an asynchronous full restore operation. Returns the new job ID
// (the API responds with a plain integer body) on success.
func (h *RestoreHandler) RestoreFull(ctx context.Context, req *models.RestoreRequest) (string, error) {
	if err := req.Validate(); err != nil {
		return "", err
	}

	body, err := buildRestoreRequest(req)
	if err != nil {
		return "", err
	}

	resp, err := h.client.RestoreFullWithResponse(ctx, *body)
	if err != nil {
		return "", fmt.Errorf("restore full request failed: %w", err)
	}

	if resp.StatusCode() != http.StatusAccepted {
		return "", fmt.Errorf("restore full failed: %s", extractErrorBody(resp.Body))
	}

	return string(resp.Body), nil
}

// RestoreIncremental triggers an asynchronous incremental restore operation. Returns the
// new job ID on success.
func (h *RestoreHandler) RestoreIncremental(ctx context.Context, req *models.RestoreRequest) (string, error) {
	if err := req.Validate(); err != nil {
		return "", err
	}

	body, err := buildRestoreRequest(req)
	if err != nil {
		return "", err
	}

	resp, err := h.client.RestoreIncrementalWithResponse(ctx, *body)
	if err != nil {
		return "", fmt.Errorf("restore incremental request failed: %w", err)
	}

	if resp.StatusCode() != http.StatusAccepted {
		return "", fmt.Errorf("restore incremental failed: %s", extractErrorBody(resp.Body))
	}

	return string(resp.Body), nil
}

// RestoreTimestamp triggers an asynchronous restore-to-timestamp operation. Returns the
// new job ID on success.
func (h *RestoreHandler) RestoreTimestamp(ctx context.Context, req *models.RestoreTimestampRequest) (string, error) {
	if err := req.Validate(); err != nil {
		return "", err
	}

	body, err := buildRestoreTimestampRequest(req)
	if err != nil {
		return "", err
	}

	resp, err := h.client.RestoreTimestampWithResponse(ctx, *body)
	if err != nil {
		return "", fmt.Errorf("restore timestamp request failed: %w", err)
	}

	if resp.StatusCode() != http.StatusAccepted {
		return "", fmt.Errorf("restore timestamp failed: %s", extractErrorBody(resp.Body))
	}

	return string(resp.Body), nil
}

// buildRestoreRequest constructs an api.DtoRestoreRequest from model inputs.
// If req.RequestFile is set, the file is parsed first; non-empty flag values then override
// the corresponding fields.
func buildRestoreRequest(req *models.RestoreRequest) (*api.DtoRestoreRequest, error) {
	body := &api.DtoRestoreRequest{}

	if req.RequestFile != "" {
		if err := loadJSONFile(req.RequestFile, body); err != nil {
			return nil, err
		}
	}

	if req.BackupDataPath != "" {
		body.BackupDataPath = req.BackupDataPath
	}

	if req.DestinationName != "" {
		name := req.DestinationName
		body.DestinationName = &name
		body.Destination = nil
	}

	if req.SourceName != "" {
		name := req.SourceName
		body.SourceName = &name
		body.Source = nil
	}

	if req.SecretAgentName != "" {
		name := req.SecretAgentName
		body.SecretAgentName = &name
		body.SecretAgent = nil
	}

	if body.BackupDataPath == "" {
		return nil, fmt.Errorf("backup-data-path is required (set via --backup-data-path or in --request-file)")
	}

	return body, nil
}

// buildRestoreTimestampRequest constructs an api.DtoRestoreTimestampRequest from model inputs.
func buildRestoreTimestampRequest(req *models.RestoreTimestampRequest) (*api.DtoRestoreTimestampRequest, error) {
	body := &api.DtoRestoreTimestampRequest{}

	if req.RequestFile != "" {
		if err := loadJSONFile(req.RequestFile, body); err != nil {
			return nil, err
		}
	}

	if req.Routine != "" {
		body.Routine = req.Routine
	}

	if req.Time > 0 {
		body.Time = req.Time
	}

	if req.DestinationName != "" {
		name := req.DestinationName
		body.DestinationName = &name
		body.Destination = nil
	}

	if req.SecretAgentName != "" {
		name := req.SecretAgentName
		body.SecretAgentName = &name
		body.SecretAgent = nil
	}

	if req.DisableReordering {
		v := true
		body.DisableReordering = &v
	}

	if body.Routine == "" {
		return nil, fmt.Errorf("routine is required (set via --routine or in --request-file)")
	}

	if body.Time <= 0 {
		return nil, fmt.Errorf("time is required (set via --time or in --request-file)")
	}

	return body, nil
}

func loadJSONFile(path string, v any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("failed to read request file %q: %w", path, err)
	}

	if err := json.Unmarshal(data, v); err != nil {
		return fmt.Errorf("failed to parse request file %q: %w", path, err)
	}

	return nil
}
