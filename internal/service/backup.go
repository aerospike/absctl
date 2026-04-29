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
)

// BackupHandler handles backup-related REST API calls and renders responses
// either as human-readable tables/sections or as structured slog entries
// depending on the JSON output flag.
type BackupHandler struct {
	client *api.ClientWithResponses
	logger *slog.Logger
	json   bool
}

// NewBackupHandler creates a new BackupHandler. The logger and json flag are
// forwarded to the logging package which selects the appropriate renderer.
func NewBackupHandler(serverURL string, logger *slog.Logger, json bool) (*BackupHandler, error) {
	client, err := api.NewClientWithResponses(serverURL)
	if err != nil {
		return nil, fmt.Errorf("failed to create API client: %w", err)
	}

	return &BackupHandler{
		client: client,
		logger: logger,
		json:   json,
	}, nil
}

// Cancel cancels a currently running backup for the given routine.
func (h *BackupHandler) Cancel(ctx context.Context, name string) error {
	if name == "" {
		return fmt.Errorf("routine name is required")
	}

	resp, err := h.client.CancelCurrentBackupWithResponse(ctx, name)
	if err != nil {
		return fmt.Errorf("cancel backup request failed: %w", err)
	}

	if resp.StatusCode() != http.StatusAccepted {
		return fmt.Errorf("cancel backup failed: %s", extractErrorBody(resp.Body))
	}

	return nil
}

// Status fetches the current backup state for the given routine and prints it.
func (h *BackupHandler) Status(ctx context.Context, name string) error {
	if name == "" {
		return fmt.Errorf("routine name is required")
	}

	resp, err := h.client.GetCurrentBackupWithResponse(ctx, name)
	if err != nil {
		return fmt.Errorf("get backup status request failed: %w", err)
	}

	if resp.StatusCode() != http.StatusOK {
		return fmt.Errorf("get backup status failed: %s", extractErrorBody(resp.Body))
	}

	logging.PrintBackupState(resp.JSON200, name, h.json, h.logger)

	return nil
}

// ListFull fetches full backups (optionally filtered by routine name and
// time range) and prints them.
func (h *BackupHandler) ListFull(ctx context.Context, name string, from, to int64) error {
	if name != "" {
		return h.listFullForRoutine(ctx, name, from, to)
	}

	return h.listFullAll(ctx, from, to)
}

func (h *BackupHandler) listFullAll(ctx context.Context, from, to int64) error {
	params := &api.GetFullBackupsParams{}
	applyTimestampFilters(params, from, to)

	resp, err := h.client.GetFullBackupsWithResponse(ctx, params)
	if err != nil {
		return fmt.Errorf("list full backups request failed: %w", err)
	}

	if resp.StatusCode() != http.StatusOK {
		return fmt.Errorf("list full backups failed: %s", extractErrorBody(resp.Body))
	}

	logging.PrintBackupDetailsByRoutine(derefMap(resp.JSON200), false, h.json, h.logger)

	return nil
}

func (h *BackupHandler) listFullForRoutine(ctx context.Context, name string, from, to int64) error {
	params := &api.GetFullBackupsForRoutineParams{}
	applyTimestampFilters(params, from, to)

	resp, err := h.client.GetFullBackupsForRoutineWithResponse(ctx, name, params)
	if err != nil {
		return fmt.Errorf("list full backups for routine request failed: %w", err)
	}

	if resp.StatusCode() != http.StatusOK {
		return fmt.Errorf("list full backups for routine failed: %s", extractErrorBody(resp.Body))
	}

	logging.PrintBackupDetailsForRoutine(derefSlice(resp.JSON200), name, false, h.json, h.logger)

	return nil
}

// ListIncremental fetches incremental backups (optionally filtered by routine
// name and time range) and prints them.
func (h *BackupHandler) ListIncremental(ctx context.Context, name string, from, to int64) error {
	if name != "" {
		return h.listIncrementalForRoutine(ctx, name, from, to)
	}

	return h.listIncrementalAll(ctx, from, to)
}

func (h *BackupHandler) listIncrementalAll(ctx context.Context, from, to int64) error {
	params := &api.GetIncrementalBackupsParams{}
	applyTimestampFilters(params, from, to)

	resp, err := h.client.GetIncrementalBackupsWithResponse(ctx, params)
	if err != nil {
		return fmt.Errorf("list incremental backups request failed: %w", err)
	}

	if resp.StatusCode() != http.StatusOK {
		return fmt.Errorf("list incremental backups failed: %s", extractErrorBody(resp.Body))
	}

	logging.PrintBackupDetailsByRoutine(derefMap(resp.JSON200), true, h.json, h.logger)

	return nil
}

func (h *BackupHandler) listIncrementalForRoutine(ctx context.Context, name string, from, to int64) error {
	params := &api.GetIncrementalBackupsForRoutineParams{}
	applyTimestampFilters(params, from, to)

	resp, err := h.client.GetIncrementalBackupsForRoutineWithResponse(ctx, name, params)
	if err != nil {
		return fmt.Errorf("list incremental backups for routine request failed: %w", err)
	}

	if resp.StatusCode() != http.StatusOK {
		return fmt.Errorf("list incremental backups for routine failed: %s", extractErrorBody(resp.Body))
	}

	logging.PrintBackupDetailsForRoutine(derefSlice(resp.JSON200), name, true, h.json, h.logger)

	return nil
}

// TriggerFull triggers a full backup for the given routine.
func (h *BackupHandler) TriggerFull(ctx context.Context, name string, delay int) error {
	if name == "" {
		return fmt.Errorf("routine name is required")
	}

	params := &api.TriggerFullBackupParams{}
	if delay > 0 {
		params.Delay = &delay
	}

	resp, err := h.client.TriggerFullBackupWithResponse(ctx, name, params)
	if err != nil {
		return fmt.Errorf("trigger full backup request failed: %w", err)
	}

	if resp.StatusCode() != http.StatusAccepted {
		return fmt.Errorf("trigger full backup failed: %s", extractErrorBody(resp.Body))
	}

	return nil
}

// TriggerIncremental triggers an incremental backup for the given routine.
func (h *BackupHandler) TriggerIncremental(ctx context.Context, name string, delay int) error {
	if name == "" {
		return fmt.Errorf("routine name is required")
	}

	params := &api.TriggerIncrementalBackupParams{}
	if delay > 0 {
		params.Delay = &delay
	}

	resp, err := h.client.TriggerIncrementalBackupWithResponse(ctx, name, params)
	if err != nil {
		return fmt.Errorf("trigger incremental backup request failed: %w", err)
	}

	if resp.StatusCode() != http.StatusAccepted {
		return fmt.Errorf("trigger incremental backup failed: %s", extractErrorBody(resp.Body))
	}

	return nil
}

// timestampFilterSetter is a generic interface for param structs with From/To fields.
type timestampFilterSetter interface {
	*api.GetFullBackupsParams |
		*api.GetFullBackupsForRoutineParams |
		*api.GetIncrementalBackupsParams |
		*api.GetIncrementalBackupsForRoutineParams
}

func applyTimestampFilters[T timestampFilterSetter](params T, from, to int64) {
	switch p := any(params).(type) {
	case *api.GetFullBackupsParams:
		if from > 0 {
			p.From = &from
		}

		if to > 0 {
			p.To = &to
		}
	case *api.GetFullBackupsForRoutineParams:
		if from > 0 {
			p.From = &from
		}

		if to > 0 {
			p.To = &to
		}
	case *api.GetIncrementalBackupsParams:
		if from > 0 {
			p.From = &from
		}

		if to > 0 {
			p.To = &to
		}
	case *api.GetIncrementalBackupsForRoutineParams:
		if from > 0 {
			p.From = &from
		}

		if to > 0 {
			p.To = &to
		}
	}
}

func extractErrorBody(body []byte) string {
	if len(body) == 0 {
		return "unknown error"
	}

	return string(body)
}
