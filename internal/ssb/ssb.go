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

package ssb

import (
	"context"
	"log/slog"

	"github.com/aerospike/absctl/internal/config"
	"github.com/aerospike/backup-go"
)

// Service represents a server side backup and restore service.
type Service struct {
	backupClient *backup.Client

	reader backup.StreamingReader

	reportToLog bool

	logger *slog.Logger
}

// NewService initializes and returns a new Service instance.
func NewService(
	ctx context.Context,
	cfg *config.ServiceConfigCommon,
	logger *slog.Logger,
) (*Service, error) {
	return &Service{
		logger: logger,
	}, nil
}

func (s *Service) Run(ctx context.Context) error {
	return nil
}
