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
	"fmt"
	"log/slog"

	"github.com/aerospike/absctl/internal/config"
	"github.com/aerospike/absctl/internal/lister"
	"github.com/aerospike/absctl/internal/storage"
	"github.com/aerospike/backup-go"
)

// Service represents a server side backup and restore service.
type Service struct {
	backupClient *backup.Client
	config       *config.SSBServiceConfig

	reader backup.StreamingReader

	// reportToLog bool

	logger *slog.Logger
}

// NewService initializes and returns a new Service instance.
func NewService(
	ctx context.Context,
	cfg *config.SSBServiceConfig,
	logger *slog.Logger,
) (*Service, error) {
	reader, err := storage.NewReader(
		ctx,
		&cfg.ServiceConfigCommon,
		cfg.SSb.List,
		"",
		"",
		"",
		0,
		false,
		logger,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create reader: %w", err)
	}

	aerospikeClient, err := storage.NewAerospikeClient(
		cfg.ClientConfig,
		cfg.ClientPolicy,
		nil,
		0,
		logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create aerospike client: %w", err)
	}

	backupClient, err := backup.NewClient(
		aerospikeClient,
		backup.WithLogger(logger),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create backup client: %w", err)
	}

	return &Service{
		backupClient: backupClient,
		reader:       reader,
		logger:       logger,
	}, nil
}

func (s *Service) Run(ctx context.Context) error {
	if s.config.SSb.List != "" {
		l := lister.NewLister(s.reader, &lister.MetafileParserAbs{})

		backups, err := l.ListABS(ctx, s.config.SSb.List)
		if err != nil {
			return fmt.Errorf("failed to list backups: %w", err)
		}

		fmt.Println(backups)
	}

	return nil
}
