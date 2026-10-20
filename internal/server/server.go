// Copyright 2026 Aerospike, Inc.
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

package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/aerospike/absctl/internal/config"
	"github.com/aerospike/absctl/internal/models"
	"github.com/aerospike/absctl/internal/storage"
	"github.com/aerospike/aerospike-client-go/v8"
	backupmodels "github.com/aerospike/backup-go/models"
	"github.com/aerospike/backup-go/pkg/asinfo"
	infomodels "github.com/aerospike/backup-go/pkg/asinfo/models"
	commonclient "github.com/aerospike/tools-common-go/client"
)

var (
	// errServiceConfig is returned when a service is asked for with neither or both of
	// the command configurations. A service serves either the backup commands or the
	// restore commands, and it reads the cluster and storage settings from whichever of
	// the two it was given.
	errServiceConfig = errors.New("exactly one of the backup and restore configurations must be set")

	// errVersionUnsupported is returned when the oldest node in the cluster is too old
	// to serve server-integrated backup and restore.
	errVersionUnsupported = errors.New("does not support integrated backup")

	// errClusterNotStable is returned when a namespace still has migrations in flight,
	// which a job that reads or writes the whole namespace cannot start on.
	errClusterNotStable = errors.New("cluster is not stable")
)

// versionGetter reports the version of the oldest node in the cluster, which is what
// decides whether the cluster can serve these commands at all.
type versionGetter interface {
	GetVersion(ctx context.Context) (infomodels.AerospikeVersion, error)
}

// stabilityChecker reports whether a namespace has migrations in flight.
type stabilityChecker interface {
	GetClusterStable(ctx context.Context, namespace string) (bool, error)
}

// Service represents a server integrated backup and restore service.
type Service struct {
	backupCfg  *config.ServerBackupServiceConfig
	restoreCfg *config.ServerRestoreServiceConfig
	logger     *slog.Logger
	// metadataRetryPolicy paces the polling of a backup manifest that the server has
	// not uploaded yet.
	metadataRetryPolicy *backupmodels.RetryPolicy
	// vanishedPollInterval paces the lookups made while a job is missing from the
	// cluster status.
	vanishedPollInterval time.Duration
	// statePollInterval paces the lookups made while a command waits for the state its
	// request leads to: an aborted job to stop, or a namespace to be prepared, cleared or
	// opened.
	statePollInterval time.Duration
	// metadataS3Client opens the storage client backup manifests are read with.
	metadataS3Client func(ctx context.Context, cfg *models.AwsS3) (s3API, error)
}

// NewService initializes and returns a new Service instance. Exactly one of backupCfg and
// restoreCfg belongs to a service: the commands of the other one are served by a service
// of their own, and everything the service reads about the cluster and its storage comes
// from the configuration it was built with.
func NewService(
	backupCfg *config.ServerBackupServiceConfig,
	restoreCfg *config.ServerRestoreServiceConfig,
	logger *slog.Logger,
) (*Service, error) {
	if backupCfg == nil && restoreCfg == nil {
		return nil, fmt.Errorf("%w: neither is set", errServiceConfig)
	}

	if backupCfg != nil && restoreCfg != nil {
		return nil, fmt.Errorf("%w: both are set", errServiceConfig)
	}

	return &Service{
		backupCfg:  backupCfg,
		restoreCfg: restoreCfg,
		logger:     logger,
		metadataRetryPolicy: backupmodels.NewRetryPolicy(
			metadataRetryBase,
			metadataRetryMultiplier,
			metadataRetryAttempts,
		),
		vanishedPollInterval: vanishedJobPollInterval,
		statePollInterval:    statePollInterval,
		metadataS3Client:     newMetadataS3Client,
	}, nil
}

// clientConfig returns the cluster to connect to, from whichever of the two command
// configurations the service was built with. NewService accepts exactly one of them, so
// the other one is nil here.
func (s *Service) clientConfig() *commonclient.AerospikeConfig {
	if s.backupCfg != nil {
		return s.backupCfg.ClientConfig
	}

	return s.restoreCfg.ClientConfig
}

// clientPolicy returns the client policy to connect with, from the same configuration as
// clientConfig.
func (s *Service) clientPolicy() *models.ClientPolicy {
	if s.backupCfg != nil {
		return s.backupCfg.ClientPolicy
	}

	return s.restoreCfg.ClientPolicy
}

// infoClient is an asinfo client bundled with the Aerospike client that owns the
// cluster it talks through.
type infoClient struct {
	*asinfo.Client
	// We keep the Aerospike client so GC won't try to clean it.
	// Which caused errors on asinfo.Client.
	asClient *aerospike.Client
}

// Close shuts down the Aerospike connections behind the info client.
func (c *infoClient) Close() {
	c.asClient.Close()
}

// activeNodes returns the number of active nodes in the cluster.
func (c *infoClient) activeNodes() int {
	var n int

	for _, node := range c.asClient.GetNodes() {
		if node.IsActive() {
			n++
		}
	}

	return n
}

// newInfoClient opens the info client every command that talks to the cluster works
// through, and makes sure that cluster supports server-integrated backup and restore
// before handing it over. The check lives here rather than in the commands so that it
// cannot be forgotten by a new one, and so that an unsupported cluster is named as such
// instead of failing later on an info command it does not know. The caller owns the
// returned client and must close it.
//
// The commands served entirely from object storage - "snapshot-backup list" and
// "snapshot-backup validate" - need no cluster and never get here.
func (s *Service) newInfoClient(ctx context.Context) (*infoClient, error) {
	client, err := s.openInfoClient()
	if err != nil {
		return nil, err
	}

	if err := s.checkServerVersion(ctx, client); err != nil {
		client.Close()

		return nil, err
	}

	return client, nil
}

// openInfoClient builds the info client without asking the cluster anything about itself.
// The caller owns the returned client and must close it.
func (s *Service) openInfoClient() (*infoClient, error) {
	asClient, err := storage.NewAerospikeClient(
		s.clientConfig(),
		s.clientPolicy(),
		nil,
		0,
		s.logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create aerospike client: %w", err)
	}

	client, err := asinfo.NewClient(
		asClient.Cluster(),
		aerospike.NewInfoPolicy(),
		backupmodels.NewDefaultRetryPolicy(),
		s.logger,
	)
	if err != nil {
		asClient.Close()

		return nil, fmt.Errorf("failed to create info client: %w", err)
	}

	return &infoClient{Client: client, asClient: asClient}, nil
}

// checkServerVersion makes sure the oldest node in the cluster is new enough to serve
// server-integrated backup and restore.
func (s *Service) checkServerVersion(ctx context.Context, client versionGetter) error {
	lowestVersion, err := client.GetVersion(ctx)
	if err != nil {
		return fmt.Errorf("failed to get server version: %w", err)
	}

	if !lowestVersion.IsGreaterOrEqual(infomodels.AerospikeVersionSupportsIntegratedBackup) {
		return fmt.Errorf("server version %s %w", lowestVersion, errVersionUnsupported)
	}

	return nil
}

// checkClusterStable makes sure namespace has no migrations in flight, which is what a
// job that reads or writes the whole namespace needs before it starts. The commands that
// only ask the cluster to stop working - the aborts - do not need it: a job has to be
// stoppable on an unstable cluster too.
//
// The server version is checked when the info client is opened, so it is not repeated
// here.
func (s *Service) checkClusterStable(ctx context.Context, client stabilityChecker, namespace string) error {
	isStable, err := client.GetClusterStable(ctx, namespace)
	if err != nil {
		return fmt.Errorf("failed to check cluster stability: %w", err)
	}

	if !isStable {
		return fmt.Errorf("%w: namespace %s", errClusterNotStable, namespace)
	}

	return nil
}
