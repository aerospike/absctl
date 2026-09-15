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
	"path/filepath"
	"strconv"
	"time"

	"github.com/aerospike/absctl/internal/config"
	"github.com/aerospike/absctl/internal/logging"
	"github.com/aerospike/absctl/internal/models"
	"github.com/aerospike/absctl/internal/storage"
	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/backup-go"
	"github.com/aerospike/backup-go/errclass"
	backupmodels "github.com/aerospike/backup-go/models"
	"github.com/aerospike/backup-go/pkg/asinfo"
	infomodels "github.com/aerospike/backup-go/pkg/asinfo/models"
	"github.com/aerospike/backup-go/pkg/server/lister"
	servermodels "github.com/aerospike/backup-go/pkg/server/lister/models"
	"github.com/aerospike/backup-go/pkg/server/segvalidator"
	"github.com/aerospike/backup-go/pkg/server/segvalidator/streamers"
	commonclient "github.com/aerospike/tools-common-go/client"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const (
	// metadataRequestTimeout bounds a single manifest request, in milliseconds.
	metadataRequestTimeout = 10_000

	// The server uploads the manifest shortly after a backup completes, so it is polled
	// at a steady pace for about a minute instead of being backed off exponentially:
	// a growing delay would end in a long blind gap for a file that is due in seconds.
	metadataRetryBase       = 2 * time.Second
	metadataRetryMultiplier = 1.0
	metadataRetryAttempts   = 30

	// vanishedJobConfirmations is how many lookups in a row must miss a job that had
	// reached its last stages before the watcher accepts that the cluster dropped the
	// state of a finished backup. The status of a running job can be unavailable for a
	// moment - between backup stages, or while a node is busy - and a single miss must
	// not be reported as a finished backup. Together with vanishedJobPollInterval this
	// tolerates a gap of about 15 seconds.
	vanishedJobConfirmations = 5
	// vanishedJobPollInterval paces those confirmation lookups.
	vanishedJobPollInterval = 3 * time.Second
	// midBackupGapConfirmations bounds the same wait for a job that stopped reporting
	// before it reached its last stages. Such a gap cannot be a completion, so the job
	// is given a much longer benefit of the doubt - about two minutes - before the watch
	// gives up on it.
	midBackupGapConfirmations = 40
	// statusGapWarnEvery paces the reminders logged while a job is missing from the
	// cluster status, so that a long wait does not look like a hung command.
	statusGapWarnEvery = 20
)

var (
	// errBackupFailed is returned when the cluster reports a failed backup job.
	errBackupFailed = errors.New("backup failed")

	// errBackupStatusLost is returned when the cluster stopped reporting a job in the
	// middle of a backup and left no manifest behind.
	errBackupStatusLost = errors.New("cluster stopped reporting backup status")
)

// completionStates are the stages a backup job can be in when the cluster drops its
// status because the job is done: the state of a finished job is kept for a while and
// then discarded. A job that stops reporting from one of these stages has completed.
// A job that stops reporting from an earlier stage has not - its status is either
// temporarily unavailable or the job is gone for good - and treating that as a finished
// backup makes the watcher announce a success in the middle of a backup.
var completionStates = map[infomodels.BackupState]bool{
	infomodels.BackupStateStoppingChangeStream: true,
	infomodels.BackupStateFinalDraining:        true,
	infomodels.BackupStateComplete:             true,
}

// S3API is an interface for the S3 client.
type S3API interface {
	ListObjectsV2(ctx context.Context, in *s3.ListObjectsV2Input, opts ...func(*s3.Options),
	) (*s3.ListObjectsV2Output, error)
	GetObject(ctx context.Context, in *s3.GetObjectInput, opts ...func(*s3.Options),
	) (*s3.GetObjectOutput, error)
	HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
}

// backupStatusGetter is the part of asinfo.Client that the progress watcher needs.
type backupStatusGetter interface {
	GetBackupStatus(ctx context.Context, jobID string) (*infomodels.ResponseBackupState, error)
}

// metadataGetter is the part of lister.Lister that the progress watcher needs.
type metadataGetter interface {
	GetMetadata(ctx context.Context, backupID string) (servermodels.Metadata, error)
}

// Service represents a server integrated backup and restore service.
type Service struct {
	backupCfg  *config.ServerBackupServiceConfig
	restoreCfg *config.ServerRestoreServiceConfig
	logger     *slog.Logger
	// metadataRetryPolicy paces the polling of a backup manifest that the server has
	// not uploaded yet.
	metadataRetryPolicy *backupmodels.RetryPolicy
	// vanishedPollInterval paces the lookups that confirm a job the cluster stopped
	// reporting is really gone.
	vanishedPollInterval time.Duration
}

// NewService initializes and returns a new Service instance.
func NewService(
	backupCfg *config.ServerBackupServiceConfig,
	restoreCfg *config.ServerRestoreServiceConfig,
	logger *slog.Logger,
) (*Service, error) {
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
	}, nil
}

func (s *Service) clientConfig() *commonclient.AerospikeConfig {
	if s.backupCfg != nil {
		return s.backupCfg.ClientConfig
	}

	return s.restoreCfg.ClientConfig
}

func (s *Service) clientPolicy() *models.ClientPolicy {
	if s.backupCfg != nil {
		return s.backupCfg.ClientPolicy
	}

	return s.restoreCfg.ClientPolicy
}

// newInfoClient separate function for a lazy load.
func (s *Service) newInfoClient() (*asinfo.Client, error) {
	aerospikeClient, err := storage.NewAerospikeClient(
		s.clientConfig(),
		s.clientPolicy(),
		nil,
		0,
		s.logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create aerospike client: %w", err)
	}

	infoClient, err := asinfo.NewClient(
		aerospikeClient.Cluster(),
		aerospike.NewInfoPolicy(),
		backupmodels.NewDefaultRetryPolicy(),
		s.logger,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create info client: %w", err)
	}

	return infoClient, nil
}

// ListBackups lists all backups from the configured storage.
func (s *Service) ListBackups(ctx context.Context) error {
	client, err := storage.NewS3Client(ctx, s.backupCfg.AwsS3)
	if err != nil {
		return fmt.Errorf("failed to create s3 client: %w", err)
	}

	l := lister.NewLister(client, s.backupCfg.AwsS3.BucketName, s.backupCfg.List.Path, lister.WithLogger(s.logger))

	mds, err := l.FetchAllMetadata(ctx)
	if err != nil {
		return fmt.Errorf("failed to list backups: %w", err)
	}

	if len(mds) == 0 {
		// try to find metadata for a specific path.
		mds, err = findBackupByPath(ctx, l, s.backupCfg.List.Path)
		if err != nil {
			if errors.Is(err, backup.ErrNotFound) {
				s.logger.Info("backups not found")
				return nil
			}

			return fmt.Errorf("failed to list backups: %w", err)
		}
	}

	if err := logging.PrintMetadata(mds, s.backupCfg.App.LogJSON, s.logger); err != nil {
		return err
	}

	return nil
}

func findBackupByPath(ctx context.Context, l *lister.Lister, path string) ([]servermodels.Metadata, error) {
	backupID := filepath.Base(path)

	md, err := l.GetMetadata(ctx, backupID)
	if err != nil {
		return nil, err
	}

	return []servermodels.Metadata{md}, nil
}

// StartBackup initiates a backup process using the service's configured backup settings
// and returns an error if it fails.
func (s *Service) StartBackup(ctx context.Context) error {
	client, err := s.newInfoClient()
	if err != nil {
		return err
	}

	if err = s.checkServerStatus(ctx, client, s.backupCfg.Start.Namespace); err != nil {
		return fmt.Errorf("failed to check server status: %w", err)
	}

	var mb, ma string

	if s.backupCfg.Start.ModifiedBefore != "" {
		mbt, err := s.backupCfg.Start.ModifiedBeforeTime()
		if err != nil {
			return fmt.Errorf("failed to parse modified-before time: %w", err)
		}
		mb = strconv.FormatInt(mbt.Unix(), 10)
	}

	if s.backupCfg.Start.ModifiedAfter != "" {
		mat, err := s.backupCfg.Start.ModifiedAfterTime()
		if err != nil {
			return fmt.Errorf("failed to parse modified-after time: %w", err)
		}
		ma = strconv.FormatInt(mat.Unix(), 10)
	}

	bReq := &infomodels.RequestBackup{
		Namespace:          s.backupCfg.Start.Namespace,
		Storage:            s.backupCfg.Start.StorageType,
		Bucket:             s.backupCfg.AwsS3.BucketName,
		Region:             s.backupCfg.AwsS3.Region,
		Profile:            s.backupCfg.AwsS3.Profile,
		AccessKey:          s.backupCfg.AwsS3.AccessKeyID,
		SecretKey:          s.backupCfg.AwsS3.SecretAccessKey,
		Endpoint:           s.backupCfg.AwsS3.Endpoint,
		ModifiedAfter:      ma,
		ModifiedBefore:     mb,
		SetList:            s.backupCfg.Start.SetList,
		NoIndexes:          s.backupCfg.Start.NoIndexes,
		NoUDFs:             s.backupCfg.Start.NoUDFs,
		EnableChangeStream: s.backupCfg.Start.EnableChangeStream,
	}

	JobID, err := client.StartServerBackup(ctx, bReq)
	if err != nil {
		return fmt.Errorf("failed to start backup: %w", err)
	}

	s.logger.Info("server integrated backup started",
		slog.String("backup-id", JobID))

	return nil
}

// StartRestore initiates a restore process for the specified job ID using the service's backup configuration.
func (s *Service) StartRestore(ctx context.Context) error {
	infoClient, err := s.newInfoClient()
	if err != nil {
		return err
	}

	if err = s.checkServerStatus(ctx, infoClient, s.restoreCfg.Start.Namespace); err != nil {
		return fmt.Errorf("failed to check server status: %w", err)
	}

	s3Client, err := storage.NewS3Client(ctx, s.restoreCfg.AwsS3)
	if err != nil {
		return fmt.Errorf("failed to create s3 client: %w", err)
	}

	if err := s.checkBackupExists(ctx, s3Client,
		s.restoreCfg.AwsS3.BucketName, s.restoreCfg.Start.JobID); err != nil {
		return err
	}

	rReq := &infomodels.RequestRestore{
		Namespace:    s.restoreCfg.Start.Namespace,
		Storage:      s.restoreCfg.Start.StorageType,
		Bucket:       s.restoreCfg.AwsS3.BucketName,
		Region:       s.restoreCfg.AwsS3.Region,
		Profile:      s.restoreCfg.AwsS3.Profile,
		AccessKey:    s.restoreCfg.AwsS3.AccessKeyID,
		SecretKey:    s.restoreCfg.AwsS3.SecretAccessKey,
		Endpoint:     s.restoreCfg.AwsS3.Endpoint,
		JobID:        s.restoreCfg.Start.JobID,
		Path:         s.restoreCfg.Start.Path,
		FuzzyRestore: s.restoreCfg.Start.FuzzyRestore,
	}

	err = infoClient.StartServerRestore(ctx, rReq)
	if err != nil {
		return fmt.Errorf("failed to start restore: %w", err)
	}

	s.logger.Info("server integrated restore started",
		slog.String("backup-id", s.restoreCfg.Start.JobID))

	return nil
}

// PrepareRestore initiates a restore preparation process for the specified job ID.
func (s *Service) PrepareRestore(ctx context.Context) error {
	client, err := s.newInfoClient()
	if err != nil {
		return err
	}

	if err = s.checkServerStatus(ctx, client, s.restoreCfg.Prepare.Namespace); err != nil {
		return fmt.Errorf("failed to check server status: %w", err)
	}

	err = client.PrepareServerRestore(
		ctx,
		s.restoreCfg.Prepare.JobID,
		s.restoreCfg.Prepare.Namespace,
	)
	if err != nil {
		return fmt.Errorf("failed to prepare restore: %w", err)
	}

	s.logger.Info("restore preparation started",
		slog.String("backup-id", s.restoreCfg.Prepare.JobID))

	return nil
}

// BackupProgress returns the progress of the currently running backup.
func (s *Service) BackupProgress(ctx context.Context) error {
	infoClient, err := s.newInfoClient()
	if err != nil {
		return err
	}

	// The storage client is created before the watch loop on purpose: a wrong endpoint
	// or wrong credentials must fail now, not after hours of watching a backup.
	s3Client, err := storage.NewS3Client(ctx, s.metadataS3Config())
	if err != nil {
		return fmt.Errorf("failed to create s3 client: %w", err)
	}

	l := lister.NewLister(s3Client, s.backupCfg.AwsS3.BucketName, "", lister.WithLogger(s.logger))

	return s.reportBackupProgress(ctx, infoClient, l)
}

// reportBackupProgress polls the cluster for the status of the configured job and logs
// it until the backup reaches a terminal state, or once when watching is disabled.
func (s *Service) reportBackupProgress(ctx context.Context, client backupStatusGetter, l metadataGetter) error {
	jobID := s.backupCfg.Progress.JobID
	printer := newProgressPrinter(s.logger)

	// lastState is the state the cluster reported for this job most recently. It stays
	// empty while the job has never been observed, and it decides what a job that stops
	// reporting means.
	var lastState infomodels.BackupState
	// misses counts the lookups in a row that did not find the job.
	var misses int

	for {
		now := time.Now()

		status, err := client.GetBackupStatus(ctx, jobID)

		switch {
		case err != nil && !errors.Is(err, asinfo.ErrNotFound):
			return fmt.Errorf("failed to get backup status: %w", err)
		case err != nil:
			misses++

			if s.jobVanished(lastState, misses) {
				return s.reportMissingBackup(ctx, l, jobID, lastState)
			}

			s.logStatusGap(jobID, lastState, misses)

			if err := waitFor(ctx, s.vanishedPollInterval); err != nil {
				return err
			}

			continue
		}

		lastState = status.State
		misses = 0

		printer.Print(status, now)

		switch status.State {
		case infomodels.BackupStateComplete:
			s.logger.Info("backup complete")

			return s.printMetadata(ctx, l, jobID, true)
		case infomodels.BackupStateFailed:
			return fmt.Errorf("%w: backup-id %s", errBackupFailed, jobID)
		}

		if !s.backupCfg.Progress.Watch {
			return nil
		}

		if err := waitFor(ctx, nextPoll(status, now)); err != nil {
			return err
		}
	}
}

// jobVanished reports whether a lookup that did not find the job is final. A one-shot
// lookup made without --watch is answered right away. A watched job has to be missing
// several times in a row, so that a momentary gap in the cluster status is not mistaken
// for a terminal outcome, and a job that was seen before it reached its last stages gets
// the longer of the two waits: its status is the only thing that can end the watch.
func (s *Service) jobVanished(lastState infomodels.BackupState, misses int) bool {
	switch {
	case !s.backupCfg.Progress.Watch:
		return true
	case lastState == "" || completionStates[lastState]:
		return misses >= vanishedJobConfirmations
	default:
		return misses >= midBackupGapConfirmations
	}
}

// logStatusGap reports a job that is temporarily missing from the cluster status. The
// first confirmations are routine and stay on the debug level, but a gap that outlives
// them is worth telling the user about, and is repeated while it lasts: it is the only
// sign that the watch is still waiting on a backup the cluster is no longer talking
// about.
func (s *Service) logStatusGap(jobID string, lastState infomodels.BackupState, misses int) {
	attrs := []any{
		slog.String("backup-id", jobID),
		slog.Int("attempt", misses),
	}

	if lastState != "" {
		attrs = append(attrs, slog.String("last-state", lastState.Describe()))
	}

	if misses == vanishedJobConfirmations || misses%statusGapWarnEvery == 0 {
		s.logger.Warn("backup status is unavailable, still waiting for the job", attrs...)

		return
	}

	s.logger.Debug("backup status is temporarily unavailable", attrs...)
}

// waitFor sleeps for d and returns the context error if the watch is canceled first.
func waitFor(ctx context.Context, d time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
		return nil
	}
}

// reportMissingBackup handles a job the cluster no longer reports, once the caller has
// confirmed that it is really gone. The cluster drops the state of a job shortly after
// finishing it, so a job last seen in one of the completionStates has completed, and its
// manifest is worth waiting for. A job that was never seen either finished long ago or
// does not exist, and is looked up only once. Anything else stopped reporting in the
// middle of a backup, and only a manifest can turn that into a completion.
func (s *Service) reportMissingBackup(
	ctx context.Context,
	l metadataGetter,
	jobID string,
	lastState infomodels.BackupState,
) error {
	switch {
	case lastState == "":
		s.logger.Info("no running backup found")

		return s.printMetadata(ctx, l, jobID, false)
	case completionStates[lastState]:
		s.logger.Info("backup complete")

		return s.printMetadata(ctx, l, jobID, true)
	}

	// The watch may have been between polls while the job ran through its last stages,
	// in which case the backup is done and the manifest is already in place. A missing
	// manifest means the backup did not finish, and reporting it as complete would hide
	// a job that died, was aborted, or fell out of the cluster status for good. Any
	// other failure is about the storage and is reported as itself.
	md, err := s.fetchMetadata(ctx, l, jobID, false)

	switch {
	case err == nil:
		s.logger.Info("backup complete")

		return s.printMetadataEntry(md)
	case errors.Is(err, errclass.ErrNotFound):
		return fmt.Errorf("%w: backup-id %s, last reported state %q",
			errBackupStatusLost, jobID, lastState.Describe())
	default:
		return err
	}
}

// printMetadata reads the manifest of a finished backup and prints it.
func (s *Service) printMetadata(ctx context.Context, l metadataGetter, jobID string, wait bool) error {
	md, err := s.fetchMetadata(ctx, l, jobID, wait)
	if err != nil {
		return err
	}

	return s.printMetadataEntry(md)
}

// printMetadataEntry prints a single backup manifest.
func (s *Service) printMetadataEntry(md servermodels.Metadata) error {
	return logging.PrintMetadata([]servermodels.Metadata{md}, s.backupCfg.App.LogJSON, s.logger)
}

// fetchMetadata reads the backup manifest from object storage. Only a missing manifest is
// retried, and only when wait is set: every other failure is permanent, and polling for a
// backup that never existed would just hang the command.
func (s *Service) fetchMetadata(
	ctx context.Context,
	l metadataGetter,
	jobID string,
	wait bool,
) (servermodels.Metadata, error) {
	if !wait {
		md, err := l.GetMetadata(ctx, jobID)
		if err != nil {
			return servermodels.Metadata{}, fmt.Errorf("failed to get backup metadata: %w", err)
		}

		return md, nil
	}

	var (
		md      servermodels.Metadata
		permErr error
	)

	retryErr := s.metadataRetryPolicy.Do(ctx, func() error {
		var err error

		md, err = l.GetMetadata(ctx, jobID)

		switch {
		case err == nil:
			return nil
		case errors.Is(err, errclass.ErrNotFound):
			s.logger.Debug("backup metadata is not uploaded yet", slog.String("backup-id", jobID))

			return err
		default:
			permErr = err

			return nil
		}
	})

	switch {
	case permErr != nil:
		return servermodels.Metadata{}, fmt.Errorf("failed to get backup metadata: %w", permErr)
	case retryErr != nil:
		return servermodels.Metadata{}, fmt.Errorf("timed out waiting for backup metadata: %w", retryErr)
	}

	return md, nil
}

// metadataS3Config returns a copy of the S3 configuration with a bounded request timeout,
// so that reading a manifest never mutates the configuration shared with other commands.
func (s *Service) metadataS3Config() *models.AwsS3 {
	cfg := *s.backupCfg.AwsS3
	cfg.RequestTimeout = metadataRequestTimeout

	return &cfg
}

// RestoreProgress returns the progress of the currently running restore.
func (s *Service) RestoreProgress(ctx context.Context) error {
	client, err := s.newInfoClient()
	if err != nil {
		return err
	}

	result, err := client.GetRestoreStatus(ctx, s.restoreCfg.Progress.Namespace)
	if err != nil {
		return fmt.Errorf("failed to get backup status: %w", err)
	}

	s.logger.Info("restore progress",
		slog.String("result", result))

	return nil
}

// BackupValidate validates the backup identified by the configured job ID.
func (s *Service) BackupValidate(ctx context.Context) error {
	client, err := storage.NewS3Client(ctx, s.backupCfg.AwsS3)
	if err != nil {
		return fmt.Errorf("failed to create s3 client: %w", err)
	}

	if err := s.checkBackupExists(ctx, client, s.backupCfg.AwsS3.BucketName, s.backupCfg.Validation.JobID); err != nil {
		return err
	}

	streamer, err := streamers.NewS3(
		client,
		s.backupCfg.AwsS3.BucketName,
		s.backupCfg.Validation.JobID,
		streamers.WithLogger(s.logger),
	)
	if err != nil {
		return fmt.Errorf("failed to create s3 streamer: %w", err)
	}

	v, err := segvalidator.NewSegValidator(streamer, segvalidator.WithLogger(s.logger))
	if err != nil {
		return fmt.Errorf("failed to create seg validator: %w", err)
	}

	report, err := v.Validate(ctx, s.backupCfg.Validation.SampleSize)
	if err != nil {
		return fmt.Errorf("failed to validate: %w", err)
	}

	logging.PrintServerValidationReport(report, s.backupCfg.App.LogJSON, s.logger)

	return nil
}

// checkServerStatus validates the server status.
func (s *Service) checkServerStatus(ctx context.Context, client *asinfo.Client, namespace string) error {
	lowestVersion, err := client.GetVersion(ctx)
	if err != nil {
		return fmt.Errorf("failed to get server version: %w", err)
	}

	if !lowestVersion.IsGreaterOrEqual(infomodels.AerospikeVersionSupportsIntegratedBackup) {
		return fmt.Errorf("server version %s does not support integrated backup", lowestVersion)
	}

	isStable, err := client.GetClusterStable(ctx, namespace)
	if err != nil {
		return fmt.Errorf("failed to check cluster stability: %w", err)
	}

	if !isStable {
		return fmt.Errorf("cluster is not stable")
	}

	return nil
}

// checkBackupExists validates the backup exists for RESTORE only.
func (s *Service) checkBackupExists(ctx context.Context, client S3API, bucket, jobID string) error {
	l := lister.NewLister(client, bucket, "", lister.WithLogger(s.logger))

	md, err := l.GetMetadata(ctx, jobID)
	if err != nil {
		return fmt.Errorf("failed to check if backup exists: %w", err)
	}

	s.logger.Info("backup found",
		slog.String("backup-id", md.BackupID),
		slog.String("namespace", md.Namespace),
	)

	return nil
}
