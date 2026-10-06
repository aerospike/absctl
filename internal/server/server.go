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
	"path"
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
	// metadataRequestTimeoutMillis bounds a single manifest request.
	metadataRequestTimeoutMillis = 10_000

	// The server uploads the manifest shortly after a backup completes, so it is polled
	// at a steady pace for about a minute instead of being backed off exponentially:
	// a growing delay would end in a long blind gap for a file that is due in seconds.
	metadataRetryBase       = 2 * time.Second
	metadataRetryMultiplier = 1.0
	metadataRetryAttempts   = 30

	// startedJobConfirmations is how many lookups a job the cluster has just accepted is
	// given to turn up in the cluster status. "backup start" follows the job the cluster
	// handed it, so that job does exist, and reporting it as missing because the status
	// does not list it yet would fail a command that has just succeeded. A job named on
	// the command line gets no such grace: the first answer about it is final.
	startedJobConfirmations = 5

	// vanishedJobConfirmations is how many lookups in a row must miss a job that was
	// being watched and had reached its last stages, before the watcher accepts that the
	// cluster dropped the state of a finished backup. The status of a running job can be
	// unavailable for a moment - between backup stages, or while a node is busy - and a
	// single miss must not be reported as a finished backup.
	//
	// A job that has never been reported is not confirmed this way: it has no state to
	// resolve, and startedJobConfirmations decides how long it is waited for.
	vanishedJobConfirmations = 5
	// vanishedJobPollInterval paces those confirmation lookups. A lookup also costs the
	// retries the info client makes internally, so a confirmation takes longer than this
	// interval alone: the client is built with the default retry policy, which backs off
	// over about three seconds before it reports a job as missing.
	vanishedJobPollInterval = 3 * time.Second
	// midBackupGapConfirmations bounds the same wait for a job that stopped reporting
	// before it reached its last stages. Such a gap cannot be a completion, so the job
	// is given a much longer benefit of the doubt before the watch gives up on it.
	midBackupGapConfirmations = 40
	// statusGapLogEvery paces the reminders logged while a job is missing from the
	// cluster status, so that a long wait can be followed in a debug log without a
	// line per lookup.
	statusGapLogEvery = 20

	// abortStatusTimeout bounds the wait for an aborted job to stop running, and
	// abortStatusPollInterval paces the status lookups made while waiting.
	abortStatusTimeout      = 5 * time.Minute
	abortStatusPollInterval = 5 * time.Second

	// msgBackupComplete announces a finished backup, whether the completion came from
	// the job status or from the manifest of a job the cluster no longer reports.
	msgBackupComplete = "backup complete"

	// msgNoRunningBackup and msgNoRunningRestore are reported by an abort that found
	// nothing running to abort.
	msgNoRunningBackup  = "no running backup found, nothing to abort"
	msgNoRunningRestore = "no running restore found, nothing to abort"
)

var (
	// errServiceConfig is returned when a service is asked for with neither or both of
	// the command configurations. A service serves either the backup commands or the
	// restore commands, and it reads the cluster and storage settings from whichever of
	// the two it was given.
	errServiceConfig = errors.New("exactly one of the backup and restore configurations must be set")

	// errBackupNotFound is returned when the cluster does not report the requested job
	// at all: the id names no backup the cluster is working on, and none whose state it
	// still holds.
	errBackupNotFound = errors.New("no backup found")

	// errEmptyBackupStatus stands for a status lookup that came back with neither a
	// status nor a failure. It wraps asinfo.ErrNotFound because an empty answer says
	// exactly as much about the job as one that did not find it, and every caller
	// already knows what to do with a job the cluster does not report.
	errEmptyBackupStatus = fmt.Errorf("empty backup status: %w", asinfo.ErrNotFound)

	// errVersionUnsupported is returned when the oldest node in the cluster is too old
	// to serve server-integrated backup and restore.
	errVersionUnsupported = errors.New("does not support integrated backup")

	// errBackupFailed is returned when the cluster reports a failed backup job.
	errBackupFailed = errors.New("backup failed")

	// errBackupAborted is returned when the cluster reports a backup job that was
	// aborted: the job stopped on request and left no usable backup behind.
	errBackupAborted = errors.New("backup aborted")

	// errBackupStatusLost is returned when the cluster stopped reporting a job in the
	// middle of a backup and left no manifest behind.
	errBackupStatusLost = errors.New("cluster stopped reporting backup status")

	// errAbortStatusTimeout is returned when the cluster accepted an abort request but
	// kept reporting the job as running until the wait ran out.
	errAbortStatusTimeout = errors.New("checking backup status after abortion timed out")

	// errAbortRestoreStatusTimeout is the same for a restore whose namespace kept
	// reporting an unfinished restore until the wait ran out.
	errAbortRestoreStatusTimeout = errors.New("checking restore status after abortion timed out")

	// errClusterNotStable is returned when a namespace still has migrations in flight,
	// which a job that reads or writes the whole namespace cannot start on.
	errClusterNotStable = errors.New("cluster is not stable")
)

// restoreActiveStates are the namespace restore states an abort has something to do in.
// Besides the two states a job works in, this covers a namespace that only got as far as
// "snapshot-restore prepare": a prepared namespace blocks writes until the restore ends,
// and the abort is what releases that block, so it is neither a namespace to refuse the
// abort for nor one to accept the abort as landed in.
//
// The remaining states - and a namespace the cluster reports no state for - mean that
// nothing is restoring into it and nothing is held on its behalf.
var restoreActiveStates = map[string]bool{
	infomodels.RestoreStatePreparing: true,
	infomodels.RestoreStateReady:     true,
	infomodels.RestoreStateRestoring: true,
}

// terminalBackupStates are the states of a backup job that has already ended. The cluster
// keeps the state of a finished job for a while after it stops working on it, so a job
// reporting one of these is not running and holds nothing left to abort.
//
// BackupStateUnknown is deliberately not one of them: a state that could not be read says
// nothing about whether the job runs, and refusing the abort on it would leave a running
// backup with no way to stop it.
var terminalBackupStates = map[infomodels.BackupState]bool{
	infomodels.BackupStateComplete: true,
	infomodels.BackupStateFailed:   true,
	infomodels.BackupStateAborted:  true,
}

// completionStates are the stages a backup job can be in when the cluster drops its
// status because the job is done: the state of a finished job is kept for a while and
// then discarded. A job that stops reporting from one of these stages has completed.
// A job that stops reporting from an earlier stage has not - its status is either
// temporarily unavailable or the job is gone for good - and treating that as a finished
// backup makes the watcher announce a success in the middle of a backup.
var completionStates = map[infomodels.BackupState]bool{
	infomodels.BackupStateStoppingChangeStream: true,
	infomodels.BackupStateFinalDraining:        true,
	infomodels.BackupStateCommitting:           true,
	infomodels.BackupStateComplete:             true,
}

// s3API is the part of the S3 client that reading a backup manifest needs: the calls the
// lister built on top of it makes.
type s3API interface {
	ListObjectsV2(ctx context.Context, in *s3.ListObjectsV2Input, opts ...func(*s3.Options),
	) (*s3.ListObjectsV2Output, error)
	GetObject(ctx context.Context, in *s3.GetObjectInput, opts ...func(*s3.Options),
	) (*s3.GetObjectOutput, error)
}

// metadataGetter is the part of lister.Lister that the progress watcher needs.
type metadataGetter interface {
	GetMetadata(ctx context.Context, backupID string) (servermodels.Metadata, error)
}

// backupLister is the part of lister.Lister that listing needs: the manifests under the
// configured path, plus the one manifest a path naming a single backup holds.
type backupLister interface {
	metadataGetter
	FetchAllMetadata(ctx context.Context) ([]servermodels.Metadata, error)
}

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
	// vanishedPollInterval paces the lookups that confirm a job the cluster stopped
	// reporting is really gone.
	vanishedPollInterval time.Duration
	// abortPollInterval paces the lookups that confirm an aborted job stopped running,
	// and abortTimeout bounds how long they are made for.
	abortPollInterval time.Duration
	abortTimeout      time.Duration
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
		abortPollInterval:    abortStatusPollInterval,
		abortTimeout:         abortStatusTimeout,
		metadataS3Client:     newMetadataS3Client,
	}, nil
}

// newMetadataS3Client opens an S3 client for reading backup manifests. Its requests get
// the bounded manifest timeout, so an unreachable storage fails the command instead of
// holding it.
func newMetadataS3Client(ctx context.Context, cfg *models.AwsS3) (s3API, error) {
	client, err := storage.NewS3Client(ctx, metadataS3Config(cfg))
	if err != nil {
		return nil, fmt.Errorf("failed to create s3 client: %w", err)
	}

	return client, nil
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

// ListBackups lists all backups from the configured storage.
func (s *Service) ListBackups(ctx context.Context) error {
	client, err := storage.NewS3Client(ctx, s.backupCfg.AwsS3)
	if err != nil {
		return fmt.Errorf("failed to create s3 client: %w", err)
	}

	listPath := models.NormalizeS3Prefix(s.backupCfg.List.Path)
	l := lister.NewLister(client, s.backupCfg.AwsS3.BucketName, listPath, lister.WithLogger(s.logger))

	return s.listBackups(ctx, l, listPath)
}

// listBackups prints the manifests of the backups under listPath.
func (s *Service) listBackups(ctx context.Context, l backupLister, listPath string) error {
	mds, err := l.FetchAllMetadata(ctx)
	if err != nil {
		return fmt.Errorf("failed to list backups: %w", err)
	}

	// Nothing under the path can also mean that the path names a single backup rather
	// than a prefix holding several, so it is read as one before it is called empty.
	if len(mds) == 0 {
		md, err := l.GetMetadata(ctx, listPath)
		if err != nil {
			if errors.Is(err, errclass.ErrNotFound) {
				s.logger.Info("backups not found")

				return nil
			}

			return fmt.Errorf("failed to list backups: %w", err)
		}

		mds = []servermodels.Metadata{md}
	}

	return logging.PrintMetadata(mds, s.backupCfg.App.LogJSON, s.logger)
}

// StartBackup initiates a backup process using the service's configured backup settings,
// then follows the job it started until the backup reaches a terminal state. With Async
// set it returns as soon as the cluster accepts the job.
func (s *Service) StartBackup(ctx context.Context) error {
	client, err := s.newInfoClient(ctx)
	if err != nil {
		return err
	}
	defer client.Close()

	if err := s.checkClusterStable(ctx, client, s.backupCfg.Start.Namespace); err != nil {
		return err
	}

	bReq, err := s.backupRequest()
	if err != nil {
		return err
	}

	// Following the backup ends by reading its manifest, so the storage is opened before
	// the backup is requested: an unusable configuration has to fail the command now,
	// not once there is a running backup that cannot be followed. It is the same
	// configuration the cluster writes the backup with, so a backup started without it
	// would not get far either.
	l, err := s.startedBackupMetadataLister(ctx)
	if err != nil {
		return err
	}

	jobID, err := client.StartBackup(ctx, bReq)
	if err != nil {
		return fmt.Errorf("failed to start backup: %w", err)
	}

	s.logger.Info("server integrated backup started",
		slog.String("backup-id", jobID))

	if s.backupCfg.Start.Async {
		return nil
	}

	// The backup runs in the cluster, so watching it only reads its status: leaving the
	// watch, whether by an interrupt or by a failure here, does not stop the backup, and
	// "backup abort" is the only thing that does.
	return s.reportBackupProgress(ctx, client, l, jobID, followStartedJob)
}

// startedBackupMetadataLister returns the reader of the manifest of the backup being
// started, which is written under the configured path. An asynchronous start reads
// nothing, and gets nil.
func (s *Service) startedBackupMetadataLister(ctx context.Context) (metadataGetter, error) {
	if s.backupCfg.Start.Async {
		return nil, nil
	}

	return s.newMetadataLister(ctx, models.NormalizeS3Prefix(s.backupCfg.Start.Path))
}

// backupRequest maps the backup configuration onto the request the cluster is sent.
func (s *Service) backupRequest() (*infomodels.RequestBackup, error) {
	start := s.backupCfg.Start

	var mb, ma string

	if start.ModifiedBefore != "" {
		mbt, err := start.ModifiedBeforeTime()
		if err != nil {
			return nil, fmt.Errorf("failed to parse modified-before time: %w", err)
		}
		mb = strconv.FormatInt(mbt.Unix(), 10)
	}

	if start.ModifiedAfter != "" {
		mat, err := start.ModifiedAfterTime()
		if err != nil {
			return nil, fmt.Errorf("failed to parse modified-after time: %w", err)
		}
		ma = strconv.FormatInt(mat.Unix(), 10)
	}

	return &infomodels.RequestBackup{
		Namespace:      start.Namespace,
		Storage:        start.StorageType,
		Path:           models.NormalizeS3Prefix(start.Path),
		Bucket:         s.backupCfg.AwsS3.BucketName,
		Region:         s.backupCfg.AwsS3.Region,
		Profile:        s.backupCfg.AwsS3.Profile,
		Endpoint:       s.backupCfg.AwsS3.Endpoint,
		SetList:        start.SetList,
		FilterExp:      start.FilterExp,
		NoIndexes:      new(start.NoIndexes),
		NoUDFs:         new(start.NoUDFs),
		BinList:        start.BinList,
		ModifiedAfter:  ma,
		ModifiedBefore: mb,
	}, nil
}

// StartRestore checks that the configured backup exists and is complete, then starts
// restoring it as the configured restore job.
func (s *Service) StartRestore(ctx context.Context) error {
	client, err := s.newInfoClient(ctx)
	if err != nil {
		return err
	}
	defer client.Close()

	start := s.restoreCfg.Start

	if err := s.checkClusterStable(ctx, client, start.Namespace); err != nil {
		return err
	}

	// The backup is looked for before the restore is requested.
	s3Client, err := s.metadataS3Client(ctx, s.restoreCfg.AwsS3)
	if err != nil {
		return err
	}

	if err := s.checkBackupsExist(ctx, s3Client, s.restoreCfg.AwsS3.BucketName,
		models.NormalizeS3Prefix(start.Path), start.BackupIDList()); err != nil {
		return err
	}

	// A prepare run before this start has to name the same job, so the fallback is
	// reported for the user to see which id that is.
	if start.JobID == "" {
		s.logger.Info("job-id is not set, the backup id is used as the restore job id",
			slog.String("job-id", start.RestoreJobID()))
	}

	rReq := s.restoreRequest()

	err = client.StartRestore(ctx, rReq)
	if err != nil {
		return fmt.Errorf("failed to start restore: %w", err)
	}

	s.logger.Info("server integrated restore started",
		slog.String("job-id", rReq.JobID),
		slog.String("backup-ids", rReq.BackupIDs))

	return nil
}

// restoreRequest maps the restore configuration onto the request the cluster is sent.
func (s *Service) restoreRequest() *infomodels.RequestRestore {
	start := s.restoreCfg.Start

	return &infomodels.RequestRestore{
		Namespace:           start.Namespace,
		Storage:             start.StorageType,
		Path:                models.NormalizeS3Prefix(start.Path),
		Bucket:              s.restoreCfg.AwsS3.BucketName,
		Region:              s.restoreCfg.AwsS3.Region,
		Profile:             s.restoreCfg.AwsS3.Profile,
		Endpoint:            s.restoreCfg.AwsS3.Endpoint,
		SetList:             start.SetList,
		FilterExp:           start.FilterExp,
		NoIndexes:           new(start.NoIndexes),
		NoUDFs:              new(start.NoUDFs),
		JobID:               start.RestoreJobID(),
		BackupIDs:           start.BackupIDs,
		FuzzyRestore:        new(start.FuzzyRestore),
		AllowUnhosted:       new(start.AllowUnhosted),
		Parallel:            start.Parallel,
		RecordsPerSecond:    start.RecordsPerSecond,
		MaxInflight:         start.MaxInflight,
		RetryBaseIntervalMs: start.RetryBaseInterval,
		RetryMultiplier:     start.RetryMultiplier,
		RetryMaxAttempts:    start.RetryMaxAttempts,
		IgnoreRecordError:   new(start.IgnoreRecordError),
	}
}

// PrepareRestore initiates a restore preparation process for the specified job ID.
func (s *Service) PrepareRestore(ctx context.Context) error {
	client, err := s.newInfoClient(ctx)
	if err != nil {
		return err
	}
	defer client.Close()

	if err := s.checkClusterStable(ctx, client, s.restoreCfg.Prepare.Namespace); err != nil {
		return err
	}

	err = client.PrepareRestore(ctx, s.prepareRestoreRequest())
	if err != nil {
		return fmt.Errorf("failed to prepare restore: %w", err)
	}

	s.logger.Info("restore preparation started",
		slog.String("job-id", s.restoreCfg.Prepare.JobID))

	return nil
}

// prepareRestoreRequest maps the prepare configuration onto the request the cluster is sent.
func (s *Service) prepareRestoreRequest() *infomodels.RequestPrepareRestore {
	return &infomodels.RequestPrepareRestore{
		Namespace:      s.restoreCfg.Prepare.Namespace,
		JobID:          s.restoreCfg.Prepare.JobID,
		HydrateReplica: new(s.restoreCfg.Prepare.HydrateReplica),
	}
}

// BackupProgress returns the progress of the currently running backup.
func (s *Service) BackupProgress(ctx context.Context) error {
	client, err := s.newInfoClient(ctx)
	if err != nil {
		return err
	}
	defer client.Close()

	// The storage client is created before the watch loop on purpose: a wrong endpoint
	// or wrong credentials must fail now, not after hours of watching a backup.
	l, err := s.newMetadataLister(ctx, models.NormalizeS3Prefix(s.backupCfg.Progress.Path))
	if err != nil {
		return err
	}

	mode := reportOnce
	if s.backupCfg.Progress.Watch {
		mode = followJob
	}

	return s.reportBackupProgress(ctx, client, l, s.backupCfg.Progress.JobID, mode)
}

// newMetadataLister returns the reader of backup manifests stored under prefix in the
// configured bucket.
func (s *Service) newMetadataLister(ctx context.Context, prefix string) (metadataGetter, error) {
	s3Client, err := s.metadataS3Client(ctx, s.backupCfg.AwsS3)
	if err != nil {
		return nil, err
	}

	return newPrefixedMetadataGetter(s3Client, s.backupCfg.AwsS3.BucketName, prefix, s.logger), nil
}

// prefixedMetadataGetter reads the manifests of backups written under a key prefix.
// lister.Lister applies its prefix to listing only, so a single manifest is addressed
// by its full key.
type prefixedMetadataGetter struct {
	metadataGetter
	prefix string
}

// newPrefixedMetadataGetter returns the reader of manifests under prefix in bucket. The
// lister gets no prefix of its own, so that the prefix is applied in one place only.
func newPrefixedMetadataGetter(client s3API, bucket, prefix string, logger *slog.Logger) prefixedMetadataGetter {
	return prefixedMetadataGetter{
		metadataGetter: lister.NewLister(client, bucket, "", lister.WithLogger(logger)),
		prefix:         prefix,
	}
}

// GetMetadata reads the manifest of backupID under the prefix.
func (g prefixedMetadataGetter) GetMetadata(ctx context.Context, backupID string) (servermodels.Metadata, error) {
	return g.metadataGetter.GetMetadata(ctx, path.Join(g.prefix, backupID))
}

// progressMode says how long the watcher stays with a job, and how much it trusts the
// cluster not reporting the job at all.
type progressMode int

const (
	// reportOnce logs the status the cluster reports now and returns.
	reportOnce progressMode = iota + 1
	// followJob keeps reporting a job named on the command line until the backup reaches
	// a terminal state.
	followJob
	// followStartedJob does the same for the job the cluster has just accepted, and is
	// the only mode that waits for a job the status does not list yet.
	followStartedJob
)

// reportBackupProgress polls the cluster for the status of jobID and logs it until the
// backup reaches a terminal state, or once in reportOnce mode. The job is named by the
// caller rather than taken from the configuration, because the job that "backup start"
// follows is the one the cluster just handed it and not one named on the command line.
//
// A job named on the command line that the cluster does not report ends the watch right
// away: the id names no job the cluster holds, and there is nothing to wait for. The job
// "backup start" follows is different - the cluster handed out its id, so it does exist -
// and is given startedJobConfirmations lookups to turn up in the status.
//
// Only a job that was reported at least once can be missing for a reason worth waiting
// out - see jobVanished.
func (s *Service) reportBackupProgress(
	ctx context.Context,
	client backup.ServerBackupInfo,
	l metadataGetter,
	jobID string,
	mode progressMode,
) error {
	printer := newProgressPrinter(s.logger)

	// lastState is the state the cluster reported for this job most recently. It stays
	// empty while the job has never been observed, and it decides what a job that stops
	// reporting means.
	var lastState infomodels.BackupState
	// misses counts the lookups in a row that did not find the job.
	var misses int

	for {
		status, err := backupStatus(ctx, client, jobID)

		switch {
		case err != nil && !errors.Is(err, asinfo.ErrNotFound):
			return fmt.Errorf("failed to get backup status: %w", err)
		case err != nil:
			misses++

			switch {
			case lastState == "" && !startingJobPending(mode, misses):
				return fmt.Errorf("%w: backup-id %s", errBackupNotFound, jobID)
			case lastState != "" && jobVanished(lastState, misses):
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

		// The clock is read after the lookup so that the speed and the estimate are
		// measured against the status just read: a lookup that took seconds would
		// otherwise stretch the interval the sample is divided by.
		now := time.Now()

		printer.Print(status, now)

		// BackupStateAborting is not terminal: the job is still winding down and keeps
		// reporting until it reaches BackupStateAborted or leaves the cluster status.
		switch status.State {
		case infomodels.BackupStateComplete:
			s.logBackupComplete(status)

			return s.printMetadata(ctx, l, jobID)
		case infomodels.BackupStateFailed:
			return terminalStateError(errBackupFailed, jobID, status.ErrorReason)
		case infomodels.BackupStateAborted:
			return terminalStateError(errBackupAborted, jobID, status.ErrorReason)
		}

		if mode == reportOnce {
			return nil
		}

		if err := waitFor(ctx, nextPoll(status, now)); err != nil {
			return err
		}
	}
}

// logBackupComplete announces a backup the cluster reported as finished, and says how
// long the job took when the cluster reported both ends of it.
func (s *Service) logBackupComplete(status *infomodels.ResponseBackupState) {
	attrs := []any{slog.String("backup-id", status.JobID)}

	if duration, ok := backupDuration(status); ok {
		attrs = append(attrs, slog.String("duration", duration.Round(time.Second).String()))
	}

	s.logger.Info(msgBackupComplete, attrs...)
}

// terminalStateError names the job that ended in a terminal state, along with the reason
// the cluster gave for it when there was one.
func terminalStateError(err error, jobID, errorReason string) error {
	if errorReason == "" {
		return fmt.Errorf("%w: backup-id %s", err, jobID)
	}

	return fmt.Errorf("%w: backup-id %s, reason %q", err, jobID, errorReason)
}

// startingJobPending reports whether a job the cluster has not listed yet is still worth
// waiting for. Only the job the cluster has just handed out is: it exists, and the status
// it is missing from is the one that has to catch up. A job named on the command line is
// not, however many times it is looked up.
func startingJobPending(mode progressMode, misses int) bool {
	return mode == followStartedJob && misses < startedJobConfirmations
}

// jobVanished reports whether a job that was reported before and is missing now has been
// missing long enough to be accepted as gone. It has to be missing several times in a
// row, so that a momentary gap in the cluster status is not mistaken for a terminal
// outcome, and a job that was last seen before it reached its last stages gets the longer
// of the two waits: its status is the only thing that can end the watch.
func jobVanished(lastState infomodels.BackupState, misses int) bool {
	if completionStates[lastState] {
		return misses >= vanishedJobConfirmations
	}

	return misses >= midBackupGapConfirmations
}

// logStatusGap reports a job that is temporarily missing from the cluster status. A gap
// is a normal part of watching a backup - the status is unavailable between stages, or
// while a node is busy - so none of it reaches the user: both lines are logged on the
// debug level, where a gap that outlives the routine confirmations says so, and is
// repeated while it lasts.
func (s *Service) logStatusGap(jobID string, lastState infomodels.BackupState, misses int) {
	attrs := []any{
		slog.String("backup-id", jobID),
		slog.Int("attempt", misses),
	}

	if lastState != "" {
		attrs = append(attrs, slog.String("last-state", lastState.Describe()))
	}

	if misses == vanishedJobConfirmations || misses%statusGapLogEvery == 0 {
		s.logger.Debug("backup status is unavailable, still waiting for the job", attrs...)

		return
	}

	s.logger.Debug("backup status is temporarily unavailable", attrs...)
}

// backupStatus reads the status of jobID, and reports an answer that carries no status at
// all as a job the cluster does not report. The status is dereferenced by every caller,
// so the one place that can hand out a nil is here.
func backupStatus(
	ctx context.Context,
	client backup.ServerBackupInfo,
	jobID string,
) (*infomodels.ResponseBackupState, error) {
	status, err := client.GetBackupStatus(ctx, jobID)
	if err != nil {
		return nil, err
	}

	if status == nil {
		return nil, errEmptyBackupStatus
	}

	return status, nil
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

// reportMissingBackup handles a job that was being watched and that the cluster no longer
// reports, once the caller has confirmed that it is really gone. The cluster drops the
// state of a job shortly after finishing it, so a job last seen in one of the
// completionStates has completed, and its manifest is worth waiting for. Anything else
// stopped reporting in the middle of a backup, and only a manifest can turn that into a
// completion.
func (s *Service) reportMissingBackup(
	ctx context.Context,
	l metadataGetter,
	jobID string,
	lastState infomodels.BackupState,
) error {
	if completionStates[lastState] {
		s.logger.Info(msgBackupComplete)

		return s.printMetadata(ctx, l, jobID)
	}

	// The watch may have been between polls while the job ran through its last stages,
	// in which case the backup is done and the manifest is already in place. A missing
	// manifest means the backup did not finish, and reporting it as complete would hide
	// a job that died, was aborted, or fell out of the cluster status for good. Any
	// other failure is about the storage and is reported as itself.
	md, err := s.fetchMetadata(ctx, l, jobID, false)

	switch {
	case err == nil:
		s.logger.Info(msgBackupComplete)

		return s.printMetadataEntry(md)
	case errors.Is(err, errclass.ErrNotFound):
		return fmt.Errorf("%w: backup-id %s, last reported state %q",
			errBackupStatusLost, jobID, lastState.Describe())
	default:
		return err
	}
}

// printMetadata reads the manifest of a finished backup and prints it. The manifest is
// waited for: every caller has seen the job reach its end, and the server uploads the
// manifest shortly after that.
func (s *Service) printMetadata(ctx context.Context, l metadataGetter, jobID string) error {
	md, err := s.fetchMetadata(ctx, l, jobID, true)
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

// metadataS3Config returns a copy of cfg with a bounded request timeout, so that reading
// a manifest never hangs on the storage and never mutates the configuration shared with
// the rest of the command.
func metadataS3Config(cfg *models.AwsS3) *models.AwsS3 {
	bounded := *cfg
	bounded.RequestTimeout = metadataRequestTimeoutMillis

	return &bounded
}

// RestoreProgress returns the progress of the currently running restore.
func (s *Service) RestoreProgress(ctx context.Context) error {
	client, err := s.newInfoClient(ctx)
	if err != nil {
		return err
	}
	defer client.Close()

	result, err := client.GetRestoreStatus(ctx, s.restoreCfg.Progress.Namespace)
	if err != nil {
		return fmt.Errorf("failed to get restore status: %w", err)
	}

	s.logger.Info("restore progress",
		slog.String("result", result))

	return nil
}

// BackupValidate validates the backup identified by the configured job ID, read from
// under the configured path.
func (s *Service) BackupValidate(ctx context.Context) error {
	prefix := models.NormalizeS3Prefix(s.backupCfg.Validation.Path)

	// The manifest is read with the bounded manifest timeout, so that an unreachable
	// storage fails the command right away. Validation itself streams whole segments
	// and gets a client without that timeout.
	metaClient, err := s.metadataS3Client(ctx, s.backupCfg.AwsS3)
	if err != nil {
		return err
	}

	if err := s.checkBackupExists(ctx, metaClient, s.backupCfg.AwsS3.BucketName, prefix,
		s.backupCfg.Validation.JobID); err != nil {
		return err
	}

	client, err := storage.NewS3Client(ctx, s.backupCfg.AwsS3)
	if err != nil {
		return fmt.Errorf("failed to create s3 client: %w", err)
	}

	// The streamer addresses a backup by a single key, so the path it was written under
	// is part of that key.
	streamer, err := streamers.NewS3(
		client,
		s.backupCfg.AwsS3.BucketName,
		path.Join(prefix, s.backupCfg.Validation.JobID),
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

// checkBackupsExist validates that every backup of backupIDs, written under prefix,
// exists and is complete. The metadata of a backup is addressed by its own id, so the
// backups are looked for one by one, and a single missing or incomplete one stops the
// restore before any of them is read.
func (s *Service) checkBackupsExist(
	ctx context.Context, client s3API, bucket, prefix string, backupIDs []string,
) error {
	for _, backupID := range backupIDs {
		if err := s.checkBackupExists(ctx, client, bucket, prefix, backupID); err != nil {
			return err
		}
	}

	return nil
}

// checkBackupExists validates that the backup backupID, written under prefix, exists and
// is complete.
func (s *Service) checkBackupExists(ctx context.Context, client s3API, bucket, prefix, backupID string) error {
	md, err := newPrefixedMetadataGetter(client, bucket, prefix, s.logger).GetMetadata(ctx, backupID)
	if err != nil {
		return fmt.Errorf("failed to check if backup exists: %w", err)
	}

	if md.Status != servermodels.MetadataStatusComplete {
		return fmt.Errorf("backup %s is not complete, has status %s", backupID, md.Status)
	}

	s.logger.Info("backup found",
		slog.String("backup-id", md.BackupID),
		slog.String("namespace", md.Namespace),
	)

	return nil
}

// abortOutcome is what a status read while waiting for an abort says about the job.
type abortOutcome int

const (
	// abortPending means the job is still winding down, and the wait goes on.
	abortPending abortOutcome = iota + 1
	// abortStopped means the job is no longer running: the abort has landed.
	abortStopped
	// abortTooLate means the job reached the end of its work before the abort did, so
	// there was nothing left to stop.
	abortTooLate
)

// waitForAbort polls the state of an aborted job until it stops running, and reports how
// it stopped. The cluster accepts an abort request before the job actually winds down, so
// the abort is confirmed by the status that follows it.
//
// The wait is bounded: a job that never reacts to the abort has to end the command instead
// of being polled forever, and the deadline also cuts short a status request that is
// already in flight. timeoutErr is what a job that kept reporting itself as running until
// the wait ran out is reported as; poll returns the state it read along with its meaning,
// so that the failure can name what the job was last doing.
func (s *Service) waitForAbort(
	ctx context.Context,
	timeoutErr error,
	poll func(ctx context.Context) (abortOutcome, string, error),
) (abortOutcome, error) {
	statusCtx, cancel := context.WithTimeout(ctx, s.abortTimeout)
	defer cancel()

	for {
		outcome, state, err := poll(statusCtx)

		// The terminal outcomes are named rather than taken as "anything but pending", so
		// that an outcome no poll is supposed to return ends in the bounded wait running
		// out instead of announcing a job that never stopped as stopped.
		switch {
		case errors.Is(err, context.DeadlineExceeded):
			return 0, timeoutErr
		case err != nil:
			return 0, err
		case outcome == abortStopped, outcome == abortTooLate:
			return outcome, nil
		}

		// waitFor returns as soon as the command is canceled or the wait runs out, so
		// the abort never keeps polling a job the caller stopped waiting for.
		if err := waitFor(statusCtx, s.abortPollInterval); err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				return 0, fmt.Errorf("%w, last reported state %q", timeoutErr, state)
			}

			return 0, err
		}
	}
}

// AbortBackup asks the cluster to abort the configured backup job, once it has confirmed
// there is a running job to abort, and waits for that job to stop.
func (s *Service) AbortBackup(ctx context.Context) error {
	client, err := s.newInfoClient(ctx)
	if err != nil {
		return err
	}
	defer client.Close()

	return s.abortBackup(ctx, client)
}

// abortBackup requests the abort and then confirms it through the job status.
func (s *Service) abortBackup(ctx context.Context, client backup.ServerBackupInfo) error {
	jobID := s.backupCfg.Abort.JobID

	running, err := s.backupIsRunning(ctx, client, jobID)
	if err != nil {
		return err
	}

	if !running {
		return nil
	}

	// The error the cluster answers with already names the failed abort, so it is
	// returned as it is: wrapping it here only repeats what the command line prints.
	if err := client.AbortBackup(ctx, jobID); err != nil {
		return err
	}

	s.logger.Info("aborting",
		slog.String("backup-id", jobID))

	outcome, err := s.waitForAbort(ctx,
		fmt.Errorf("%w: backup-id %s", errAbortStatusTimeout, jobID),
		func(ctx context.Context) (abortOutcome, string, error) {
			return backupAbortState(ctx, client, jobID)
		})
	if err != nil {
		return err
	}

	// A job that reached the end of the backup before the abort landed was not aborted:
	// its data is in storage, and announcing it as aborted would hide a backup that
	// finished.
	if outcome == abortTooLate {
		s.logger.Info("backup finished before it could be aborted",
			slog.String("backup-id", jobID))

		return nil
	}

	s.logger.Info("backup was successfully aborted")

	return nil
}

// backupAbortState reads the state of a job that was asked to stop, and says what it
// means for the abort.
func backupAbortState(
	ctx context.Context,
	client backup.ServerBackupInfo,
	jobID string,
) (abortOutcome, string, error) {
	status, err := backupStatus(ctx, client, jobID)
	if err != nil {
		// The cluster drops the state of a job that is no longer running.
		if errors.Is(err, asinfo.ErrNotFound) {
			return abortStopped, "", nil
		}

		return 0, "", fmt.Errorf("failed to get backup status: %w", err)
	}

	switch status.State {
	// The job is done winding down once it reports the abort. A job that hit a failure
	// on the way out reports that instead, and is just as stopped.
	case infomodels.BackupStateAborted, infomodels.BackupStateFailed:
		return abortStopped, status.State.Describe(), nil
	case infomodels.BackupStateComplete:
		return abortTooLate, status.State.Describe(), nil
	default:
		return abortPending, status.State.Describe(), nil
	}
}

// backupIsRunning reports whether the cluster is working on jobID, so that an abort is
// only requested when there is a job to abort. The cluster accepts an abort for a job it
// knows nothing about without complaining, and the status that follows such a request
// looks exactly like the status of a job that stopped on request - which would announce a
// backup that never ran, or one that ended hours ago, as successfully aborted.
func (s *Service) backupIsRunning(
	ctx context.Context,
	client backup.ServerBackupInfo,
	jobID string,
) (bool, error) {
	status, err := backupStatus(ctx, client, jobID)
	if err != nil {
		// The cluster reports only the jobs it still holds the state of, so an unknown
		// job id and a backup that finished long ago are answered the same way.
		if errors.Is(err, asinfo.ErrNotFound) {
			s.logger.Info(msgNoRunningBackup, slog.String("backup-id", jobID))

			return false, nil
		}

		return false, fmt.Errorf("failed to get backup status: %w", err)
	}

	// A job that already reached its end is still reported for a while. Naming the state
	// it ended in says why the abort did nothing.
	if terminalBackupStates[status.State] {
		s.logger.Info(msgNoRunningBackup,
			slog.String("backup-id", jobID),
			slog.String("state", status.State.Describe()))

		return false, nil
	}

	return true, nil
}

// AbortRestore asks the cluster to abort the configured restore job, once it has
// confirmed the namespace holds a restore to abort, and waits for the namespace to report
// that it holds one no longer.
func (s *Service) AbortRestore(ctx context.Context) error {
	client, err := s.newInfoClient(ctx)
	if err != nil {
		return err
	}
	defer client.Close()

	return s.abortRestore(ctx, client)
}

// abortRestore checks that the namespace holds a restore to abort, requests the abort,
// and then confirms it through the restore status.
//
// A restore job is addressed by its namespace as well as by its id, but the status the
// cluster answers with is per namespace rather than per job: it says what the namespace
// is doing, not which job is doing it. Neither the check before the abort nor the
// confirmation after it can therefore tell the requested job apart from another restore
// into the same namespace, and a job id that names neither ends the command with a
// timeout instead of a success.
func (s *Service) abortRestore(ctx context.Context, client backup.ServerBackupInfo) error {
	namespace, jobID := s.restoreCfg.Abort.Namespace, s.restoreCfg.Abort.JobID

	active, err := s.restoreIsActive(ctx, client, namespace)
	if err != nil {
		return err
	}

	if !active {
		return nil
	}

	// The error the cluster answers with already names the failed abort, so it is
	// returned as it is: wrapping it here only repeats what the command line prints.
	if err := client.AbortRestore(ctx, namespace, jobID); err != nil {
		return err
	}

	s.logger.Info("aborting restore",
		slog.String("namespace", namespace),
		slog.String("job-id", jobID))

	if _, err := s.waitForAbort(ctx,
		fmt.Errorf("%w: namespace %s, job-id %s", errAbortRestoreStatusTimeout, namespace, jobID),
		func(ctx context.Context) (abortOutcome, string, error) {
			return restoreAbortState(ctx, client, namespace)
		}); err != nil {
		return err
	}

	s.logger.Info("restore was successfully aborted")

	return nil
}

// restoreAbortState reads the state of a namespace whose restore was asked to stop, and
// says what it means for the abort. A restore that is no longer held by the namespace has
// stopped, however it ended: the namespace is what the state describes, so there is no
// state of its own for a restore that reached the end before the abort landed.
func restoreAbortState(
	ctx context.Context,
	client backup.ServerBackupInfo,
	namespace string,
) (abortOutcome, string, error) {
	state, err := client.GetRestoreStatus(ctx, namespace)
	if err != nil {
		// The cluster reports no restore state for a namespace it holds nothing for.
		if errors.Is(err, asinfo.ErrNotFound) {
			return abortStopped, "", nil
		}

		return 0, "", fmt.Errorf("failed to get restore status: %w", err)
	}

	if !restoreActiveStates[state] {
		return abortStopped, state, nil
	}

	return abortPending, state, nil
}

// restoreIsActive reports whether namespace holds a restore the abort can act on, so that
// an abort is only requested when there is something to abort. The cluster accepts the
// abort of an idle namespace, and the status that follows such a request cannot be told
// from a restore that stopped on request - which announces a restore that never ran as
// aborted.
func (s *Service) restoreIsActive(
	ctx context.Context,
	client backup.ServerBackupInfo,
	namespace string,
) (bool, error) {
	state, err := client.GetRestoreStatus(ctx, namespace)
	if err != nil {
		// A namespace no restore has ever touched carries no state at all.
		if errors.Is(err, asinfo.ErrNotFound) {
			s.logger.Info(msgNoRunningRestore, slog.String("namespace", namespace))

			return false, nil
		}

		return false, fmt.Errorf("failed to get restore status: %w", err)
	}

	if !restoreActiveStates[state] {
		s.logger.Info(msgNoRunningRestore,
			slog.String("namespace", namespace),
			slog.String("state", state))

		return false, nil
	}

	return true, nil
}
