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
	"os"
	"text/tabwriter"
	"time"

	"github.com/aerospike/absctl/internal/config"
	"github.com/aerospike/absctl/internal/lister"
	"github.com/aerospike/absctl/internal/storage"
	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/backup-go"
	"github.com/aerospike/backup-go/models"
	"github.com/aerospike/backup-go/pkg/asinfo"
)

// Service represents a server side backup and restore service.
type Service struct {
	config *config.SSBServiceConfig

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
	var (
		reader backup.StreamingReader
		err    error
	)

	if cfg.SSb.List != "" {
		reader, err = storage.NewReader(
			ctx,
			&cfg.ServiceConfigCommon,
			cfg.SSb.List,
			"",
			"",
			"",
			0,
			false,
			true,
			logger,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create reader: %w", err)
		}
	}

	return &Service{
		config: cfg,
		reader: reader,
		logger: logger,
	}, nil
}

func (s *Service) Run(ctx context.Context) error {
	switch {
	case s.config.SSb.List != "":
		return s.listBackups(ctx)
	case s.config.SSb.Backup:
		return s.startBackup(ctx)
	case s.config.SSb.Restore:
		return s.startRestore(ctx, s.config.SSb.JobID)
	default:
		return fmt.Errorf("invalid command")
	}
}

func (s *Service) listBackups(ctx context.Context) error {
	l := lister.NewLister(s.reader, &lister.MetafileParserSSb{})

	if s.config.SSb.List == "/" || s.config.SSb.List == "\\" {
		s.config.SSb.List = ""
	}

	backups, err := l.ListSSB(ctx, s.config.SSb.List)
	if err != nil {
		return fmt.Errorf("failed to list backups: %w", err)
	}

	printBackupEntriesForSSB(backups)

	return nil
}

func (s *Service) startBackup(ctx context.Context) error {
	client, err := s.newInfoClient()
	if err != nil {
		return err
	}

	jobID := time.Now().UnixMilli()

	err = client.StartBackup(
		ctx,
		jobID,
		s.config.SSb.Namespace,
		s.config.SSb.StorageType,
		s.config.AwsS3.BucketName,
		s.config.AwsS3.Region,
		s.config.AwsS3.Profile,
		s.config.AwsS3.AccessKeyID,
		s.config.AwsS3.SecretAccessKey,
	)
	if err != nil {
		return fmt.Errorf("failed to start backup: %w", err)
	}

	return nil
}

func (s *Service) startRestore(ctx context.Context, jobID int64) error {
	client, err := s.newInfoClient()
	if err != nil {
		return err
	}

	err = client.StartRestore(
		ctx,
		jobID,
		s.config.SSb.Namespace,
		s.config.SSb.StorageType,
		s.config.AwsS3.BucketName,
		s.config.AwsS3.Region,
		s.config.AwsS3.Profile,
		s.config.AwsS3.AccessKeyID,
		s.config.AwsS3.SecretAccessKey,
	)
	if err != nil {
		return fmt.Errorf("failed to start restore: %w", err)
	}

	return nil
}

// newInfoClient separate function for lazy load.
func (s *Service) newInfoClient() (*asinfo.Client, error) {
	aerospikeClient, err := storage.NewAerospikeClient(
		s.config.ClientConfig,
		s.config.ClientPolicy,
		nil,
		0,
		s.logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create aerospike client: %w", err)
	}

	infoClient, err := asinfo.NewClient(
		aerospikeClient.Cluster(),
		aerospike.NewInfoPolicy(),
		models.NewDefaultRetryPolicy(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create info client: %w", err)
	}

	return infoClient, nil
}

//nolint:unused // Function for testing concept.
func printBackupEntriesForABS(backups []*lister.BackupEntry) {
	// Initialize tabwriter
	// minwidth, tabwidth, padding, padchar, flags
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)

	// Print the Header
	fmt.Fprintln(w, "NAMESPACE\tCREATED\tDURATION\tRECORDS\tSIZE\tFILES\tCOMPRESSION")

	for _, b := range backups {
		// Calculate duration if finished time exists
		duration := "N/A"
		if !b.Finished.IsZero() {
			duration = b.Finished.Sub(b.Created).Round(time.Second).String()
		}

		// Format size for readability (Optional: could add a helper for MB/GB)
		sizeStr := fmt.Sprintf("%d B", b.ByteCount)
		if b.ByteCount > 1024*1024 {
			sizeStr = fmt.Sprintf("%.2f MB", float64(b.ByteCount)/(1024*1024))
		}

		// Use \t to separate columns
		fmt.Fprintf(w, "%s\t%s\t%s\t%d\t%s\t%d\t%s\n",
			b.BackupMetadata.Namespace,
			b.Created.Format("2006-01-02 15:04:05"),
			duration,
			b.RecordCount,
			sizeStr,
			b.FileCount,
			b.Compression,
		)
	}

	// Flush the writer to output the buffered data
	w.Flush()
}

func printBackupEntriesForSSB(backups []*lister.BackupEntry) {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)

	// Header (adapted to Root fields)
	fmt.Fprintln(w, "NAMESPACE\tBACKUP_ID\tNODE\tFORMAT\tPARTITIONS")

	for _, b := range backups {
		// Skip entries without Root (safety)
		if b.Root == nil {
			continue
		}

		fmt.Fprintf(w, "%s\t%s\t%s\t%d\t%d\n",
			b.Root.Namespace,
			b.BackupID,
			b.NodeID,
			b.FormatVersion,
			len(b.Partitions),
		)
	}

	w.Flush()
}
