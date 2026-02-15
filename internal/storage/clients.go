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

package storage

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"time"

	gcpStorage "cloud.google.com/go/storage"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	appConfig "github.com/aerospike/absctl/internal/config"
	"github.com/aerospike/absctl/internal/models"
	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/backup-go"
	"github.com/aerospike/tools-common-go/client"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/googleapis/gax-go/v2"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"
)

// NewAerospikeClient initializes and returns a new Aerospike client with the specified configuration and settings.
// It validates input parameters, applies client policies, and optionally warms up the client for better performance.
// Returns an Aerospike client instance or an error if initialization fails.
func NewAerospikeClient(
	ctx context.Context,
	cfg *client.AerospikeConfig,
	cp *models.ClientPolicy,
	racks string,
	warmUp int,
	logger *slog.Logger,
	sa *backup.SecretAgentConfig,
) (*aerospike.Client, error) {
	if len(cfg.Seeds) < 1 {
		return nil, fmt.Errorf("at least one seed must be provided")
	}

	logger.Info("initializing Aerospike client",
		slog.String("seeds", cfg.Seeds.String()),
	)

	if sa != nil {
		var err error

		cfg.User, err = backup.ParseSecret(ctx, sa, cfg.User)
		if err != nil {
			return nil, fmt.Errorf("failed to parse secret for user: %w", err)
		}

		cfg.Password, err = backup.ParseSecret(ctx, sa, cfg.Password)
		if err != nil {
			return nil, fmt.Errorf("failed to parse secret for password: %w", err)
		}
	}

	p, err := cfg.NewClientPolicy()
	if err != nil {
		return nil, fmt.Errorf("failed to create Aerospike client policy: %w", err)
	}

	p.Timeout = time.Duration(cp.Timeout) * time.Millisecond
	p.IdleTimeout = time.Duration(cp.IdleTimeout) * time.Millisecond
	p.LoginTimeout = time.Duration(cp.LoginTimeout) * time.Millisecond

	if racks != "" {
		racksIDs, err := appConfig.ParseRacks(racks)
		if err != nil {
			return nil, err
		}

		p.RackIds = racksIDs
		p.RackAware = true
	}

	asClient, err := aerospike.NewClientWithPolicyAndHost(p, toHosts(cfg.Seeds)...)
	if err != nil {
		return nil, fmt.Errorf("failed to create Aerospike client: %w", err)
	}

	if warmUp > 0 {
		_, err = asClient.WarmUp(warmUp)
		if err != nil {
			return nil, fmt.Errorf("failed to warm up Aerospike client: %w", err)
		}
	}

	return asClient, nil
}

func newS3Client(ctx context.Context, a *models.AwsS3) (*s3.Client, error) {
	cfgOpts := make([]func(*config.LoadOptions) error, 0)

	// use an adaptive mode for more aggressive retries
	cfgOpts = append(cfgOpts,
		config.WithRetryer(func() aws.Retryer {
			return retry.NewAdaptiveMode(func(o *retry.AdaptiveModeOptions) {
				o.StandardOptions = append(o.StandardOptions,
					func(so *retry.StandardOptions) {
						so.MaxAttempts = a.RetryMaxAttempts
						so.MaxBackoff = time.Duration(a.RetryMaxBackoff) * time.Millisecond
						so.Backoff = retry.NewExponentialJitterBackoff(time.Duration(a.RetryMaxBackoff) * time.Millisecond)
					})
			})
		}),
		config.WithHTTPClient(
			newHTTPClient(newTransport(a.MaxConnsPerHost), a.RequestTimeout)),
	)

	if a.Profile != "" {
		cfgOpts = append(cfgOpts, config.WithSharedConfigProfile(a.Profile))
	}

	if a.Region != "" {
		cfgOpts = append(cfgOpts, config.WithRegion(a.Region))
	}

	if a.AccessKeyID != "" && a.SecretAccessKey != "" {
		cfgOpts = append(cfgOpts, config.WithCredentialsProvider(credentials.StaticCredentialsProvider{
			Value: aws.Credentials{
				AccessKeyID: a.AccessKeyID, SecretAccessKey: a.SecretAccessKey,
			},
		}))
	}

	cfg, err := config.LoadDefaultConfig(ctx, cfgOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		if a.Endpoint != "" {
			o.BaseEndpoint = &a.Endpoint
		}

		o.UsePathStyle = true
		o.DisableLogOutputChecksumValidationSkipped = true
	})

	return s3Client, nil
}

func newGcpClient(ctx context.Context, g *models.GcpStorage) (*gcpStorage.Client, error) {
	opts := make([]option.ClientOption, 0)

	if g.Endpoint != "" {
		// Used with fake-gcs for tests only.
		opts = append(opts, option.WithEndpoint(g.Endpoint), option.WithoutAuthentication())
	} else {
		transport, err := getGcpTransport(ctx, g)
		if err != nil {
			return nil, fmt.Errorf("failed to get GCP transport: %w", err)
		}

		opts = append(opts, option.WithHTTPClient(newHTTPClient(transport, g.RequestTimeout)))
	}

	gcpClient, err := gcpStorage.NewClient(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCP client: %w", err)
	}

	backoff := gax.Backoff{
		Initial:    time.Duration(g.RetryBackoffInit) * time.Millisecond,
		Max:        time.Duration(g.RetryBackoffMax) * time.Millisecond,
		Multiplier: g.RetryBackoffMultiplier,
	}

	gcpClient.SetRetry(
		gcpStorage.WithPolicy(gcpStorage.RetryAlways),
		gcpStorage.WithBackoff(backoff),
		gcpStorage.WithMaxAttempts(g.RetryMaxAttempts))

	return gcpClient, nil
}

func getGcpTransport(ctx context.Context, g *models.GcpStorage) (http.RoundTripper, error) {
	var (
		transport = newTransport(g.MaxConnsPerHost)
		ts        oauth2.TokenSource
		err       error
	)
	// GCP can't apply option.WithCredentialsFile() with custom http client option.WithHTTPClient().
	// So we implement our own logic to load auth key and set http headers.
	if g.KeyFile != "" {
		creds, err := getGcpAuth(ctx, g.KeyFile)
		if err != nil {
			return nil, err
		}
		// Use client with custom auth.
		ts = creds.TokenSource
	} else {
		// ADC: uses attached VM service account on GCE (or GOOGLE_APPLICATION_CREDENTIALS, etc.)
		ts, err = google.DefaultTokenSource(ctx, gcpStorage.ScopeReadWrite)
		if err != nil {
			return nil, fmt.Errorf("failed to get ADC token source: %w", err)
		}
	}

	return newAuthTransport(transport, ts), nil
}

// getGcpAuth read and load auth key from a file for GCP.
func getGcpAuth(ctx context.Context, keyFile string) (*google.Credentials, error) {
	jsonKey, err := os.ReadFile(keyFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read key file %s: %w", keyFile, err)
	}

	creds, err := google.CredentialsFromJSON(ctx, jsonKey,
		gcpStorage.ScopeReadWrite,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to parse JSON key file %s: %w", keyFile, err)
	}

	return creds, nil
}

func newAzureClient(a *models.AzureBlob) (*azblob.Client, error) {
	var (
		azClient *azblob.Client
		err      error
	)

	azOpts := &azblob.ClientOptions{
		ClientOptions: azcore.ClientOptions{
			Transport: newHTTPClient(newTransport(a.MaxConnsPerHost), a.RequestTimeout),
			Retry: policy.RetryOptions{
				MaxRetries:    int32(a.RetryMaxAttempts),
				RetryDelay:    time.Duration(a.RetryDelay) * time.Millisecond,
				MaxRetryDelay: time.Duration(a.RetryMaxDelay) * time.Millisecond,
				StatusCodes: []int{
					http.StatusRequestTimeout,
					http.StatusTooManyRequests,
					http.StatusInternalServerError,
					http.StatusBadGateway,
					http.StatusServiceUnavailable,
					http.StatusGatewayTimeout,
				},
			},
		},
	}

	switch {
	case a.AccountName != "" && a.AccountKey != "":
		cred, err := azblob.NewSharedKeyCredential(a.AccountName, a.AccountKey)
		if err != nil {
			return nil, fmt.Errorf("failed to create Azure shared key credentials: %w", err)
		}

		azClient, err = azblob.NewClientWithSharedKeyCredential(a.Endpoint, cred, azOpts)
		if err != nil {
			return nil, fmt.Errorf("failed to create Azure Blob client with shared key: %w", err)
		}
	case a.TenantID != "" && a.ClientID != "" && a.ClientSecret != "":
		cred, err := azidentity.NewClientSecretCredential(a.TenantID, a.ClientID, a.ClientSecret, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create Azure AAD credentials: %w", err)
		}

		azClient, err = azblob.NewClient(a.Endpoint, cred, azOpts)
		if err != nil {
			return nil, fmt.Errorf("failed to create Azure Blob client with AAD: %w", err)
		}
	default:
		azClient, err = azblob.NewClientWithNoCredential(a.Endpoint, azOpts)
		if err != nil {
			return nil, fmt.Errorf("failed to create Azure Blob client with SAS: %w", err)
		}
	}

	return azClient, nil
}

func toHosts(htpSlice client.HostTLSPortSlice) []*aerospike.Host {
	hosts := make([]*aerospike.Host, len(htpSlice))
	for i, htp := range htpSlice {
		hosts[i] = &aerospike.Host{
			Name:    htp.Host,
			TLSName: htp.TLSName,
			Port:    htp.Port,
		}
	}

	return hosts
}

// newTransport returns a new http.Transport.
func newTransport(maxConnsPerHost int) *http.Transport {
	return &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxConnsPerHost:     maxConnsPerHost,
		IdleConnTimeout:     120 * time.Second,
		TLSHandshakeTimeout: 10 * time.Second,
		ReadBufferSize:      64 * 1024,
		ForceAttemptHTTP2:   true,
	}
}

// newAuthTransport returns transport with auth.
// It is used only for GCP, because it can't pass auth to custom http.Client.
func newAuthTransport(baseTransport http.RoundTripper, tokenSource oauth2.TokenSource) *oauth2.Transport {
	return &oauth2.Transport{
		Base:   baseTransport,
		Source: tokenSource,
	}
}

// newHTTPClient returns a new http.Client.
func newHTTPClient(transport http.RoundTripper, requestTimeoutSeconds int) *http.Client {
	return &http.Client{
		Transport: transport,
		Timeout:   time.Duration(requestTimeoutSeconds) * time.Millisecond,
	}
}
