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
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/aerospike/absctl/internal/models"
	"github.com/aerospike/aerospike-client-go/v8"
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
//
// The caller owns the returned client and must close it. On error no client is left open.
//
// The Aerospike client cannot cancel a connect in progress, so if ctx is done first,
// NewAerospikeClient returns at once with the context error and the connect keeps running
// in the background until it succeeds or the client's own timeouts end it; the client
// it produces, if any, is closed there.
func NewAerospikeClient(
	ctx context.Context,
	cfg *client.AerospikeConfig,
	cp *models.ClientPolicy,
	racksIDs []int,
	warmUp int,
	logger *slog.Logger,
) (*aerospike.Client, error) {
	if len(cfg.Seeds) < 1 {
		return nil, fmt.Errorf("at least one seed must be provided")
	}

	logger.Info("initializing Aerospike client",
		slog.String("seeds", cfg.Seeds.String()),
	)

	p, err := cfg.NewClientPolicy()
	if err != nil {
		return nil, fmt.Errorf("failed to create Aerospike client policy: %w", err)
	}

	p.Timeout = time.Duration(cp.Timeout) * time.Millisecond
	p.IdleTimeout = time.Duration(cp.IdleTimeout) * time.Millisecond
	p.LoginTimeout = time.Duration(cp.LoginTimeout) * time.Millisecond

	if len(racksIDs) > 0 {
		p.RackIds = racksIDs
		p.RackAware = true
	}

	hosts := toHosts(cfg.Seeds)

	return connectWithContext(
		ctx,
		func() (*aerospike.Client, error) {
			return connectAerospikeClient(p, hosts, warmUp)
		},
		(*aerospike.Client).Close,
	)
}

// connectWithContext runs connect in the background and returns its result, unless
// ctx is done first: then it returns the bare context error at once, so that callers
// wrap it into a short message, and release is called on the value connect produces
// later, if it succeeds. connect must leave nothing to release when it fails.
func connectWithContext[T any](
	ctx context.Context,
	connect func() (T, error),
	release func(T),
) (T, error) {
	var zero T

	if err := ctx.Err(); err != nil {
		return zero, err
	}

	// Unbuffered on purpose: a result is handed over only while the caller is still
	// waiting for it. Once the caller has given up on ctx, the send can never happen,
	// so the connecting goroutine knows the value is its own to release.
	results := make(chan connectResult[T])

	go func() {
		var res connectResult[T]

		res.value, res.err = connect()

		select {
		case results <- res:
		case <-ctx.Done():
			if res.err == nil {
				release(res.value)
			}
		}
	}()

	select {
	case res := <-results:
		return res.value, res.err
	case <-ctx.Done():
		return zero, ctx.Err()
	}
}

// connectResult is the outcome of a connect handed from the connecting goroutine
// to connectWithContext.
type connectResult[T any] struct {
	value T
	err   error
}

// connectAerospikeClient connects to the cluster and warms up the connection pool
// when warmUp is positive. It returns once the client is ready or the attempt fails,
// at the latest when the client's own timeouts end it. On error no client is left open.
func connectAerospikeClient(
	p *aerospike.ClientPolicy,
	hosts []*aerospike.Host,
	warmUp int,
) (*aerospike.Client, error) {
	asClient, err := aerospike.NewClientWithPolicyAndHost(p, hosts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create Aerospike client: %w", err)
	}

	if warmUp > 0 {
		if _, err = asClient.WarmUp(warmUp); err != nil {
			asClient.Close()

			return nil, fmt.Errorf("failed to warm up Aerospike client: %w", err)
		}
	}

	return asClient, nil
}

// NewS3Client initializes and returns a new S3 client with the specified configuration.
func NewS3Client(ctx context.Context, a *models.AwsS3) (*s3.Client, error) {
	s3Client, _, err := newS3Client(ctx, a)

	return s3Client, err
}

// newS3Client is NewS3Client that also returns the transport of the client,
// so that the caller can close its idle connections when done with the client.
func newS3Client(ctx context.Context, a *models.AwsS3) (*s3.Client, *http.Transport, error) {
	transport := newTransport(a.MaxConnsPerHost)

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
			newHTTPClient(transport, a.RequestTimeout)),
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
		return nil, nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		if a.Endpoint != "" {
			o.BaseEndpoint = &a.Endpoint
		}

		o.UsePathStyle = true
		o.DisableLogOutputChecksumValidationSkipped = true
	})

	return s3Client, transport, nil
}

// newGcpClient returns a GCP storage client and the transport it was built on. Closing
// the client does not reach the idle connections of that transport through the wrappers
// around it, so the caller closes them as well.
func newGcpClient(ctx context.Context, g *models.GcpStorage) (*gcpStorage.Client, *http.Transport, error) {
	transport := newTransport(g.MaxConnsPerHost)

	var opts []option.ClientOption

	if g.Endpoint != "" {
		// An endpoint override sends unauthenticated requests. WithoutAuthentication is
		// kept next to the HTTP client so that creating the client never looks up
		// credentials in the environment.
		opts = append(opts,
			option.WithEndpoint(g.Endpoint),
			option.WithoutAuthentication(),
			option.WithHTTPClient(newHTTPClient(transport, g.RequestTimeout)),
		)
	} else {
		authTransport, err := newGcpAuthTransport(ctx, g, transport)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to get GCP transport: %w", err)
		}

		opts = append(opts, option.WithHTTPClient(newHTTPClient(authTransport, g.RequestTimeout)))
	}

	gcpClient, err := gcpStorage.NewClient(ctx, opts...)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create GCP client: %w", err)
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

	return gcpClient, transport, nil
}

// newGcpAuthTransport wraps transport with GCP authentication. Tokens read from a key
// file or a credentials file are fetched through transport as well, so that closing
// its idle connections releases them too. Tokens from the GCE metadata server are not:
// that source always uses the shared client of the metadata package.
func newGcpAuthTransport(
	ctx context.Context,
	g *models.GcpStorage,
	transport *http.Transport,
) (http.RoundTripper, error) {
	var (
		ts  oauth2.TokenSource
		err error
	)

	// The token sources backed by a file send their requests through the HTTP client
	// found in ctx, or through http.DefaultClient without one.
	ctx = context.WithValue(ctx, oauth2.HTTPClient, newHTTPClient(transport, g.RequestTimeout))
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

	creds, err := google.CredentialsFromJSONWithType(
		ctx,
		jsonKey,
		google.ServiceAccount,
		gcpStorage.ScopeReadWrite,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to parse JSON key file %s: %w", keyFile, err)
	}

	return creds, nil
}

// newAzureClient returns an Azure Blob client and the transport it was built on,
// so that the caller can close its idle connections when done with the client.
func newAzureClient(a *models.AzureBlob) (*azblob.Client, *http.Transport, error) {
	var (
		azClient   *azblob.Client
		err        error
		transport  = newTransport(a.MaxConnsPerHost)
		httpClient = newHTTPClient(transport, a.RequestTimeout)
	)

	azOpts := &azblob.ClientOptions{
		Transport: httpClient,
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
	}

	switch {
	case a.AccountName != "" && a.AccountKey != "":
		cred, err := azblob.NewSharedKeyCredential(a.AccountName, a.AccountKey)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create Azure shared key credentials: %w", err)
		}

		azClient, err = azblob.NewClientWithSharedKeyCredential(a.Endpoint, cred, azOpts)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create Azure Blob client with shared key: %w", err)
		}
	case a.TenantID != "" && a.ClientID != "" && a.ClientSecret != "":
		// Tokens are fetched through the same transport as the data, so that closing
		// its idle connections releases them too.
		cred, err := azidentity.NewClientSecretCredential(a.TenantID, a.ClientID, a.ClientSecret,
			&azidentity.ClientSecretCredentialOptions{Transport: httpClient})
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create Azure AAD credentials: %w", err)
		}

		azClient, err = azblob.NewClient(a.Endpoint, cred, azOpts)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create Azure Blob client with AAD: %w", err)
		}
	default:
		azClient, err = azblob.NewClientWithNoCredential(a.Endpoint, azOpts)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create Azure Blob client with SAS: %w", err)
		}
	}

	return azClient, transport, nil
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

// maxIdleConnsPerHost is the least number of idle connections kept per storage host,
// instead of the default 2, so that parallel requests to one host reuse their
// connections rather than reopen them. A larger --*-max-conns-per-host raises it.
const maxIdleConnsPerHost = 100

// newTransport returns a new http.Transport.
func newTransport(maxConnsPerHost int) *http.Transport {
	return &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxConnsPerHost:     maxConnsPerHost,
		MaxIdleConnsPerHost: max(maxConnsPerHost, maxIdleConnsPerHost),
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
