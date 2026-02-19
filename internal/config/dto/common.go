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

package dto

import (
	"fmt"
	"strings"

	"github.com/aerospike/absctl/internal/models"
	"github.com/aerospike/tools-common-go/client"
	"github.com/aerospike/tools-common-go/flags"
)

// App represents the application-level configuration parsed from a YAML file.
type App struct {
	Verbose  *bool   `yaml:"verbose"`
	LogLevel *string `yaml:"log-level"`
	LogJSON  *bool   `yaml:"log-json"`
}

// defaultApp creates a new App with default values.
func defaultApp() App {
	return App{
		Verbose:  boolPtr(models.DefaultAppVerbose),
		LogLevel: stringPtr(models.DefaultAppLogLevel),
		LogJSON:  boolPtr(models.DefaultAppLogJSON),
	}
}

func (a *App) ToModelApp() *models.App {
	return &models.App{
		Verbose:  derefBool(a.Verbose),
		LogLevel: derefString(a.LogLevel),
		LogJSON:  derefBool(a.LogJSON),
	}
}

// Cluster defines the configuration for connecting to an Aerospike cluster, including seeds, auth, and TLS settings
// parsed from a YAML file.
type Cluster struct {
	Seeds              []ClusterSeed `yaml:"seeds"`
	User               *string       `yaml:"user"`
	Password           *string       `yaml:"password"` // #nosec G117
	Auth               *string       `yaml:"auth"`
	ClientTimeout      *int64        `yaml:"client-timeout"`
	ClientIdleTimeout  *int64        `yaml:"client-idle-timeout"`
	ClientLoginTimeout *int64        `yaml:"client-login-timeout"`
	ServiceAlternate   *bool         `yaml:"services-alternate"`
	TLS                *ClusterTLS   `yaml:"tls"`
}

type ClusterSeed struct {
	Host    *string `yaml:"host"`
	TLSName *string `yaml:"tls-name"`
	Port    *int    `yaml:"port"`
}

func defaultClusterSeed() *ClusterSeed {
	return &ClusterSeed{
		Host:    stringPtr(flags.DefaultIPv4),
		Port:    intPtr(flags.DefaultPort),
		TLSName: new(""),
	}
}

type ClusterTLS struct {
	Enable          *bool   `yaml:"enable"`
	Protocols       *string `yaml:"protocols"`
	CaFile          *string `yaml:"cafile"`
	CaPath          *string `yaml:"capath"`
	CertFile        *string `yaml:"certfile"`
	KeyFile         *string `yaml:"keyfile"`
	KeyFilePassword *string `yaml:"keyfile-password"`
}

func defaultClusterTLS() *ClusterTLS {
	flag := flags.NewDefaultTLSProtocolsFlag()

	return &ClusterTLS{
		Enable:          new(false),
		Protocols:       new(flag.String()),
		CaFile:          new(""),
		CaPath:          new(""),
		CertFile:        new(""),
		KeyFile:         new(""),
		KeyFilePassword: new(""),
	}
}

func defaultCluster() Cluster {
	return Cluster{
		Seeds:              []ClusterSeed{*defaultClusterSeed()},
		TLS:                defaultClusterTLS(),
		User:               new(""),
		Password:           new(""),
		Auth:               new("INTERNAL"),
		ClientTimeout:      int64Ptr(models.DefaultClientPolicyTimeout),
		ClientIdleTimeout:  int64Ptr(models.DefaultClientPolicyIdleTimeout),
		ClientLoginTimeout: int64Ptr(models.DefaultClientPolicyLoginTimeout),
		ServiceAlternate:   new(false),
	}
}

func (c *Cluster) ToAerospikeConfig() (*client.AerospikeConfig, error) {
	if c == nil {
		return nil, fmt.Errorf("cluster cannot be nil")
	}

	var f flags.AerospikeFlags

	if err := c.applySeeds(&f); err != nil {
		return nil, err
	}

	if err := c.applyAuthAndUser(&f); err != nil {
		return nil, err
	}

	if err := c.applyTLS(&f); err != nil {
		return nil, err
	}

	f.UseServicesAlternate = derefBool(c.ServiceAlternate)

	return f.NewAerospikeConfig(), nil
}

func (c *Cluster) applySeeds(f *flags.AerospikeFlags) error {
	if c.Seeds == nil {
		return nil
	}

	hosts := make([]string, 0, len(c.Seeds))

	for i := range c.Seeds {
		seed := c.Seeds[i]

		hostStr := derefString(seed.Host)

		if seed.TLSName != nil && *seed.TLSName != "" {
			hostStr = fmt.Sprintf("%s:%s", hostStr, derefString(seed.TLSName))
		}

		if seed.Port != nil && *seed.Port != 0 {
			hostStr = fmt.Sprintf("%s:%d", hostStr, *seed.Port)
		}

		hosts = append(hosts, hostStr)
	}

	hostPorts := strings.Join(hosts, ",")
	if hostPorts == "" {
		return nil
	}

	if err := f.Seeds.Set(hostPorts); err != nil {
		return fmt.Errorf("failed to set seeds: %w", err)
	}

	return nil
}

func (c *Cluster) applyAuthAndUser(f *flags.AerospikeFlags) error {
	if c.User != nil && *c.User != "" {
		f.User = *c.User
	}

	if c.Password != nil && *c.Password != "" {
		if err := f.Password.Set(*c.Password); err != nil {
			return fmt.Errorf("failed to set password: %w", err)
		}
	}

	if c.Auth != nil && *c.Auth != "" {
		if err := f.AuthMode.Set(*c.Auth); err != nil {
			return fmt.Errorf("failed to set auth mode: %w", err)
		}
	}

	return nil
}

func (c *Cluster) applyTLS(f *flags.AerospikeFlags) error {
	if c.TLS == nil {
		return nil
	}

	f.TLSEnable = derefBool(c.TLS.Enable)

	if !f.TLSEnable {
		return nil
	}

	// Protocols
	if c.TLS.Protocols != nil && *c.TLS.Protocols != "" {
		if err := f.TLSProtocols.Set(*c.TLS.Protocols); err != nil {
			return fmt.Errorf("failed to set tls protocols: %w", err)
		}
	}

	// CA file
	if c.TLS.CaFile != nil && *c.TLS.CaFile != "" {
		if err := f.TLSRootCAFile.Set(*c.TLS.CaFile); err != nil {
			return fmt.Errorf("failed to set tls root ca file: %w", err)
		}
	}

	// CA path
	if c.TLS.CaPath != nil && *c.TLS.CaPath != "" {
		if err := f.TLSRootCAPath.Set(*c.TLS.CaPath); err != nil {
			return fmt.Errorf("failed to set tls root ca path: %w", err)
		}
	}

	// Cert file
	if c.TLS.CertFile != nil && *c.TLS.CertFile != "" {
		if err := f.TLSCertFile.Set(*c.TLS.CertFile); err != nil {
			return fmt.Errorf("failed to set tls cert file: %w", err)
		}
	}

	// Key file
	if c.TLS.KeyFile != nil && *c.TLS.KeyFile != "" {
		if err := f.TLSKeyFile.Set(*c.TLS.KeyFile); err != nil {
			return fmt.Errorf("failed to set tls key file: %w", err)
		}
	}

	// Key password
	if c.TLS.KeyFilePassword != nil && *c.TLS.KeyFilePassword != "" {
		if err := f.TLSKeyFilePass.Set(*c.TLS.KeyFilePassword); err != nil {
			return fmt.Errorf("failed to set tls key file password: %w", err)
		}
	}

	return nil
}

func (c *Cluster) ToModelClientPolicy() *models.ClientPolicy {
	if c == nil {
		return nil
	}

	return &models.ClientPolicy{
		Timeout:      derefInt64(c.ClientTimeout),
		IdleTimeout:  derefInt64(c.ClientIdleTimeout),
		LoginTimeout: derefInt64(c.ClientLoginTimeout),
	}
}

// Compression represents the configuration for data compression, including the mode and compression level
// parsed from a YAML file.
type Compression struct {
	Mode  *string `yaml:"compress"`
	Level *int    `yaml:"level"`
}

func defaultCompression() Compression {
	return Compression{
		Mode:  stringPtr(models.DefaultCompressionMode),
		Level: intPtr(models.DefaultCompressionLevel),
	}
}

func (c *Compression) ToModelCompression() *models.Compression {
	if c == nil {
		return nil
	}

	return &models.Compression{
		Mode:  derefString(c.Mode),
		Level: derefInt(c.Level),
	}
}

// Encryption defines encryption configuration options parsed from a YAML file.
// It includes fields for mode, key file, key environment variable, and key secret
// parsed from a YAML file.
type Encryption struct {
	Mode      *string `yaml:"encrypt"`
	KeyFile   *string `yaml:"key-file"`
	KeyEnv    *string `yaml:"key-env"`
	KeySecret *string `yaml:"key-secret"`
}

func defaultEncryption() Encryption {
	return Encryption{
		Mode:      stringPtr(models.DefaultEncryptionMode),
		KeyFile:   stringPtr(models.DefaultEncryptionKeyFile),
		KeyEnv:    stringPtr(models.DefaultEncryptionKeyEnv),
		KeySecret: stringPtr(models.DefaultEncryptionKeySecret),
	}
}

func (e *Encryption) ToModelEncryption() *models.Encryption {
	if e == nil {
		return nil
	}

	return &models.Encryption{
		Mode:      derefString(e.Mode),
		KeyFile:   derefString(e.KeyFile),
		KeyEnv:    derefString(e.KeyEnv),
		KeySecret: derefString(e.KeySecret),
	}
}

// SecretAgent defines connection properties for a secure agent, including address, port,
// timeout, and encryption settings parsed from a YAML file.
type SecretAgent struct {
	ConnectionType     *string `yaml:"connection-type"`
	Address            *string `yaml:"address"`
	Port               *int    `yaml:"port"`
	TimeoutMillisecond *int    `yaml:"timeout"`
	CaFile             *string `yaml:"ca-file"`
	CertFile           *string `yaml:"cert-file"`
	KeyFile            *string `yaml:"key-file"`
	TLSName            *string `yaml:"tls-name"`
	IsBase64           *bool   `yaml:"is-base64"`
}

func defaultSecretAgent() SecretAgent {
	return SecretAgent{
		ConnectionType:     stringPtr(models.DefaultSecretAgentConnectionType),
		Address:            stringPtr(models.DefaultSecretAgentAddress),
		Port:               intPtr(models.DefaultSecretAgentPort),
		TimeoutMillisecond: intPtr(models.DefaultSecretAgentTimeoutMillisecond),
		CaFile:             stringPtr(models.DefaultSecretAgentCaFile),
		CertFile:           stringPtr(models.DefaultSecretAgentCertFile),
		KeyFile:            stringPtr(models.DefaultSecretAgentKeyFile),
		TLSName:            stringPtr(models.DefaultSecretAgentTLSName),
		IsBase64:           boolPtr(models.DefaultSecretAgentIsBase64),
	}
}

func (s *SecretAgent) ToModelSecretAgent() *models.SecretAgent {
	if s == nil {
		return nil
	}

	return &models.SecretAgent{
		ConnectionType:     derefString(s.ConnectionType),
		Address:            derefString(s.Address),
		Port:               derefInt(s.Port),
		TimeoutMillisecond: derefInt(s.TimeoutMillisecond),
		CaFile:             derefString(s.CaFile),
		CertFile:           derefString(s.CertFile),
		KeyFile:            derefString(s.KeyFile),
		TLSName:            derefString(s.TLSName),
		IsBase64:           derefBool(s.IsBase64),
	}
}

// AwsS3 defines configuration for AWS S3 storage including bucket details and retry mechanisms
// parsed from a YAML file.
type AwsS3 struct {
	BucketName           *string  `yaml:"bucket-name"`
	Region               *string  `yaml:"region"`
	Profile              *string  `yaml:"profile"`
	EndpointOverride     *string  `yaml:"endpoint-override"`
	AccessKeyID          *string  `yaml:"access-key-id"`
	SecretAccessKey      *string  `yaml:"secret-access-key"`
	RestorePollDuration  *int64   `yaml:"restore-poll-duration"`
	StorageClass         *string  `yaml:"storage-class"`
	AccessTier           *string  `yaml:"tier"`
	RetryMaxAttempts     *int     `yaml:"retry-max-attempts"`
	RetryMaxBackoff      *int     `yaml:"retry-max-backoff"`
	ChunkSize            *int     `yaml:"chunk-size"`
	UploadConcurrency    *int     `yaml:"upload-concurrency"`
	CalculateChecksum    *bool    `yaml:"calculate-checksum"`
	RetryReadBackoff     *int     `yaml:"retry-read-backoff"`
	RetryReadMultiplier  *float64 `yaml:"retry-read-multiplier"`
	RetryReadMaxAttempts *uint    `yaml:"retry-read-max-attempts"`
	MaxConnsPerHost      *int     `yaml:"max-conns-per-host"`
	RequestTimeout       *int     `yaml:"request-timeout"`
}

func defaultAwsS3() AwsS3 {
	return AwsS3{
		BucketName:           stringPtr(models.DefaultS3BucketName),
		Region:               stringPtr(models.DefaultS3Region),
		Profile:              stringPtr(models.DefaultS3Profile),
		EndpointOverride:     stringPtr(models.DefaultS3Endpoint),
		AccessKeyID:          stringPtr(models.DefaultS3AccessKeyID),
		SecretAccessKey:      stringPtr(models.DefaultS3SecretAccessKey),
		RestorePollDuration:  int64Ptr(models.DefaultS3RestorePollDuration),
		StorageClass:         stringPtr(models.DefaultS3StorageClass),
		AccessTier:           stringPtr(models.DefaultS3AccessTier),
		RetryMaxAttempts:     intPtr(models.DefaultS3RetryMaxAttempts),
		RetryMaxBackoff:      intPtr(models.DefaultS3RetryMaxBackoff),
		ChunkSize:            intPtr(models.DefaultS3ChunkSize),
		UploadConcurrency:    intPtr(models.DefaultS3UploadConcurrency),
		CalculateChecksum:    boolPtr(models.DefaultCloudCalculateChecksum),
		RetryReadBackoff:     intPtr(models.DefaultCloudRetryReadBackoff),
		RetryReadMultiplier:  float64Ptr(models.DefaultCloudRetryReadMultiplier),
		RetryReadMaxAttempts: uintPtr(models.DefaultCloudRetryReadMaxAttempts),
		MaxConnsPerHost:      intPtr(models.DefaultCloudMaxConnsPerHost),
		RequestTimeout:       intPtr(models.DefaultCloudRequestTimeout),
	}
}

func (a *AwsS3) ToModelAwsS3() *models.AwsS3 {
	if a == nil {
		return nil
	}

	return &models.AwsS3{
		BucketName:          derefString(a.BucketName),
		Region:              derefString(a.Region),
		Profile:             derefString(a.Profile),
		Endpoint:            derefString(a.EndpointOverride),
		AccessKeyID:         derefString(a.AccessKeyID),
		SecretAccessKey:     derefString(a.SecretAccessKey),
		StorageClass:        derefString(a.StorageClass),
		AccessTier:          derefString(a.AccessTier),
		RetryMaxAttempts:    derefInt(a.RetryMaxAttempts),
		RetryMaxBackoff:     derefInt(a.RetryMaxBackoff),
		ChunkSize:           derefInt(a.ChunkSize),
		UploadConcurrency:   derefInt(a.UploadConcurrency),
		RestorePollDuration: derefInt64(a.RestorePollDuration),
		StorageCommon: models.StorageCommon{
			CalculateChecksum:    derefBool(a.CalculateChecksum),
			RetryReadBackoff:     derefInt(a.RetryReadBackoff),
			RetryReadMultiplier:  derefFloat64(a.RetryReadMultiplier),
			RetryReadMaxAttempts: derefUint(a.RetryReadMaxAttempts),
			MaxConnsPerHost:      derefInt(a.MaxConnsPerHost),
			RequestTimeout:       derefInt(a.RequestTimeout),
		},
	}
}

type GcpStorage struct {
	KeyFile                *string  `yaml:"key-path"`
	BucketName             *string  `yaml:"bucket-name"`
	EndpointOverride       *string  `yaml:"endpoint-override"`
	RetryMaxAttempts       *int     `yaml:"retry-max-attempts"`
	RetryMaxBackoff        *int     `yaml:"retry-max-backoff"`
	RetryInitBackoff       *int     `yaml:"retry-init-backoff"`
	RetryBackoffMultiplier *float64 `yaml:"retry-backoff-multiplier"`
	ChunkSize              *int     `yaml:"chunk-size"`
	CalculateChecksum      *bool    `yaml:"calculate-checksum"`
	RetryReadBackoff       *int     `yaml:"retry-read-backoff"`
	RetryReadMultiplier    *float64 `yaml:"retry-read-multiplier"`
	RetryReadMaxAttempts   *uint    `yaml:"retry-read-max-attempts"`
	MaxConnsPerHost        *int     `yaml:"max-conns-per-host"`
	RequestTimeout         *int     `yaml:"request-timeout"`
}

func defaultGcpStorage() GcpStorage {
	return GcpStorage{
		KeyFile:                stringPtr(models.DefaultGcpKeyFile),
		BucketName:             stringPtr(models.DefaultGcpBucketName),
		EndpointOverride:       stringPtr(models.DefaultGcpEndpoint),
		RetryMaxAttempts:       intPtr(models.DefaultGcpRetryMaxAttempts),
		RetryMaxBackoff:        intPtr(models.DefaultGcpRetryBackoffMax),
		RetryInitBackoff:       intPtr(models.DefaultGcpRetryBackoffInit),
		RetryBackoffMultiplier: float64Ptr(models.DefaultGcpRetryBackoffMultiplier),
		ChunkSize:              intPtr(models.DefaultGcpChunkSize),
		CalculateChecksum:      boolPtr(models.DefaultCloudCalculateChecksum),
		RetryReadBackoff:       intPtr(models.DefaultCloudRetryReadBackoff),
		RetryReadMultiplier:    float64Ptr(models.DefaultCloudRetryReadMultiplier),
		RetryReadMaxAttempts:   uintPtr(models.DefaultCloudRetryReadMaxAttempts),
		MaxConnsPerHost:        intPtr(models.DefaultCloudMaxConnsPerHost),
		RequestTimeout:         intPtr(models.DefaultCloudRequestTimeout),
	}
}

func (g *GcpStorage) ToModelGcpStorage() *models.GcpStorage {
	if g == nil {
		return nil
	}

	return &models.GcpStorage{
		KeyFile:                derefString(g.KeyFile),
		BucketName:             derefString(g.BucketName),
		Endpoint:               derefString(g.EndpointOverride),
		RetryMaxAttempts:       derefInt(g.RetryMaxAttempts),
		RetryBackoffMax:        derefInt(g.RetryMaxBackoff),
		RetryBackoffInit:       derefInt(g.RetryInitBackoff),
		RetryBackoffMultiplier: derefFloat64(g.RetryBackoffMultiplier),
		ChunkSize:              derefInt(g.ChunkSize),
		StorageCommon: models.StorageCommon{
			CalculateChecksum:    derefBool(g.CalculateChecksum),
			RetryReadBackoff:     derefInt(g.RetryReadBackoff),
			RetryReadMultiplier:  derefFloat64(g.RetryReadMultiplier),
			RetryReadMaxAttempts: derefUint(g.RetryReadMaxAttempts),
			MaxConnsPerHost:      derefInt(g.MaxConnsPerHost),
			RequestTimeout:       derefInt(g.RequestTimeout),
		},
	}
}

type AzureBlob struct {
	AccountName          *string  `yaml:"account-name"`
	AccountKey           *string  `yaml:"account-key"`
	TenantID             *string  `yaml:"tenant-id"`
	ClientID             *string  `yaml:"client-id"`
	ClientSecret         *string  `yaml:"client-secret"` // #nosec G117
	EndpointOverride     *string  `yaml:"endpoint"`
	ContainerName        *string  `yaml:"container-name"`
	AccessTier           *string  `yaml:"access-tier"`
	RestorePollDuration  *int64   `yaml:"rehydrate-poll-duration"`
	RetryMaxAttempts     *int     `yaml:"retry-max-attempts"`
	RetryDelay           *int     `yaml:"retry-delay"`
	RetryMaxDelay        *int     `yaml:"retry-max-delay"`
	UploadConcurrency    *int     `yaml:"upload-concurrency"`
	CalculateChecksum    *bool    `yaml:"calculate-checksum"`
	RetryReadBackoff     *int     `yaml:"retry-read-backoff"`
	RetryReadMultiplier  *float64 `yaml:"retry-read-multiplier"`
	RetryReadMaxAttempts *uint    `yaml:"retry-read-max-attempts"`
	MaxConnsPerHost      *int     `yaml:"max-conns-per-host"`
	RequestTimeout       *int     `yaml:"request-timeout"`
	BlockSize            *int     `yaml:"block-size"`
}

func defaultAzureBlob() AzureBlob {
	return AzureBlob{
		AccountName:          stringPtr(models.DefaultAzureAccountName),
		AccountKey:           stringPtr(models.DefaultAzureAccountKey),
		TenantID:             stringPtr(models.DefaultAzureTenantID),
		ClientID:             stringPtr(models.DefaultAzureClientID),
		ClientSecret:         stringPtr(models.DefaultAzureClientSecret),
		EndpointOverride:     stringPtr(models.DefaultAzureEndpoint),
		ContainerName:        stringPtr(models.DefaultAzureContainerName),
		AccessTier:           stringPtr(models.DefaultAzureAccessTier),
		RestorePollDuration:  int64Ptr(models.DefaultAzureRestorePollDuration),
		RetryMaxAttempts:     intPtr(models.DefaultAzureRetryMaxAttempts),
		RetryDelay:           intPtr(models.DefaultAzureRetryDelay),
		RetryMaxDelay:        intPtr(models.DefaultAzureRetryMaxDelay),
		UploadConcurrency:    intPtr(models.DefaultAzureUploadConcurrency),
		BlockSize:            intPtr(models.DefaultAzureBlockSize),
		CalculateChecksum:    boolPtr(models.DefaultCloudCalculateChecksum),
		RetryReadBackoff:     intPtr(models.DefaultCloudRetryReadBackoff),
		RetryReadMultiplier:  float64Ptr(models.DefaultCloudRetryReadMultiplier),
		RetryReadMaxAttempts: uintPtr(models.DefaultCloudRetryReadMaxAttempts),
		MaxConnsPerHost:      intPtr(models.DefaultCloudMaxConnsPerHost),
		RequestTimeout:       intPtr(models.DefaultCloudRequestTimeout),
	}
}

func (a *AzureBlob) ToModelAzureBlob() *models.AzureBlob {
	if a == nil {
		return nil
	}

	return &models.AzureBlob{
		AccountName:         derefString(a.AccountName),
		AccountKey:          derefString(a.AccountKey),
		TenantID:            derefString(a.TenantID),
		ClientID:            derefString(a.ClientID),
		ClientSecret:        derefString(a.ClientSecret),
		Endpoint:            derefString(a.EndpointOverride),
		ContainerName:       derefString(a.ContainerName),
		AccessTier:          derefString(a.AccessTier),
		RetryMaxAttempts:    derefInt(a.RetryMaxAttempts),
		RetryDelay:          derefInt(a.RetryDelay),
		RetryMaxDelay:       derefInt(a.RetryMaxDelay),
		UploadConcurrency:   derefInt(a.UploadConcurrency),
		RestorePollDuration: derefInt64(a.RestorePollDuration),
		BlockSize:           derefInt(a.BlockSize),
		StorageCommon: models.StorageCommon{
			CalculateChecksum:    derefBool(a.CalculateChecksum),
			RetryReadBackoff:     derefInt(a.RetryReadBackoff),
			RetryReadMultiplier:  derefFloat64(a.RetryReadMultiplier),
			RetryReadMaxAttempts: derefUint(a.RetryReadMaxAttempts),
			MaxConnsPerHost:      derefInt(a.MaxConnsPerHost),
			RequestTimeout:       derefInt(a.RequestTimeout),
		},
	}
}

type Local struct {
	BufferSize int `yaml:"buffer-size"`
}

func defaultLocal() Local {
	return Local{
		BufferSize: models.DefaultLocalBufferSize,
	}
}

func (l *Local) ToModelLocal() *models.Local {
	if l == nil {
		return nil
	}

	return &models.Local{
		BufferSize: l.BufferSize,
	}
}

//go:fix inline
func intPtr(i int) *int { return new(i) }

//go:fix inline
func uintPtr(i uint) *uint { return new(i) }

//go:fix inline
func int64Ptr(i int64) *int64 { return new(i) }

//go:fix inline
func uint64Ptr(i uint64) *uint64 { return new(i) }

//go:fix inline
func boolPtr(b bool) *bool { return new(b) }

//go:fix inline
func stringPtr(s string) *string { return new(s) }

//go:fix inline
func float64Ptr(f float64) *float64 { return new(f) }

func derefInt(p *int) int {
	if p == nil {
		return 0
	}

	return *p
}

func derefUint(p *uint) uint {
	if p == nil {
		return 0
	}

	return *p
}

func derefInt64(p *int64) int64 {
	if p == nil {
		return 0
	}

	return *p
}

func derefUint64(p *uint64) uint64 {
	if p == nil {
		return 0
	}

	return *p
}

func derefBool(p *bool) bool {
	if p == nil {
		return false
	}

	return *p
}

func derefString(p *string) string {
	if p == nil {
		return ""
	}

	return *p
}

func derefFloat64(p *float64) float64 {
	if p == nil {
		return 0
	}

	return *p
}
