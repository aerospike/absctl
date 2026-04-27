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
	"fmt"
	"strconv"
	"strings"

	"github.com/aerospike/absctl/api"
	"github.com/aerospike/absctl/internal/models"
)

// The buildXxxBody helpers construct an API request body from the typed field
// model. When File is set the JSON body is loaded as a base; non-zero/non-empty
// flag values then override the corresponding fields. This mirrors the
// override semantics already used by the restore commands.

func buildClusterBody(f *models.ConfigClusterFields) (*api.DtoAerospikeCluster, error) {
	body := &api.DtoAerospikeCluster{}

	if f.File != "" {
		if err := loadJSONFile(f.File, body); err != nil {
			return nil, err
		}
	}

	if len(f.SeedNodes) > 0 {
		nodes, err := parseSeedNodes(f.SeedNodes)
		if err != nil {
			return nil, err
		}

		body.SeedNodes = nodes
	}

	if len(f.PreferRacks) > 0 {
		racks := append([]int(nil), f.PreferRacks...)
		body.PreferRacks = &racks
	}

	if f.ConnTimeout > 0 {
		v := f.ConnTimeout
		body.ConnTimeout = &v
	}

	if f.Label != "" {
		v := f.Label
		body.Label = &v
	}

	if f.MaxParallelScans > 0 {
		v := f.MaxParallelScans
		body.MaxParallelScans = &v
	}

	if f.UseServicesAlternate {
		v := true
		body.UseServicesAlternate = &v
	}

	if anyClusterCredentialsSet(f) {
		if body.Credentials == nil {
			body.Credentials = &api.DtoCredentials{}
		}

		applyClusterCredentials(body.Credentials, f)
	}

	if anyClusterTLSSet(f) {
		if body.Tls == nil {
			body.Tls = &api.DtoTLS{}
		}

		applyClusterTLS(body.Tls, f)
	}

	return body, nil
}

func anyClusterCredentialsSet(f *models.ConfigClusterFields) bool {
	return f.CredentialsUser != "" ||
		f.CredentialsPassword != "" ||
		f.CredentialsPasswordPath != "" ||
		f.CredentialsAuthMode != "" ||
		f.CredentialsSecretAgentName != ""
}

func applyClusterCredentials(c *api.DtoCredentials, f *models.ConfigClusterFields) {
	if f.CredentialsUser != "" {
		v := f.CredentialsUser
		c.User = &v
	}

	if f.CredentialsPassword != "" {
		v := f.CredentialsPassword
		c.Password = &v
	}

	if f.CredentialsPasswordPath != "" {
		v := f.CredentialsPasswordPath
		c.PasswordPath = &v
	}

	if f.CredentialsAuthMode != "" {
		mode := api.DtoCredentialsAuthMode(f.CredentialsAuthMode)
		c.AuthMode = &mode
	}

	if f.CredentialsSecretAgentName != "" {
		v := f.CredentialsSecretAgentName
		c.SecretAgentName = &v
		c.SecretAgent = nil
	}
}

func anyClusterTLSSet(f *models.ConfigClusterFields) bool {
	return f.TLSCaFile != "" ||
		f.TLSCaPath != "" ||
		f.TLSCertFile != "" ||
		f.TLSCipherSuite != "" ||
		f.TLSKeyFile != "" ||
		f.TLSKeyFilePassword != "" ||
		f.TLSName != "" ||
		f.TLSProtocols != ""
}

func applyClusterTLS(t *api.DtoTLS, f *models.ConfigClusterFields) {
	if f.TLSCaFile != "" {
		v := f.TLSCaFile
		t.CaFile = &v
	}

	if f.TLSCaPath != "" {
		v := f.TLSCaPath
		t.CaPath = &v
	}

	if f.TLSCertFile != "" {
		v := f.TLSCertFile
		t.CertFile = &v
	}

	if f.TLSCipherSuite != "" {
		v := f.TLSCipherSuite
		t.CipherSuite = &v
	}

	if f.TLSKeyFile != "" {
		v := f.TLSKeyFile
		t.KeyFile = &v
	}

	if f.TLSKeyFilePassword != "" {
		v := f.TLSKeyFilePassword
		t.KeyFilePassword = &v
	}

	if f.TLSName != "" {
		v := f.TLSName
		t.Name = &v
	}

	if f.TLSProtocols != "" {
		v := f.TLSProtocols
		t.Protocols = &v
	}
}

// parseSeedNodes parses repeated seed-node entries of the form
// "host:port" or "host:port/tls-name" into DtoSeedNode values.
func parseSeedNodes(entries []string) ([]api.DtoSeedNode, error) {
	nodes := make([]api.DtoSeedNode, 0, len(entries))

	for _, raw := range entries {
		entry := strings.TrimSpace(raw)
		if entry == "" {
			continue
		}

		hostPort := entry

		var tlsName string

		if before, after, ok := strings.Cut(entry, "/"); ok {
			hostPort = before
			tlsName = strings.TrimSpace(after)
		}

		colon := strings.LastIndex(hostPort, ":")
		if colon < 0 {
			return nil, fmt.Errorf(
				"invalid --seed-node %q: expected host:port[/tls-name]", raw)
		}

		host := strings.TrimSpace(hostPort[:colon])
		portStr := strings.TrimSpace(hostPort[colon+1:])

		port, err := strconv.Atoi(portStr)
		if err != nil {
			return nil, fmt.Errorf(
				"invalid --seed-node %q: port %q is not an integer", raw, portStr)
		}

		node := api.DtoSeedNode{HostName: host, Port: port}

		if tlsName != "" {
			n := tlsName
			node.TlsName = &n
		}

		nodes = append(nodes, node)
	}

	return nodes, nil
}

func buildPolicyBody(f *models.ConfigPolicyFields) (*api.DtoBackupPolicy, error) {
	body := &api.DtoBackupPolicy{}

	if f.File != "" {
		if err := loadJSONFile(f.File, body); err != nil {
			return nil, err
		}
	}

	setIntPtr(&body.Bandwidth, f.Bandwidth)
	setIntPtr(&body.FileLimit, f.FileLimit)
	setIntPtr(&body.MaxConcurrentNodes, f.MaxConcurrentNodes)
	setIntPtr(&body.Parallel, f.Parallel)
	setIntPtr(&body.ParallelWrite, f.ParallelWrite)
	setIntPtr(&body.RecordsPerSecond, f.RecordsPerSecond)
	setIntPtr(&body.SocketTimeout, f.SocketTimeout)
	setIntPtr(&body.TotalTimeout, f.TotalTimeout)

	setBoolPtrTrue(&body.ConcurrentIncremental, f.ConcurrentIncremental)
	setBoolPtrTrue(&body.NoIndexes, f.NoIndexes)
	setBoolPtrTrue(&body.NoRecords, f.NoRecords)
	setBoolPtrTrue(&body.NoUdfs, f.NoUdfs)
	setBoolPtrTrue(&body.Sealed, f.Sealed)
	setBoolPtrTrue(&body.UseScanCompression, f.UseScanCompression)
	setBoolPtrTrue(&body.WithClusterConfiguration, f.WithClusterConfiguration)

	if f.CompressionMode != "" || f.CompressionLevel > 0 {
		if body.Compression == nil {
			body.Compression = &api.DtoCompressionPolicy{}
		}

		if f.CompressionMode != "" {
			mode := api.DtoCompressionPolicyMode(f.CompressionMode)
			body.Compression.Mode = &mode
		}

		setIntPtr(&body.Compression.Level, f.CompressionLevel)
	}

	if f.EncryptionMode != "" ||
		f.EncryptionKeyEnv != "" ||
		f.EncryptionKeyFile != "" ||
		f.EncryptionKeySecret != "" {
		if body.Encryption == nil {
			body.Encryption = &api.DtoEncryptionPolicy{}
		}

		if f.EncryptionMode != "" {
			mode := api.DtoEncryptionPolicyMode(f.EncryptionMode)
			body.Encryption.Mode = &mode
		}

		setStringPtr(&body.Encryption.KeyEnv, f.EncryptionKeyEnv)
		setStringPtr(&body.Encryption.KeyFile, f.EncryptionKeyFile)
		setStringPtr(&body.Encryption.KeySecret, f.EncryptionKeySecret)
	}

	if f.RetentionFull > 0 || f.RetentionIncremental > 0 {
		if body.Retention == nil {
			body.Retention = &api.DtoRetentionPolicy{}
		}

		setIntPtr(&body.Retention.Full, f.RetentionFull)
		setIntPtr(&body.Retention.Incremental, f.RetentionIncremental)
	}

	if f.RetryBaseTimeout > 0 || f.RetryMaxRetries > 0 || f.RetryMultiplier > 0 {
		if body.RetryPolicy == nil {
			body.RetryPolicy = &api.DtoRetryPolicy{}
		}

		setIntPtr(&body.RetryPolicy.BaseTimeout, f.RetryBaseTimeout)
		setIntPtr(&body.RetryPolicy.MaxRetries, f.RetryMaxRetries)

		if f.RetryMultiplier > 0 {
			v := f.RetryMultiplier
			body.RetryPolicy.Multiplier = &v
		}
	}

	return body, nil
}

func buildRoutineBody(f *models.ConfigRoutineFields) (*api.DtoBackupRoutine, error) {
	body := &api.DtoBackupRoutine{}

	if f.File != "" {
		if err := loadJSONFile(f.File, body); err != nil {
			return nil, err
		}
	}

	if f.BackupPolicy != "" {
		v := f.BackupPolicy
		body.BackupPolicy = &v
	}

	if f.SourceCluster != "" {
		body.SourceCluster = f.SourceCluster
	}

	if f.Storage != "" {
		body.Storage = f.Storage
	}

	if f.SecretAgent != "" {
		v := f.SecretAgent
		body.SecretAgent = &v
	}

	if f.IntervalCron != "" {
		body.IntervalCron = f.IntervalCron
	}

	if f.IncrIntervalCron != "" {
		v := f.IncrIntervalCron
		body.IncrIntervalCron = &v
	}

	if f.PartitionList != "" {
		v := f.PartitionList
		body.PartitionList = &v
	}

	if f.Disabled {
		v := true
		body.Disabled = &v
	}

	if len(f.Namespaces) > 0 {
		body.Namespaces = append([]string(nil), f.Namespaces...)
	}

	if len(f.BinList) > 0 {
		v := append([]string(nil), f.BinList...)
		body.BinList = &v
	}

	if len(f.SetList) > 0 {
		v := append([]string(nil), f.SetList...)
		body.SetList = &v
	}

	if len(f.NodeList) > 0 {
		v := append([]string(nil), f.NodeList...)
		body.NodeList = &v
	}

	if len(f.RackList) > 0 {
		v := append([]int(nil), f.RackList...)
		body.RackList = &v
	}

	return body, nil
}

func buildStorageBody(f *models.ConfigStorageFields) (*api.DtoStorage, error) {
	body := &api.DtoStorage{}

	if f.File != "" {
		if err := loadJSONFile(f.File, body); err != nil {
			return nil, err
		}
	}

	if f.LocalPath != "" || f.LocalMinPartSize > 0 {
		if body.LocalStorage == nil {
			body.LocalStorage = &api.DtoLocalStorage{}
		}

		if f.LocalPath != "" {
			body.LocalStorage.Path = f.LocalPath
		}

		setIntPtr(&body.LocalStorage.MinPartSize, f.LocalMinPartSize)
	}

	if anyS3Set(f) {
		if body.S3Storage == nil {
			body.S3Storage = &api.DtoS3Storage{}
		}

		applyS3Storage(body.S3Storage, f)
	}

	if anyGcpSet(f) {
		if body.GcpStorage == nil {
			body.GcpStorage = &api.DtoGcpStorage{}
		}

		applyGcpStorage(body.GcpStorage, f)
	}

	if anyAzureSet(f) {
		if body.AzureStorage == nil {
			body.AzureStorage = &api.DtoAzureStorage{}
		}

		applyAzureStorage(body.AzureStorage, f)
	}

	return body, nil
}

func anyS3Set(f *models.ConfigStorageFields) bool {
	return f.S3Bucket != "" ||
		f.S3Region != "" ||
		f.S3AccessKeyID != "" ||
		f.S3SecretAccessKey != "" ||
		f.S3Path != "" ||
		f.S3EndpointOverride != "" ||
		f.S3Profile != "" ||
		f.S3LogLevel != "" ||
		f.S3MinPartSize > 0 ||
		f.S3MaxAsyncConnections > 0 ||
		f.S3SecretAgentName != ""
}

func applyS3Storage(s *api.DtoS3Storage, f *models.ConfigStorageFields) {
	if f.S3Bucket != "" {
		s.Bucket = f.S3Bucket
	}

	if f.S3Region != "" {
		s.S3Region = f.S3Region
	}

	if f.S3AccessKeyID != "" {
		v := f.S3AccessKeyID
		s.AccessKeyId = &v
	}

	if f.S3SecretAccessKey != "" {
		v := f.S3SecretAccessKey
		s.SecretAccessKey = &v
	}

	setStringPtr(&s.Path, f.S3Path)
	setStringPtr(&s.S3EndpointOverride, f.S3EndpointOverride)
	setStringPtr(&s.S3Profile, f.S3Profile)

	if f.S3LogLevel != "" {
		level := api.DtoS3StorageS3LogLevel(f.S3LogLevel)
		s.S3LogLevel = &level
	}

	setIntPtr(&s.MinPartSize, f.S3MinPartSize)
	setIntPtr(&s.MaxAsyncConnections, f.S3MaxAsyncConnections)

	if f.S3SecretAgentName != "" {
		v := f.S3SecretAgentName
		s.SecretAgentName = &v
		s.SecretAgent = nil
	}
}

func anyGcpSet(f *models.ConfigStorageFields) bool {
	return f.GcpBucketName != "" ||
		f.GcpKey != "" ||
		f.GcpKeyFilePath != "" ||
		f.GcpEndpoint != "" ||
		f.GcpPath != "" ||
		f.GcpMinPartSize > 0 ||
		f.GcpSecretAgentName != "" ||
		f.GcpStorageClassData != ""
}

func applyGcpStorage(g *api.DtoGcpStorage, f *models.ConfigStorageFields) {
	if f.GcpBucketName != "" {
		g.BucketName = f.GcpBucketName
	}

	setStringPtr(&g.Key, f.GcpKey)
	setStringPtr(&g.KeyFilePath, f.GcpKeyFilePath)
	setStringPtr(&g.Endpoint, f.GcpEndpoint)
	setStringPtr(&g.Path, f.GcpPath)
	setIntPtr(&g.MinPartSize, f.GcpMinPartSize)

	if f.GcpSecretAgentName != "" {
		v := f.GcpSecretAgentName
		g.SecretAgentName = &v
		g.SecretAgent = nil
	}

	if f.GcpStorageClassData != "" {
		if g.StorageClass == nil {
			g.StorageClass = &api.DtoGcpStorageClass{}
		}

		class := api.DtoGcpStorageClassData(f.GcpStorageClassData)
		g.StorageClass.Data = &class
	}
}

func anyAzureSet(f *models.ConfigStorageFields) bool {
	return f.AzureAccountName != "" ||
		f.AzureAccountKey != "" ||
		f.AzureClientID != "" ||
		f.AzureClientSecret != "" ||
		f.AzureContainerName != "" ||
		f.AzureEndpoint != "" ||
		f.AzureTenantID != "" ||
		f.AzurePath != "" ||
		f.AzureMinPartSize > 0 ||
		f.AzureSecretAgentName != "" ||
		f.AzureStorageClassData != "" ||
		f.AzureStorageClassMeta != ""
}

func applyAzureStorage(a *api.DtoAzureStorage, f *models.ConfigStorageFields) {
	setStringPtr(&a.AccountName, f.AzureAccountName)
	setStringPtr(&a.AccountKey, f.AzureAccountKey)
	setStringPtr(&a.ClientId, f.AzureClientID)
	setStringPtr(&a.ClientSecret, f.AzureClientSecret)

	if f.AzureContainerName != "" {
		a.ContainerName = f.AzureContainerName
	}

	if f.AzureEndpoint != "" {
		a.Endpoint = f.AzureEndpoint
	}

	setStringPtr(&a.TenantId, f.AzureTenantID)
	setStringPtr(&a.Path, f.AzurePath)
	setIntPtr(&a.MinPartSize, f.AzureMinPartSize)

	if f.AzureSecretAgentName != "" {
		v := f.AzureSecretAgentName
		a.SecretAgentName = &v
		a.SecretAgent = nil
	}

	if f.AzureStorageClassData != "" || f.AzureStorageClassMeta != "" {
		if a.StorageClass == nil {
			a.StorageClass = &api.DtoAzureStorageClass{}
		}

		if f.AzureStorageClassData != "" {
			class := api.DtoAzureStorageClassData(f.AzureStorageClassData)
			a.StorageClass.Data = &class
		}

		if f.AzureStorageClassMeta != "" {
			class := api.DtoAzureStorageClassMetadata(f.AzureStorageClassMeta)
			a.StorageClass.Metadata = &class
		}
	}
}

// setStringPtr assigns a pointer to v when v is non-empty.
func setStringPtr(target **string, v string) {
	if v == "" {
		return
	}

	s := v
	*target = &s
}

// setIntPtr assigns a pointer to v when v is non-zero (positive).
func setIntPtr(target **int, v int) {
	if v <= 0 {
		return
	}

	n := v
	*target = &n
}

// setBoolPtrTrue assigns a pointer to true when v is true. False values cannot
// be set from a flag alone; use --file to explicitly disable a previously set
// boolean.
func setBoolPtrTrue(target **bool, v bool) {
	if !v {
		return
	}

	b := true
	*target = &b
}
