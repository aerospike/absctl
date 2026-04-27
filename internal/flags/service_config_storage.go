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

package flags

import (
	"github.com/aerospike/absctl/internal/models"
	"github.com/spf13/pflag"
)

// ServiceConfigStorage holds flags for the `service config storage add|update`
// commands. Storage wraps exactly one of local/s3/gcp/azure provider blocks;
// populating any field of a provider group selects that provider. --file may
// be used as a base body that flags then override.
type ServiceConfigStorage struct {
	models.ConfigStorageFields
}

func NewServiceConfigStorage() *ServiceConfigStorage {
	return &ServiceConfigStorage{}
}

func (f *ServiceConfigStorage) NewFlagSet() *pflag.FlagSet {
	flagSet := &pflag.FlagSet{}

	flagSet.StringVar(&f.Name, "name", "",
		"Storage name (used as the URL path parameter).")

	flagSet.StringVar(&f.File, "file", "",
		"Path to a JSON file with a DtoStorage body. "+
			"Other flags override fields loaded from this file.")

	flagSet.StringVar(&f.LocalPath, "local-storage-path", "",
		"Local storage: root path of the backup repository.")

	flagSet.IntVar(&f.LocalMinPartSize, "local-storage-min-part-size", 0,
		"Local storage: minimum size in bytes of individual chunks.")

	flagSet.StringVar(&f.S3Bucket, "s3-storage-bucket", "",
		"S3 storage: bucket name.")

	flagSet.StringVar(&f.S3Region, "s3-storage-s3-region", "",
		"S3 storage: AWS region.")

	flagSet.StringVar(&f.S3AccessKeyID, "s3-storage-access-key-id", "",
		"S3 storage: access key ID for static credentials.")

	flagSet.StringVar(&f.S3SecretAccessKey, "s3-storage-secret-access-key", "",
		"S3 storage: secret access key for static credentials.")

	flagSet.StringVar(&f.S3Path, "s3-storage-path", "",
		"S3 storage: root path within the bucket.")

	flagSet.StringVar(&f.S3EndpointOverride, "s3-storage-s3-endpoint-override", "",
		"S3 storage: alternative endpoint URL.")

	flagSet.StringVar(&f.S3Profile, "s3-storage-s3-profile", "",
		"S3 storage: profile name from the AWS credentials file.")

	flagSet.StringVar(&f.S3LogLevel, "s3-storage-s3-log-level", "",
		"S3 storage: AWS S3 SDK log level.")

	flagSet.IntVar(&f.S3MinPartSize, "s3-storage-min-part-size", 0,
		"S3 storage: minimum size in bytes of individual upload parts.")

	flagSet.IntVar(&f.S3MaxAsyncConnections, "s3-storage-max-async-connections", 0,
		"S3 storage: maximum number of simultaneous requests.")

	flagSet.StringVar(&f.S3SecretAgentName, "s3-storage-secret-agent-name", "",
		"S3 storage: name of a preconfigured secret agent.")

	flagSet.StringVar(&f.GcpBucketName, "gcp-storage-bucket-name", "",
		"GCP storage: bucket name.")

	flagSet.StringVar(&f.GcpKey, "gcp-storage-key", "",
		"GCP storage: service account key in JSON format.")

	flagSet.StringVar(&f.GcpKeyFilePath, "gcp-storage-key-file-path", "",
		"GCP storage: path to a service account key JSON file.")

	flagSet.StringVar(&f.GcpEndpoint, "gcp-storage-endpoint", "",
		"GCP storage: alternative endpoint URL.")

	flagSet.StringVar(&f.GcpPath, "gcp-storage-path", "",
		"GCP storage: root path within the bucket.")

	flagSet.IntVar(&f.GcpMinPartSize, "gcp-storage-min-part-size", 0,
		"GCP storage: minimum size in bytes of individual chunks.")

	flagSet.StringVar(&f.GcpSecretAgentName, "gcp-storage-secret-agent-name", "",
		"GCP storage: name of a preconfigured secret agent.")

	flagSet.StringVar(&f.GcpStorageClassData, "gcp-storage-storage-class-data", "",
		"GCP storage: storage class for object data.")

	flagSet.StringVar(&f.AzureAccountName, "azure-storage-account-name", "",
		"Azure storage: storage account name.")

	flagSet.StringVar(&f.AzureAccountKey, "azure-storage-account-key", "",
		"Azure storage: storage account key.")

	flagSet.StringVar(&f.AzureClientID, "azure-storage-client-id", "",
		"Azure storage: AAD client ID.")

	flagSet.StringVar(&f.AzureClientSecret, "azure-storage-client-secret", "",
		"Azure storage: AAD client secret.")

	flagSet.StringVar(&f.AzureContainerName, "azure-storage-container-name", "",
		"Azure storage: blob container name.")

	flagSet.StringVar(&f.AzureEndpoint, "azure-storage-endpoint", "",
		"Azure storage: blob service endpoint URL.")

	flagSet.StringVar(&f.AzureTenantID, "azure-storage-tenant-id", "",
		"Azure storage: AAD tenant ID.")

	flagSet.StringVar(&f.AzurePath, "azure-storage-path", "",
		"Azure storage: root path within the container.")

	flagSet.IntVar(&f.AzureMinPartSize, "azure-storage-min-part-size", 0,
		"Azure storage: minimum size in bytes of individual chunks.")

	flagSet.StringVar(&f.AzureSecretAgentName, "azure-storage-secret-agent-name", "",
		"Azure storage: name of a preconfigured secret agent.")

	flagSet.StringVar(&f.AzureStorageClassData, "azure-storage-storage-class-data", "",
		"Azure storage: storage class tier for object data.")

	flagSet.StringVar(&f.AzureStorageClassMeta, "azure-storage-storage-class-metadata", "",
		"Azure storage: storage class tier for metadata.")

	return flagSet
}
