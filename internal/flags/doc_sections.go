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

// Text for usage pretty-print.

const (
	SectionTextUsageBackup  = "\nUsage:\n  absctl backup [flags]"
	SectionTextUsageRestore = "\nUsage:\n  absctl restore [flags]"

	SectionTextSecretAgentBackup = "\nSecret Agent Flags:\n" +
		"Options pertaining to the Aerospike Secret Agent.\n" +
		"See documentation here: https://aerospike.com/docs/tools/secret-agent.\n" +
		"Both backup and restore commands support getting all the cloud configuration parameters\n" +
		"from the Aerospike Secret Agent.\n" +
		"To use a secret as an option, use this format: 'secrets:<resource_name>:<secret_name>' \n" +
		"Example: absctl backup --azure-account-name secret:resource1:azaccount"
	SectionTextSecretAgentRestore = "\nSecret Agent Flags:\n" +
		"Options pertaining to the Aerospike Secret Agent.\n" +
		"See documentation here: https://aerospike.com/docs/tools/secret-agent.\n" +
		"Both backup and restore commands support getting all the cloud configuration parameters\n" +
		"from the Aerospike Secret Agent.\n" +
		"To use a secret as an option, use this format: 'secrets:<resource_name>:<secret_name>' \n" +
		"Example: absctl restore --azure-account-name secret:resource1:azaccount"

	SectionTextBackup  = "\nBackup Flags:"
	SectionTextRestore = "\nBackup Flags:"

	SectionTextGeneral     = "\nGeneral Flags:"
	SectionTextAerospike   = "\nAerospike Client Flags:"
	SectionTextCompression = "\nCompression Flags:"
	SectionTextEncryption  = "\nEncryption Flags:"

	SectionTextLocal = "\nLocal Storage Flags:"
	SectionTextAWS   = "\nAWS Storage Flags:\n" +
		"For S3, the storage bucket name must be set with the --s3-bucket-name flag.\n" +
		"--directory path will only contain the folder name.\n" +
		"--s3-endpoint-override is used for MinIO storage instead of AWS.\n" +
		"Any AWS parameter can be retrieved from Secret Agent."
	SectionTextGCP = "\nGCP Storage Flags:\n" +
		"For GCP storage, the bucket name must be set with --gcp-bucket-name flag.\n" +
		"--directory path will only contain the folder name.\n" +
		"The flag --gcp-endpoint-override  is optional, and is used for tests or any other GCP emulator.\n" +
		"Any GCP parameter can be retrieved from Secret Agent."
	SectionTextAzure = "\nAzure Storage Flags:\n" +
		"For Azure storage, the container name must be set with --azure-container-name flag.\n" +
		"--directory path will only contain folder name.\n" +
		"The flag --azure-endpoint is also mandatory, as each storage account has different service address.\n" +
		"For authentication, use --azure-account-name and --azure-account-key, or \n" +
		"--azure-tenant-id, --azure-client-id and --azure-client-secret.\n" +
		"Any Azure parameter can be retrieved from Secret Agent."
)
