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

package models

// App.
const (
	DefaultAppHelp           = false
	DefaultAppVersion        = false
	DefaultAppVerbose        = false
	DefaultAppLogLevel       = "debug"
	DefaultAppLogFile        = ""
	DefaultAppLogJSON        = false
	DefaultAppConfigFilePath = ""
)

// Aws S3 Storage.
const (
	DefaultS3BucketName          = ""
	DefaultS3Region              = ""
	DefaultS3Profile             = ""
	DefaultS3Endpoint            = ""
	DefaultS3AccessKeyID         = ""
	DefaultS3SecretAccessKey     = ""
	DefaultS3StorageClass        = ""
	DefaultS3AccessTier          = ""
	DefaultS3RestorePollDuration = int64(60000)
	DefaultS3RetryMaxAttempts    = 10
	DefaultS3RetryMaxBackoff     = 90000
	DefaultS3ChunkSize           = 5
	DefaultS3UploadConcurrency   = 0
)

// Azure Blob Storage
const (
	DefaultAzureAccountName         = ""
	DefaultAzureAccountKey          = ""
	DefaultAzureTenantID            = ""
	DefaultAzureClientID            = ""
	DefaultAzureClientSecret        = ""
	DefaultAzureEndpoint            = ""
	DefaultAzureContainerName       = ""
	DefaultAzureAccessTier          = ""
	DefaultAzureRestorePollDuration = int64(60000)
	DefaultAzureRetryMaxAttempts    = 10
	DefaultAzureRetryDelay          = 60000
	DefaultAzureRetryMaxDelay       = 90000
	DefaultAzureBlockSize           = 5
	DefaultAzureUploadConcurrency   = 1
)

// Gpc Storage.
const (
	DefaultGcpKeyFile                = ""
	DefaultGcpBucketName             = ""
	DefaultGcpEndpoint               = ""
	DefaultGcpRetryMaxAttempts       = 10
	DefaultGcpRetryBackoffMax        = 90000
	DefaultGcpRetryBackoffInit       = 60000
	DefaultGcpRetryBackoffMultiplier = 2.0
	DefaultGcpChunkSize              = 5
)

// noinspection GoCommentStart
// Local Storage.
const (
	DefaultLocalBufferSize = 5
)

// Cloud common.
const (
	DefaultCloudMaxConnsPerHost      = 0
	DefaultCloudRequestTimeout       = 600000
	DefaultCloudCalculateChecksum    = false
	DefaultCloudRetryReadBackoff     = 1000
	DefaultCloudRetryReadMultiplier  = float64(2.0)
	DefaultCloudRetryReadMaxAttempts = uint(3)
)

// Client Policy.
const (
	DefaultClientPolicyTimeout      = int64(30000)
	DefaultClientPolicyIdleTimeout  = int64(0)
	DefaultClientPolicyLoginTimeout = int64(10000)
)

// Compression.
const (
	DefaultCompressionMode  = "NONE"
	DefaultCompressionLevel = 3
)

// Encryption.
const (
	DefaultEncryptionMode      = "NONE"
	DefaultEncryptionKeyFile   = ""
	DefaultEncryptionKeyEnv    = ""
	DefaultEncryptionKeySecret = ""
)

// Secret Agent.
const (
	DefaultSecretAgentConnectionType     = "TCP"
	DefaultSecretAgentAddress            = ""
	DefaultSecretAgentPort               = 0
	DefaultSecretAgentTimeoutMillisecond = 10000
	DefaultSecretAgentCaFile             = ""
	DefaultSecretAgentTLSName            = ""
	DefaultSecretAgentCertFile           = ""
	DefaultSecretAgentKeyFile            = ""
	DefaultSecretAgentIsBase64           = false
)

// Common for backup and restore.
const (
	DefaultCommonDirectory             = ""
	DefaultCommonNamespace             = ""
	DefaultCommonSetList               = ""
	DefaultCommonBinList               = ""
	DefaultCommonNoRecords             = false
	DefaultCommonNoIndexes             = false
	DefaultCommonNoUDFs                = false
	DefaultCommonRecordsPerSecond      = 0
	DefaultCommonSocketTimeout         = int64(10000)
	DefaultCommonInfoTimeout           = int64(10000)
	DefaultCommonInfoMaxRetries        = uint(3)
	DefaultCommonInfoRetriesMultiplier = 1.0
	DefaultCommonInfoRetryInterval     = int64(1000)
	DefaultCommonBandwidth             = int64(0)
	DefaultCommonStdBufferSize         = 4
)

// Backup.
const (
	DefaultBackupOutputFile          = ""
	DefaultBackupRemoveFiles         = false
	DefaultBackupModifiedBefore      = ""
	DefaultBackupModifiedAfter       = ""
	DefaultBackupFileLimit           = uint64(250)
	DefaultBackupAfterDigest         = ""
	DefaultBackupMaxRecords          = int64(0)
	DefaultBackupNoBins              = false
	DefaultBackupSleepBetweenRetries = 5
	DefaultBackupFilterExpression    = ""
	DefaultBackupRemoveArtifacts     = false
	DefaultBackupCompact             = false
	DefaultBackupNodeList            = ""
	DefaultBackupNoTTLOnly           = false
	DefaultBackupPreferRacks         = ""
	DefaultBackupPartitionList       = ""
	DefaultBackupEstimate            = false
	DefaultBackupEstimateSamples     = int64(10000)
	DefaultBackupStateFileDst        = ""
	DefaultBackupContinue            = ""
	DefaultBackupScanPageSize        = int64(10000)
	DefaultBackupOutputFilePrefix    = ""
	DefaultBackupRackList            = ""
	DefaultBackupTotalTimeout        = int64(0)
	DefaultBackupParallel            = 1
	DefaultBackupMaxRetries          = 5
)

// Restore.
const (
	DefaultRestoreTotalTimeout       = int64(10000)
	DefaultRestoreParallel           = 0
	DefaultRestoreInputFile          = ""
	DefaultRestoreDirectoryList      = ""
	DefaultRestoreParentDirectory    = ""
	DefaultRestoreDisableBatchWrites = false
	DefaultRestoreBatchSize          = 128
	DefaultRestoreMaxAsyncBatches    = 32
	DefaultRestoreWarmUp             = 0
	DefaultRestoreExtraTTL           = int64(0)
	DefaultRestoreIgnoreRecordError  = false
	DefaultRestoreUniq               = false
	DefaultRestoreReplace            = false
	DefaultRestoreNoGeneration       = false
	DefaultRestoreRetryBaseInterval  = int64(1000)
	DefaultRestoreRetryMultiplier    = 1.0
	DefaultRestoreRetryMaxAttempts   = uint(0)

	DefaultRestoreValidateOnly      = false
	DefaultRestoreApplyMetadataLast = false
)

const (
	DefaultBackupXDRDirectory             = ""
	DefaultBackupXDRFileLimit             = 250
	DefaultBackupXDRRemoveFiles           = false
	DefaultBackupXDRParallelWrite         = 0
	DefaultBackupXDRDC                    = "dc"
	DefaultBackupXDRLocalAddress          = "127.0.0.1"
	DefaultBackupXDRLocalPort             = 8080
	DefaultBackupXDRNamespace             = ""
	DefaultBackupXDRRewind                = "all"
	DefaultBackupXDRMaxThroughput         = 0
	DefaultBackupXDRReadTimeout           = 1000
	DefaultBackupXDRWriteTimeout          = 1000
	DefaultBackupXDRResultQueueSize       = 256
	DefaultBackupXDRAckQueueSize          = 256
	DefaultBackupXDRMaxConnections        = 4096
	DefaultBackupXDRInfoPolingPeriod      = 1000
	DefaultBackupXDRStartTimeout          = 30000
	DefaultBackupXDRInfoTimeout           = 10000
	DefaultBackupXDRStopXDR               = false
	DefaultBackupXDRUnblockMRT            = false
	DefaultBackupXDRInfoMaxRetries        = 3
	DefaultBackupXDRInfoRetriesMultiplier = 1.0
	DefaultBackupXDRInfoRetryInterval     = 1000
	DefaultBackupXDRForward               = false
)
