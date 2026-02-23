# Aerospike restore (absctl restore)
Aerospike Restore CLI tool. This page describes capabilities and configuration options of the Aerospike restore tool, `absctl restore`.

## Overview
`absctl restore` restores backups created with `absctl backup`. With the `absctl restore` tool, you can restore to specific bins or sets, secure connections using username/password credentials or TLS (or both), and use configuration files to automate restore operations.

## Considerations for Aerospike restore
When using `absctl restore`, be aware of the following considerations:

- The TTL of restored keys is preserved, but the last-update-time and generation count are reset to the current time.
- `absctl restore` creates records from the backup. If records exist in the namespace on the cluster, you can configure a write policy to determine whether the backup records or the records in the namespace take precedence when using `absctl restore`.
- If a restore transaction fails, you can configure timeout options for retries.
- Restore is cluster-configuration-agnostic. A backup can be restored to a cluster of any size and configuration. Restored data is evenly distributed among cluster nodes, regardless of cluster configuration.

## Privileges required for `absctl restore`
The privileges required to run `absctl restore` depend on the type of objects in the namespace.

- If the namespace does not contain [user-defined functions](https://aerospike.com/docs/database/learn/architecture/udf) or [secondary indexes](https://aerospike.com/docs/database/learn/architecture/data-storage/secondary-index), `read-write` is the minimum necessary privilege.
- If the namespace contains [user-defined functions](https://aerospike.com/docs/database/learn/architecture/udf), `udf-admin` is the minimum necessary privilege to restore UDFs for Database 6.0 or later. Otherwise, use `data-admin`.
- If the namespace contains [secondary indexes](https://aerospike.com/docs/database/learn/architecture/data-storage/secondary-index), `sindex-admin` is the minimum necessary privilege to restore secondary indexes for Database 6.0 or later. Otherwise, use `data-admin`.

For more information about Aerospike’s role-based access control system, see [Configuring Access Control in EE and FE](https://aerospike.com/docs/database/manage/security/rbac/#privileges).

---

## Build
### Dev
```bash
make build
```
### Release
Version artifacts are automatically built and uploaded under releases in GitHub.

## Supported flags
```
Usage:
  absctl restore [flags]

General Flags:
  -Z, --help               Display help information.
  -v, --verbose            Enable more detailed logging.
      --log-level string   Determine log level for --verbose output. Log levels are: debug, info, warn, error. (default "debug")
      --log-json           Set output in JSON format for parsing by external tools.
      --log-file string    Path to log file. If empty logs will be printed to stderr.
      --config string      Path to YAML configuration file.

Aerospike Client Flags:
  -h, --host host[:tls-name][:port][,...]                                                           The Aerospike host. (default 127.0.0.1)
  -p, --port int                                                                                    The default Aerospike port. (default 3000)
  -U, --user string                                                                                 The Aerospike user for the connection to the Aerospike cluster.
  -P, --password "env-b64:<env-var>,b64:<b64-pass>,file:<pass-file>,<clear-pass>"                   The Aerospike password for the connection to the Aerospike
                                                                                                    cluster.
      --auth INTERNAL,EXTERNAL,PKI                                                                  The authentication mode used by the Aerospike server. INTERNAL
                                                                                                    uses standard user/pass. EXTERNAL uses external methods (like LDAP)
                                                                                                    which are configured on the server. EXTERNAL requires TLS. PKI allows
                                                                                                    TLS authentication and authorization based on a certificate. No
                                                                                                    username needs to be configured. (default INTERNAL)
      --tls-enable                                                                                  Enable TLS authentication with Aerospike. If false, other TLS
                                                                                                    options are ignored.
      --tls-name string                                                                             The server TLS context to use to authenticate the connection to
                                                                                                    Aerospike.
      --tls-cafile env-b64:<cert>,b64:<cert>,<cert-file-name>                                       The CA used when connecting to Aerospike.
      --tls-capath <cert-path-name>                                                                 A path containing CAs for connecting to Aerospike.
      --tls-certfile env-b64:<cert>,b64:<cert>,<cert-file-name>                                     The certificate file for mutual TLS authentication with
                                                                                                    Aerospike.
      --tls-keyfile env-b64:<cert>,b64:<cert>,<cert-file-name>                                      The key file used for mutual TLS authentication with Aerospike.
      --tls-keyfile-password "env-b64:<env-var>,b64:<b64-pass>,file:<pass-file>,<clear-pass>"       The password used to decrypt the key file if encrypted.
      --tls-protocols "[[+][-]all] [[+][-]TLSv1] [[+][-]TLSv1.1] [[+][-]TLSv1.2] [[+][-]TLSv1.3]"   Set the TLS protocol selection criteria. This format is the same
                                                                                                    as Apache's SSLProtocol documented at
                                                                                                    https://httpd.apache.org/docs/current/mod/mod_ssl.html#sslprotocol (default +TLSv1.2)
      --services-alternate                                                                          Determines if the client should use "services-alternate" instead
                                                                                                    of "services" in info request during cluster tending.
      --client-timeout int         Initial host connection timeout duration. The timeout when opening a connection
                                   to the server host for the first time. (default 30000)
      --client-idle-timeout int    Idle timeout. Every time a connection is used, its idle
                                   deadline will be extended by this duration. When this deadline is reached,
                                   the connection will be closed and discarded from the connection pool.
                                   The value is limited to 24 hours (86400s).
                                   It's important to set this value to a few seconds less than the server's proto-fd-idle-ms
                                   (default 60000 milliseconds or 1 minute), so the client does not attempt to use a socket
                                   that has already been reaped by the server.
                                   Connection pools are now implemented by a LIFO stack. Connections at the tail of the
                                   stack will always be the least used. These connections are checked for IdleTimeout
                                   on every tend (usually 1 second).

      --client-login-timeout int   Specifies the login operation timeout for external authentication methods such as LDAP. (default 10000)

Restore Flags:
  -d, --directory string              The directory that holds the backup files. Required, unless --input-file is used.
  -n, --namespace string              Used to restore to a different namespace. Example: source-ns,destination-ns
  -s, --set-list string               Only restore the given sets from the backup.
                                      Default: restore all sets.
  -B, --bin-list string               Only restore the given bins in the backup.
                                      If empty, include all bins.
  -R, --no-records                    Don't restore any records.
  -I, --no-indexes                    Don't restore any secondary indexes.
      --no-udfs                       Don't restore any UDFs.
  -w, --parallel int                  The number of restore threads. Accepts values from 1-1024 inclusive.
                                      If not set, the default value is automatically calculated and appears as the number of CPUs on your machine.
  -L, --records-per-second int        Limit total returned records per second (RPS). If 0, no limit is applied.
      --total-timeout int             Total transaction timeout (in ms). If 0, no timeout is applied.  (default 10000)
      --socket-timeout int            Socket timeout (in ms). If 0, the value for --total-timeout is used.
                                      If both this and --total-timeout are 0, there is no socket idle time limit. (default 10000)
      --nice int                      The limits for read/write storage bandwidth in MiB/s.
                                      Default is 0 (no limit). (DEPRECATED: use --bandwidth instead)
  -N, --bandwidth int                 The limits for read/write storage bandwidth in MiB/s.
                                      Default is 0 (no limit).
  -T, --info-timeout int              Set the timeout (in ms) for asinfo commands sent from restore tool to the database.
                                      The info commands are to check version, get indexes, get udfs, count records, and check batch write support. (default 10000)
      --info-retry-interval int       Set the initial interval for a retry (in ms) when info commands are sent. (default 1000)
      --info-retry-multiplier float   Increases the delay between subsequent retry attempts.
                                      The actual delay is calculated as: info-retry-interval * (info-retry-multiplier ^ attemptNumber) (default 1)
      --info-max-retries uint         Number of retries to send info commands before failing. (default 3)
      --std-buffer int                Buffer size in MiB for stdin and stdout operations. Used for pipelining. (default 4)
  -i, --input-file string         Restore from a single backup file. Use '-' for stdin.
                                  Required, unless --directory or --directory-list is used.

      --directory-list string     A comma-separated list of paths to directories that hold the backup files. Required,
                                  unless -i or -d is used. The paths may not contain commas.
                                  Example: 'absctl restore --directory-list /path/to/dir1/,/path/to/dir2'

      --parent-directory string   A common root path for all paths used in --directory-list.
                                  This path is prepended to all entries in --directory-list.
                                  Example: 'absctl restore --parent-directory /common/root/path
                                  --directory-list /path/to/dir1/,/path/to/dir2'

  -u, --unique                    Skip modifying records that already exist in the namespace.
  -r, --replace                   Fully replace records that already exist in the namespace.
                                  This option still performs a generation check by default and needs to be combined with the -g option
                                  if you do not want to perform a generation check.
                                  This option is mutually exclusive with --unique.
  -g, --no-generation             Don't check the generation of records that already exist in the namespace.
      --ignore-record-error       Ignore errors specific to records, not UDFs or indexes. The errors are:
                                  AEROSPIKE_RECORD_TOO_BIG,
                                  AEROSPIKE_KEY_MISMATCH,
                                  AEROSPIKE_BIN_NAME_TOO_LONG,
                                  AEROSPIKE_ALWAYS_FORBIDDEN,
                                  AEROSPIKE_FAIL_FORBIDDEN,
                                  AEROSPIKE_BIN_TYPE_ERROR,
                                  AEROSPIKE_BIN_NOT_FOUND.
                                  By default, these errors are not ignored and restore tool terminates.
      --disable-batch-writes      Disables the use of batch writes when restoring records to the Aerospike cluster.
                                  By default, the cluster is checked for batch write support. Only set this flag if you explicitly
                                  don't want batch writes to be used or if restore tool is failing to work because it cannot recognize
                                  that batch writes are disabled.

      --max-async-batches int     To send data to Aerospike Database, restore tool creates write workers that work in parallel.
                                  This value is the number of workers that form batches and send them to the database.
                                  For Aerospike Database versions prior to 6.0, 'batches' are only a logical grouping of records,
                                  and each record is uploaded individually.
                                  The true max number of async Aerospike calls would then be <max-async-batches> * <batch-size>.
                                   (default 32)
      --warm-up int               Warm Up fills the connection pool with connections for all nodes. This is necessary for batch restore.
                                  By default is calculated as (--max-async-batches + 1), as one connection per node is reserved
                                  for tend operations and is not used for transactions.

      --batch-size int            The max allowed number of records to simultaneously upload to Aerospike.
                                  Default is 128 with batch writes enabled. If you disable batch writes,
                                  this flag is superseded because each worker sends writes one by one.
                                  All three batch flags are linked. If --disable-batch-writes=false,
                                  The restore tool uses batch write workers to send data to the database.
                                  restore tool creates a number of workers equal to --max-async-batches that work in parallel,
                                  and form and send a number of records equal to --batch-size to the database.
                                   (default 128)
      --extra-ttl int             For records with expirable void-times, add N seconds of extra-ttl to the
                                  recorded void-time.

      --retry-base-interval int   Set the initial interval for a retry (in ms) when data is sent to the Aerospike database
                                  during a restore. This retry sequence is triggered by the following non-critical errors:
                                  AEROSPIKE_NO_AVAILABLE_CONNECTIONS_TO_NODE,
                                  AEROSPIKE_TIMEOUT,
                                  AEROSPIKE_DEVICE_OVERLOAD,
                                  AEROSPIKE_NETWORK_ERROR,
                                  AEROSPIKE_SERVER_NOT_AVAILABLE,
                                  AEROSPIKE_BATCH_FAILED,
                                  AEROSPIKE_MAX_ERROR_RATE.
                                  This base timeout value is also used as the interval multiplied by --retry-multiplier to increase
                                  the timeout value between retry attempts. (default 1000)
      --retry-multiplier float    Increases the delay between subsequent retry attempts for the errors listed under --retry-base-interval.
                                  The actual delay is calculated as: retry-base-interval * (retry-multiplier ^ attemptNumber) (default 1)
      --retry-max-attempts uint   Set the maximum number of retry attempts for the errors listed under --retry-base-interval.
                                  The default is 0, indicating no retries will be performed
      --validate                  Validate backup files without restoring.
      --apply-metadata-last       Defines when to restore metadata (secondary indexes and UDFs).
                                  If set to true, metadata from separate file will be restored after all records have been processed.

Compression Flags:
  -z, --compress string         Enables decompressing of backup files using the specified compression algorithm.
                                This must match the compression mode used when backing up the data.
                                Supported compression algorithms are: ZSTD, NONE
                                Set the ZSTD compression level via the --compression-level option. (default "NONE")
      --compression-level int   ZSTD compression level. (default 3)

Encryption Flags:
      --encrypt string                 Enables decryption of backup files using the specified encryption algorithm.
                                       This must match the encryption mode used when backing up the data.
                                       Supported encryption algorithms are: NONE, AES128, AES256.
                                       A private key must be given, either with the --encryption-key-file option or
                                       the --encryption-key-env option or the --encryption-key-secret. (default "NONE")
      --encryption-key-file string     Gets the encryption key from the given file, which must be in PEM format.
      --encryption-key-env string      Gets the encryption key from the given environment variable, which must be Base64 encoded.
      --encryption-key-secret string   Gets the encryption key from secret-agent.

Secret Agent Flags:
Options pertaining to the Aerospike Secret Agent.
See documentation here: https://aerospike.com/docs/tools/secret-agent.
Both backup and restore commands support getting all the cloud configuration parameters
from the Aerospike Secret Agent.
To use a secret as an option, use this format: 'secrets:<resource_name>:<secret_name>'
Example: absctl restore --azure-account-name secret:resource1:azaccount
      --sa-connection-type string   Secret Agent connection type. Supported types: TCP, UNIX. (default "TCP")
      --sa-address string           Secret Agent host for TCP connection or socket file path for UDS connection.
      --sa-port int                 Secret Agent port (only for TCP connection).
      --sa-timeout int              Secret Agent connection and reading timeout. (default 10000)
      --sa-ca-file string           Path to ca file for encrypted connections.
      --sa-tls-name string          TLS name (SNI) for encrypted connections.
      --sa-cert-file string         Path to a client certificate file for mutual TLS authentication.
      --sa-key-file string          Path to a client private key file for mutual TLS authentication.
      --sa-is-base64                Whether Secret Agent responses are Base64 encoded.

AWS Storage Flags:
For S3, the storage bucket name must be set with the --s3-bucket-name flag.
--directory path will only contain the folder name.
--s3-endpoint-override is used for MinIO storage instead of AWS.
Any AWS parameter can be retrieved from Secret Agent.
      --s3-bucket-name string             Existing S3 bucket name
      --s3-region string                  The S3 region that the bucket(s) exist in.
      --s3-profile string                 The S3 profile to use for credentials.
      --s3-access-key-id string           S3 access key ID. If not set, profile auth info will be used.
      --s3-secret-access-key string       S3 secret access key. If not set, profile auth info will be used.
      --s3-endpoint-override string       An alternate URL endpoint to send S3 API calls to.
      --s3-tier string                    If is set, tool will try to restore archived files to the specified tier.
                                          Attention! This triggers an asynchronous process that cannot be terminated.
                                          Tiers are: Standard, Bulk, Expedited.
      --s3-restore-poll-duration int      How often ((in ms)) a backup client checks object status when restoring an archived object. (default 60000)
      --s3-retry-read-backoff int         The initial delay (in ms) between retry attempts. In case of connection errors
                                          tool will retry reading the object from the last known position. (default 1000)
      --s3-retry-read-multiplier float    Multiplier is used to increase the delay between subsequent retry attempts.
                                          Used in combination with initial delay. (default 2)
      --s3-retry-read-max-attempts uint   The maximum number of retry attempts that will be made. If set to 0, no retries will be performed. (default 3)
      --s3-retry-max-attempts int         Maximum number of attempts that should be made in case of an error. (default 10)
      --s3-retry-max-backoff int          Max backoff duration (in ms) between retried attempts.
                                          The delay increases exponentially with each retry up to the maximum specified by s3-retry-max-backoff. (default 90000)
      --s3-max-conns-per-host int         Max connections per host optionally limits the total number of connections per host,
                                          including connections in the dialing, active, and idle states. On limit violation, dials will block.
                                          Should be greater than --parallel to avoid download speed degradation.
                                          0 means no limit.
      --s3-request-timeout int            Timeout (in ms) specifies a time limit for requests made by this Client.
                                          The timeout includes connection time, any redirects, and reading the response body.
                                          0 means no limit. (default 600000)

GCP Storage Flags:
For GCP storage, the bucket name must be set with --gcp-bucket-name flag.
--directory path will only contain the folder name.
The flag --gcp-endpoint-override  is optional, and is used for tests or any other GCP emulator.
Any GCP parameter can be retrieved from Secret Agent.
      --gcp-key-path string                  Path to file containing service account JSON key.
      --gcp-bucket-name string               Name of the Google cloud storage bucket.
      --gcp-endpoint-override string         An alternate url endpoint to send GCP API calls to.
      --gcp-retry-read-backoff int           The initial delay (in ms) between retry attempts. In case of connection errors
                                             tool will retry reading the object from the last known position. (default 1000)
      --gcp-retry-read-multiplier float      Multiplier is used to increase the delay between subsequent retry attempts.
                                             Used in combination with initial delay. (default 2)
      --gcp-retry-read-max-attempts uint     The maximum number of retry attempts that will be made. If set to 0, no retries will be performed. (default 3)
      --gcp-retry-max-attempts int           Max retries specifies the maximum number of attempts a failed operation will be retried
                                             before producing an error. (default 10)
      --gcp-retry-max-backoff int            Max backoff is the maximum value (in ms) of the retry period. (default 90000)
      --gcp-retry-init-backoff int           Initial backoff is the initial value (in ms) of the retry period. (default 60000)
      --gcp-retry-backoff-multiplier float   Multiplier is the factor by which the retry period increases.
                                             It should be greater than 1. (default 2)
      --gcp-max-conns-per-host int           Max connections per host optionally limits the total number of connections per host,
                                             including connections in the dialing, active, and idle states. On limit violation, dials will block.
                                             Should be greater than --parallel to avoid speed degradation.
                                             0 means no limit.
      --gcp-request-timeout int              Timeout (in ms) specifies a time limit for requests made by this Client.
                                             The timeout includes connection time, any redirects, and reading the response body.
                                             0 means no limit. (default 600000)

Azure Storage Flags:
For Azure storage, the container name must be set with --azure-container-name flag.
--directory path will only contain folder name.
The flag --azure-endpoint is also mandatory, as each storage account has different service address.
For authentication, use --azure-account-name and --azure-account-key, or
--azure-tenant-id, --azure-client-id and --azure-client-secret.
Any Azure parameter can be retrieved from Secret Agent.
      --azure-account-name string            Azure account name for account name, key authorization.
      --azure-account-key string             Azure account key for account name, key authorization.
      --azure-tenant-id string               Azure tenant ID for Azure Active Directory authorization.
      --azure-client-id string               Azure client ID for Azure Active Directory authorization.
      --azure-client-secret string           Azure client secret for Azure Active Directory authorization.
      --azure-endpoint string                Azure endpoint.
      --azure-container-name string          Azure container Name.
      --azure-access-tier string             If is set, tool will try to rehydrate archived files to the specified tier.
                                             Attention! This triggers an asynchronous process that cannot be terminated.
                                             Tiers are: Cold, Cool, Hot.
      --azure-rehydrate-poll-duration int    How often ((in ms)) a backup client checks object status when restoring an archived object. (default 60000)
      --azure-retry-read-backoff int         The initial delay (in ms) between retry attempts. In case of connection errors
                                             tool will retry reading the object from the last known position. (default 1000)
      --azure-retry-read-multiplier float    Multiplier is used to increase the delay between subsequent retry attempts.
                                             Used in combination with initial delay. (default 2)
      --azure-retry-read-max-attempts uint   The maximum number of retry attempts that will be made. If set to 0, no retries will be performed. (default 3)
      --azure-retry-max-attempts int         Max retries specifies the maximum number of attempts a failed operation will be retried
                                             before producing an error. (default 10)
      --azure-retry-max-delay int            Max retry delay specifies the maximum delay (in ms) allowed before retrying an operation.
                                             Typically the value is greater than or equal to the value specified in azure-retry-delay. (default 90000)
      --azure-retry-delay int                Retry delay specifies the initial amount of delay (in ms) to use before retrying an operation.
                                             The value is used only if the HTTP response does not contain a Retry-After header.
                                             The delay increases exponentially with each retry up to the maximum specified by azure-retry-max-delay. (default 60000)
      --azure-max-conns-per-host int         Max connections per host optionally limits the total number of connections per host,
                                             including connections in the dialing, active, and idle states. On limit violation, dials will block.
                                             Should be greater than --parallel to avoid download speed degradation.
                                             0 means no limit.
      --azure-request-timeout int            Timeout (in ms) specifies a time limit for requests made by this Client.
                                             The timeout includes connection time, any redirects, and reading the response body.
                                             0 means no limit. (default 600000)
```


## Configuration file schema with example values
```yaml
app:
  # Enable more detailed logging.
  verbose: false
  # Determine log level for verbose output. Log levels are: debug, info, warn, error.
  log-level: debug
  # Set output in JSON format for parsing by external tools.
  log-json: false
  # Path to log file. If empty logs will be printed to stderr.
  log-file: ""

cluster:
  seeds:
    - host: 127.0.0.1
      tls-name: ""
      port: 3000
  # The Aerospike user to use to connect to the Aerospike cluster.
  user: "db_user"
  # The Aerospike password to use to connect to the Aerospike cluster.
  password: "db_password"
  # The authentication mode used by the Aerospike server. INTERNAL
  # uses standard user/pass. EXTERNAL uses external methods (like LDAP)
  # which are configured on the server. EXTERNAL requires TLS. PKI allows
  # TLS authentication and authorization based on a certificate. No
  # username needs to be configured. (default INTERNAL)
  auth: INTERNAL
  # Initial host connection timeout duration. The timeout when opening a connection
  # to the server host for the first time.
  client-timeout: 30000
  # Idle timeout. Every time a connection is used, its idle
  # deadline will be extended by this duration. When this deadline is reached,
  # the connection will be closed and discarded from the connection pool.
  # The value is limited to 24 hours (86400s).
  # It's important to set this value to a few seconds less than the server's proto-fd-idle-ms
  # (default 60000 milliseconds or 1 minute), so the client does not attempt to use a socket
  # that has already been reaped by the server.
  # Connection pools are now implemented by a LIFO stack. Connections at the tail of the
  # stack will always be the least used. These connections are checked for IdleTimeout
  # on every tend (usually 1 second).
  client-idle-timeout: 60000
  # Specifies the login operation timeout for external authentication methods such as LDAP.
  client-login-timeout: 10000
  # Determines if the client should use "services-alternate" instead
  # of "services" in info request during cluster tending.
  services-alternate: false
  tls:
    # Enable TLS authentication with Aerospike. If false, other TLS options are ignored.
    enable: true
    # Set the TLS protocol selection criteria. This format is the same
    # as Apache's SSLProtocol documented at
    # https://httpd.apache.org/docs/current/mod/mod_ssl.html#sslprotocol.
    protocols: "+TLSv1.2"
    # The CA used when connecting to Aerospike.
    cafile: ""
    # A path containing CAs for connecting to Aerospike.
    capath: ""
    # The certificate file for mutual TLS authentication with Aerospike.
    certfile: ""
    # The key file used for mutual TLS authentication with Aerospike.
    keyfile: ""
    # The password used to decrypt the key file if encrypted.
    keyfile-password: ""

restore:
  # The directory that holds the backup files. Required, unless input-file is used.
  directory: "backup_dir"
  # Used to restore to a different namespace. Example: source-ns,destination-ns
  namespace: "source-ns1"
  # Only restore the given sets from the backup.
  # Default: restore all sets.
  set-list:
    - "set1"
    - "set2"
  # Only restore the given bins in the backup.
  # If empty, include all bins.
  bin-list:
    - "bin1"
    - "bin2"
  # The number of restore threads. Accepts values from 1-1024 inclusive.
  # If not set, the default value is automatically calculated and appears as the number of CPUs on your machine.
  parallel: 1
  # Don't restore any records.
  no-records: false
  # Don't restore any secondary indexes.
  no-indexes: false
  # Don't restore any UDFs.
  no-udfs: false
  # Limit total returned records per second (RPS). If 0, no limit is applied.
  records-per-second: 0
  # Total transaction timeout (in ms). If 0, no timeout is applied.
  total-timeout: 10000
  # Socket timeout (in ms). If 0, the value for total-timeout is used.
  # If both this and total-timeout are 0, there is no socket idle time limit.
  socket-timeout: 10000
  # The limits for read/write storage bandwidth in MiB/s.
  # Default is 0 (no limit).
  bandwidth: 0
  # Restore from a single backup file. Use '-' for stdin.
  # Required, unless directory or directory-list is used.
  input-file: ""
  # A comma-separated list of paths to directories that hold the backup files. Required,
  # unless -i or -d is used. The paths may not contain commas.
  # Example: 'absctl restore directory-list /path/to/dir1/,/path/to/dir2'
  directory-list:
    - "dir1"
    - "dir2"
  # A common root path for all paths used in directory-list.
  # This path is prepended to all entries in directory-list.
  # Example: 'absctl restore parent-directory /common/root/path
  # directory-list /path/to/dir1/,/path/to/dir2'
  parent-directory: ""
  # Disables the use of batch writes when restoring records to the Aerospike cluster.
  # By default, the cluster is checked for batch write support. Only set this flag if you explicitly
  # don't want batch writes to be used or if absctl restore is failing to work because it cannot recognize
  # that batch writes are disabled.
  disable-batch-writes: false
  # The max allowed number of records to simultaneously upload to Aerospike.
  # Default is 128 with batch writes enabled. If you disable batch writes,
  # this flag is superseded because each worker sends writes one by one.
  # All three batch flags are linked. If disable-batch-writes=false,
  # absctl restore uses batch write workers to send data to the database.
  # absctl restore creates a number of workers equal to max-async-batches that work in parallel,
  # and form and send a number of records equal to batch-size to the database.
  batch-size: 128
  # To send data to Aerospike Database, absctl restore creates write workers that work in parallel.
  # This value is the number of workers that form batches and send them to the database.
  # For Aerospike Database versions prior to 6.0, 'batches' are only a logical grouping of records,
  # and each record is uploaded individually.
  # The true max number of async Aerospike calls would then be <max-async-batches> * <batch-size>.
  max-async-batches: 32
  # Warm Up fills the connection pool with connections for all nodes. This is necessary for batch restore.
  # By default is calculated as (max-async-batches + 1), as one connection per node is reserved
  # for tend operations and is not used for transactions.
  warm-up: 0
  # For records with expirable void-times, add N seconds of extra-ttl to the
  # recorded void-time.
  extra-ttl: 0
  # Ignore errors specific to records, not UDFs or indexes. The errors are:
  # AEROSPIKE_RECORD_TOO_BIG,
  # AEROSPIKE_KEY_MISMATCH,
  # AEROSPIKE_BIN_NAME_TOO_LONG,
  # AEROSPIKE_ALWAYS_FORBIDDEN,
  # AEROSPIKE_FAIL_FORBIDDEN,
  # AEROSPIKE_BIN_TYPE_ERROR,
  # AEROSPIKE_BIN_NOT_FOUND.
  # By default, these errors are not ignored and absctl restore terminates.
  ignore-record-error: false
  # Skip modifying records that already exist in the namespace.
  unique: false
  # Fully replace records that already exist in the namespace.
  # This option still performs a generation check by default and needs to be combined with the -g option
  # if you do not want to perform a generation check.
  # This option is mutually exclusive with unique.
  replace: false
  # Don't check the generation of records that already exist in the namespace.
  no-generation: false
  # Set the timeout (in ms) for asinfo commands sent from absctl restore to the database.
  # The info commands are to check version, get indexes, get udfs, count records, and check batch write support.
  info-timeout: 10000
  # Number of retries to send info commands before failing.
  info-max-retries: 3
  # Increases the delay between subsequent retry attempts.
  # The actual delay is calculated as: info-retry-interval * (info-retry-multiplier ^ attemptNumber)
  info-retry-multiplier: 1
  # Set the initial interval for a retry (in ms) when info commands are sent.
  info-retry-interval: 1000
  # Set the initial interval for a retry (in ms) when data is sent to the Aerospike database
  # during a restore. This retry sequence is triggered by the following non-critical errors:
  # AEROSPIKE_NO_AVAILABLE_CONNECTIONS_TO_NODE,
  # AEROSPIKE_TIMEOUT,
  # AEROSPIKE_DEVICE_OVERLOAD,
  # AEROSPIKE_NETWORK_ERROR,
  # AEROSPIKE_SERVER_NOT_AVAILABLE,
  # AEROSPIKE_BATCH_FAILED,
  # AEROSPIKE_MAX_ERROR_RATE.
  # This base timeout value is also used as the interval multiplied by retry-multiplier to increase
  # the timeout value between retry attempts.
  retry-base-interval: 1000
  # Increases the delay between subsequent retry attempts for the errors listed under retry-base-interval.
  # The actual delay is calculated as: retry-base-interval * (retry-multiplier ^ attemptNumber)
  retry-multiplier: 1
  # Set the maximum number of retry attempts for the errors listed under retry-base-interval.
  # The default is 0, indicating no retries will be performed
  retry-max-attempts: 0
  # Validate backup files without restoring.
  validate: false
  # Defines when to restore metadata (secondary indexes and UDFs).
  # If set to true, metadata from separate file will be restored after all records have been processed.
  apply-metadata-last: false
  # Buffer size in MiB for stdin and stdout operations. Used for pipelining.
  std-buffer: 4

compression:
  # Enables decompressing of backup files using the specified compression algorithm.
  # This must match the compression mode used when backing up the data.
  # Supported compression algorithms are: ZSTD, NONE
  # Set the ZSTD compression level via the compression-level option.
  compress: NONE
  # ZSTD compression level.
  level: 3

encryption:
  # Enables decryption of backup files using the specified encryption algorithm.
  # This must match the encryption mode used when backing up the data.
  # Supported encryption algorithms are: NONE, AES128, AES256.
  # A private key must be given, either with the encryption-key-file option or
  # the encryption-key-env option or the encryption-key-secret.
  encrypt: NONE
  # Gets the encryption key from the given file, which must be in PEM format.
  key-file: ""
  # Gets the encryption key from the given environment variable, which must be Base64 encoded.
  key-env: ""
  # Gets the encryption key from secret-agent.
  key-secret: ""

secret-agent:
  # Secret Agent connection type. Supported types: TCP, UNIX.
  connection-type: TCP
  # Secret Agent host for TCP connection or socket file path for UDS connection.
  address: ""
  # Secret Agent port (only for TCP connection).
  port: 0
  # Secret Agent connection and reading timeout.
  timeout: 10000
  # Path to ca file for encrypted connections.
  ca-file: ""
  # TLS name (SNI) for encrypted connections.
  tls-name: ""
  # Path to a client certificate file for mutual TLS authentication.
  cert-file: ""
  # Path to a client private key file for mutual TLS authentication.
  key-file: ""
  # Whether Secret Agent responses are Base64 encoded.
  # Whether Secret Agent responses are Base64 encoded.
  is-base64: false

aws:
  s3:
    # Existing S3 bucket name
    bucket-name: ""
    # The S3 region that the bucket(s) exist in.
    region: ""
    # The S3 profile to use for credentials.
    profile: ""
    # An alternate URL endpoint to send S3 API calls to.
    endpoint-override: ""
    # S3 access key ID. If not set, profile auth info will be used.
    access-key-id: ""
    # S3 secret access key. If not set, profile auth info will be used.
    secret-access-key: ""
    # If is set, tool will try to restore archived files to the specified tier.
    # Attention! This triggers an asynchronous process that cannot be terminated.
    # Tiers are: Standard, Bulk, Expedited.
    tier: ""
    # How often ((in ms)) a backup client checks object status when restoring an archived object.
    restore-poll-duration: 60000
    # Maximum number of attempts that should be made in case of an error.
    retry-max-attempts: 10
    # Max backoff duration (in ms) between retried attempts.
    # The delay increases exponentially with each retry up to the maximum specified by s3-retry-max-backoff.
    retry-max-backoff: 90000
    # The initial delay (in ms) between retry attempts.
    # In case of connection errors tool will retry reading the object from the last known position.
    retry-read-backoff: 1
    # Multiplier is used to increase the delay between subsequent retry attempts.
    # Used in combination with initial delay.
    retry-read-multiplier: 2.0
    # The maximum number of retry attempts that will be made. If set to 0, no retries will be performed.
    retry-read-max-attempts: 3
    # MaxConnsPerHost optionally limits the total number of connections per host,
    # including connections in the dialing, active, and idle states. On limit violation, dials will block.
    # Should be greater than parallel to avoid download speed degradation.
    # 0 means no limit.
    max-conns-per-host: 0
    # Timeout (in ms) specifies a time limit for requests made by this Client.
    # The timeout includes connection time, any redirects, and reading the response body.
    # 0 means no limit.
    request-timeout: 600000

gcp:
  storage:
    # Path to file containing service account JSON key.
    key-path: ""
    # Name of the Google cloud storage bucket.
    bucket-name: ""
    # An alternate url endpoint to send GCP API calls to.
    endpoint-override: ""
    # Max retries specifies the maximum number of attempts a failed operation will be retried
    # before producing an error.
    retry-max-attempts: 10
    # Max backoff is the maximum value (in ms) of the retry period.
    retry-max-backoff: 90000
    # Initial backoff is the initial value (in ms) of the retry period.
    retry-init-backoff: 60000
    # Multiplier is the factor by which the retry period increases.
    # It should be greater than 1.
    retry-backoff-multiplier: 2
    # The initial delay (in ms) between retry attempts.
    # In case of connection errors tool will retry reading the object from the last known position.
    retry-read-backoff: 1
    # Multiplier is used to increase the delay between subsequent retry attempts.
    # Used in combination with initial delay.
    retry-read-multiplier: 2.0
    # The maximum number of retry attempts that will be made. If set to 0, no retries will be performed.
    retry-read-max-attempts: 3
    # MaxConnsPerHost optionally limits the total number of connections per host,
    # including connections in the dialing, active, and idle states. On limit violation, dials will block.
    # Should be greater than parallel to avoid speed degradation.
    # 0 means no limit.
    max-conns-per-host: 0
    # Timeout (in ms) specifies a time limit for requests made by this Client.
    # The timeout includes connection time, any redirects, and reading the response body.
    # 0 means no limit.
    request-timeout: 600000

azure:
  blob:
    # Azure account name for account name, key authorization.
    account-name: ""
    # Azure account key for account name, key authorization.
    account-key: ""
    # Azure tenant ID for Azure Active Directory authorization.
    tenant-id: ""
    # Azure client ID for Azure Active Directory authorization.
    client-id: ""
    # Azure client secret for Azure Active Directory authorization.
    client-secret: ""
    # Azure endpoint.
    endpoint: ""
    # Azure container Name.
    container-name: ""
    # If is set, tool will try to rehydrate archived files to the specified tier.
    # Attention! This triggers an asynchronous process that cannot be terminated.
    # Tiers are: Cold, Cool, Hot.
    access-tier: ""
    # How often ((in ms)) a backup client checks object status when restoring an archived object.
    rehydrate-poll-duration: 60000
    # Max retries specifies the maximum number of attempts a failed operation will be retried
    # before producing an error.
    retry-max-attempts: 10
    # Retry delay specifies the initial amount of delay (in ms) to use before retrying an operation.
    # The value is used only if the HTTP response does not contain a Retry-After header.
    # The delay increases exponentially with each retry up to the maximum specified by azure-retry-max-delay.
    retry-delay: 60000
    # Max retry delay specifies the maximum delay (in ms) allowed before retrying an operation.
    # Typically the value is greater than or equal to the value specified in azure-retry-delay.
    retry-max-delay: 90000
    # The initial delay (in ms) between retry attempts.
    # In case of connection errors tool will retry reading the object from the last known position.
    retry-read-backoff: 1
    # Multiplier is used to increase the delay between subsequent retry attempts.
    # Used in combination with initial delay.
    retry-read-multiplier: 2.0
    # The maximum number of retry attempts that will be made. If set to 0, no retries will be performed.
    retry-read-max-attempts: 3
    # MaxConnsPerHost optionally limits the total number of connections per host,
    # including connections in the dialing, active, and idle states. On limit violation, dials will block.
    # Should be greater than parallel to avoid download speed degradation.
    # 0 means no limit.
    max-conns-per-host: 0
    # Timeout (in ms) specifies a time limit for requests made by this Client.
    # The timeout includes connection time, any redirects, and reading the response body.
    # 0 means no limit.
    request-timeout: 600000
```
