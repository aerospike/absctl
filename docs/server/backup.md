# Aerospike server-integrated backup (`absctl backup`)

This page describes the server-integrated backup commands exposed by `absctl backup`.
These commands coordinate backups through the Aerospike cluster rather than scanning records from a client.

<!-- docgen -->

## start

Start a server-integrated backup

Start a server-integrated backup on the Aerospike cluster.

### Supported flags
```bash

Usage:
  absctl backup start [flags]

General Flags:
  -Z, --help               Display help information.
  -v, --verbose            Enable more detailed logging.
      --log-level string   Determine log level for --verbose output. Log levels are: debug, info, warn, error. (default "debug")
      --log-json           Set output in JSON format for parsing by external tools.
      --log-file string    Path to log file. If empty, logs will be printed to stderr.
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

Backup Flags:
      --namespace string             The namespace to be backed up.
      --object-storage-type string   Type of object storage. Example: aws-s3
  -a, --modified-after string        <YYYY-MM-DD_HH:MM:SS>
                                     Perform an incremental backup; only include records
                                     that changed after the given date and time. The system's
                                     local timezone applies. If only HH:MM:SS is specified, then
                                     today's date is assumed as the date. If only YYYY-MM-DD is
                                     specified, then 00:00:00 (midnight) is assumed as the time.

  -b, --modified-before string       <YYYY-MM-DD_HH:MM:SS>
                                     Only include records that last changed before the given
                                     date and time. May combined with --modified-after to specify a range.
  -s, --set-list string              The set(s) to be backed up. Accepts comma-separated values with no spaces: 'set1,set2,set3'
                                     If multiple sets are being backed up, filter-exp cannot be used.
                                     If empty, include all sets.
  -i, --no-indexes                   Exclude indexes from the backup.
  -u, --no-udfs                      Exclude user-defined functions from the backup.

AWS Storage Flags:
For S3, the storage bucket name must be set with the --s3-bucket-name flag.
--directory path will only contain the folder name.
--s3-endpoint-override is used for MinIO storage instead of AWS.
Any AWS parameter can be retrieved from Secret Agent.
      --s3-bucket-name string         Existing S3 bucket name
      --s3-region string              The S3 region that the bucket(s) exist in.
      --s3-profile string             The S3 profile to use for credentials.
      --s3-access-key-id string       S3 access key ID. If not set, profile auth info will be used.
      --s3-secret-access-key string   S3 secret access key. If not set, profile auth info will be used.
      --s3-endpoint-override string   An alternate URL endpoint to send S3 API calls to.

Secret Agent Flags:
Options pertaining to the Aerospike Secret Agent.
See documentation here: https://aerospike.com/docs/tools/secret-agent.
Both backup and restore commands support getting all the cloud configuration parameters
from the Aerospike Secret Agent.
To use a secret as an option, use this format: 'secrets:<resource_name>:<secret_name>'
Example: absctl backup --azure-account-name secret:resource1:azaccount
      --sa-connection-type string   Secret Agent connection type. Supported types: TCP, UNIX. (default "TCP")
      --sa-address string           Secret Agent host for TCP connection or socket file path for UDS connection.
      --sa-port int                 Secret Agent port (only for TCP connection).
      --sa-timeout int              Secret Agent connection and reading timeout. (default 10000)
      --sa-ca-file string           Path to ca file for encrypted connections.
      --sa-tls-name string          TLS name (SNI) for encrypted connections.
      --sa-cert-file string         Path to a client certificate file for mutual TLS authentication.
      --sa-key-file string          Path to a client private key file for mutual TLS authentication.
      --sa-is-base64                Whether Secret Agent responses are Base64 encoded.
```

## list

List server-integrated backups

List available server-integrated backups from the configured storage.

### Supported flags
```bash

Usage:
  absctl backup list [flags]

Backup Flags:
      --path string   Path to list backups from. (default "/")

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
```

## progress

Show the progress of a backup

Show the progress of a currently running server-integrated backup.

### Supported flags
```bash

Usage:
  absctl backup progress [flags]

General Flags:
  -Z, --help               Display help information.
  -v, --verbose            Enable more detailed logging.
      --log-level string   Determine log level for --verbose output. Log levels are: debug, info, warn, error. (default "debug")
      --log-json           Set output in JSON format for parsing by external tools.
      --log-file string    Path to log file. If empty, logs will be printed to stderr.
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

Secret Agent Flags:
Options pertaining to the Aerospike Secret Agent.
See documentation here: https://aerospike.com/docs/tools/secret-agent.
Both backup and restore commands support getting all the cloud configuration parameters
from the Aerospike Secret Agent.
To use a secret as an option, use this format: 'secrets:<resource_name>:<secret_name>'
Example: absctl backup --azure-account-name secret:resource1:azaccount
      --sa-connection-type string   Secret Agent connection type. Supported types: TCP, UNIX. (default "TCP")
      --sa-address string           Secret Agent host for TCP connection or socket file path for UDS connection.
      --sa-port int                 Secret Agent port (only for TCP connection).
      --sa-timeout int              Secret Agent connection and reading timeout. (default 10000)
      --sa-ca-file string           Path to ca file for encrypted connections.
      --sa-tls-name string          TLS name (SNI) for encrypted connections.
      --sa-cert-file string         Path to a client certificate file for mutual TLS authentication.
      --sa-key-file string          Path to a client private key file for mutual TLS authentication.
      --sa-is-base64                Whether Secret Agent responses are Base64 encoded.
```

## validate

Validate server-integrated backups

Validate available server-integrated backups from the configured storage.

### Supported flags
```bash

Usage:
  absctl backup validate [flags]

Backup Flags:
      --sample-size int    Number of segments for random validation. (default 10000)
      --backup-id string   Backup id used for validation.

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
```
