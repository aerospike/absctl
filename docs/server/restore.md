# Aerospike server-integrated restore (`absctl snapshot-restore`)

This page describes the server-integrated restore commands exposed by `absctl snapshot-restore`.
These commands coordinate restores through the Aerospike cluster rather than importing records from a client.

<!-- docgen -->

## start

Start a server-integrated restore

Start a server-integrated restore on the Aerospike cluster and
follow its progress until it completes. Interrupting the command
stops the reporting only; use the abort command to stop the restore.
With --async the command returns as soon as the cluster accepts
the restore.

### Supported flags
```bash

Usage:
  absctl snapshot-restore start [flags]

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

Restore Flags:
  -n, --namespace string          The namespace to restore.
      --backup-ids string         Comma separated list of the ids of the backups to restore from.
      --job-id string             Id of the restore job. A strict (cold) restore must use the job id
                                  passed to the restore preparation. If empty, the id of the single backup
                                  being restored is used, so it is required when several backup ids are given.
      --path string               Key prefix in the bucket the backup was written under.
      --incremental-path string   Key prefix in the bucket the incremental backups, every backup id
                                  after the first one, were written under. If empty, --path is used.
                                  Requires more than one backup id.
  -s, --set-list string           Only restore the given sets from the backup.
                                  Default: restore all sets.
      --filter-exp string         Base64 encoded filter expression. Strict policy only.
  -I, --no-indexes                Don't restore any secondary indexes.
      --no-udfs                   Don't restore any UDFs.
      --async                     Return as soon as the cluster accepts the restore, without
                                  following its progress. Use the progress command to follow
                                  the restore later.
      --parallel-read int         Number of threads per node that download the backup files, from 1 to 128. (default 8)
  -w, --parallel-write int        Number of threads per node that write the records, from 1 to 128. (default 8)
  -L, --records-per-second int    Limit the records per second each node writes. If 0, no limit is applied.
      --extra-ttl int             Seconds to add to the expiry of every restored record that expires,
                                  from 0 to 315360000 (10 years). Records that never expire are unchanged.
      --policy string             Restore policy: strict or fuzzy. strict, the cold restore, hydrates the
                                  partitions of a prepared namespace from strict backups only. fuzzy
                                  writes the records of a single backup of any consistency into the
                                  live namespace.
                                  If empty, the server default is used, which is strict.
                                  The flags below apply to the fuzzy policy only.
      --allow-unhosted            Allow the restore even if the principal does not host the namespace.
      --max-inflight int          Maximum number of concurrent writes, up to 100000. (default 200)
      --retry-base-interval int   Base delay before a retry (in ms). 0 retries without a delay. (default 1000)
      --retry-multiplier float    Backoff multiplier of the retry delay. Must be at least 1.0. (default 1)
      --retry-max-attempts int    Maximum number of attempts per record, the first one included. (default 5)
      --ignore-record-error       Continue the restore on errors of individual records.
      --apply-metadata-last       Create secondary indexes and UDFs after the records instead of before.

AWS Storage Flags:
For S3, the storage bucket name must be set with the --s3-bucket-name flag.
--directory path will only contain the folder name.
--s3-endpoint-override is used for MinIO storage instead of AWS.
Any AWS parameter can be retrieved from Secret Agent.
      --s3-bucket-name string         Existing S3 bucket name
      --s3-region string              The S3 region that the bucket(s) exist in.
      --s3-profile string             The S3 profile to use for credentials.
      --s3-access-key-id string       S3 access key ID. If not set, profile auth info will be used.
                                      Used locally by absctl only, it is not sent to the server.
      --s3-secret-access-key string   S3 secret access key. If not set, profile auth info will be used.
                                      Used locally by absctl only, it is not sent to the server.
      --s3-endpoint-override string   An alternate URL endpoint to send S3 API calls to.

Secret Agent Flags:
Options pertaining to the Aerospike Secret Agent.
See documentation here: https://aerospike.com/docs/tools/secret-agent.
Both backup and restore commands support getting all the cloud configuration parameters
from the Aerospike Secret Agent.
To use a secret as an option, use this format: 'secrets:<resource_name>:<secret_name>'
Example: absctl snapshot-restore --azure-account-name secret:resource1:azaccount
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

## prepare

Prepare a server-integrated restore

Prepare a server-integrated restore on the Aerospike cluster and
wait until the namespace is ready for the restore. If the storage of
the namespace has to be cleared on some nodes first, the command says
so and returns: run the clear-storage command then. With --async the
command returns as soon as the cluster accepts the request.

### Supported flags
```bash

Usage:
  absctl snapshot-restore prepare [flags]

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

Restore Flags:
  -n, --namespace string   The namespace to restore.
      --job-id string      Id of the restore job. The restore must be started with the same job id.
      --hydrate-replica    Restore all replicas. If false, only the master is restored and
                           the other replicas are filled by migrations. (default true)
      --async              Return as soon as the cluster accepts the request, without
                           waiting for the namespace to become ready for the restore.

Secret Agent Flags:
Options pertaining to the Aerospike Secret Agent.
See documentation here: https://aerospike.com/docs/tools/secret-agent.
Both backup and restore commands support getting all the cloud configuration parameters
from the Aerospike Secret Agent.
To use a secret as an option, use this format: 'secrets:<resource_name>:<secret_name>'
Example: absctl snapshot-restore --azure-account-name secret:resource1:azaccount
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

## progress

Show the progress of a restore

Show the progress of a server-integrated restore job.
With --watch the progress is followed until the restore completes.

### Supported flags
```bash

Usage:
  absctl snapshot-restore progress [flags]

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

Restore Flags:
  -n, --namespace string   The namespace the restore writes into.
      --job-id string      Id of the restore job to follow.
      --watch              Watch the progress of the restore.

Secret Agent Flags:
Options pertaining to the Aerospike Secret Agent.
See documentation here: https://aerospike.com/docs/tools/secret-agent.
Both backup and restore commands support getting all the cloud configuration parameters
from the Aerospike Secret Agent.
To use a secret as an option, use this format: 'secrets:<resource_name>:<secret_name>'
Example: absctl snapshot-restore --azure-account-name secret:resource1:azaccount
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

## abort

Abort a server-integrated restore

Abort a server-integrated restore on the Aerospike cluster and wait
until it stops. A restore job is identified by the namespace it
restores into and by its job id, so both are required. An aborted
cold restore keeps the namespace closed to clients until it is
released with the open command. With --async the command returns as
soon as the cluster accepts the abort.

### Supported flags
```bash

Usage:
  absctl snapshot-restore abort [flags]

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

Restore Flags:
  -n, --namespace string   The namespace the restore is aborted for.
      --job-id string      Id of the restore job to abort.
      --async              Return as soon as the cluster accepts the abort, without
                           waiting for the restore to stop.

Secret Agent Flags:
Options pertaining to the Aerospike Secret Agent.
See documentation here: https://aerospike.com/docs/tools/secret-agent.
Both backup and restore commands support getting all the cloud configuration parameters
from the Aerospike Secret Agent.
To use a secret as an option, use this format: 'secrets:<resource_name>:<secret_name>'
Example: absctl snapshot-restore --azure-account-name secret:resource1:azaccount
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

## open

Release the namespace of an aborted cold restore

Release the namespace held by an aborted cold restore, so that it
opens to clients again, and wait until it does. The data the restore
wrote so far stays in the namespace. With --async the command
returns as soon as the cluster accepts the request.

### Supported flags
```bash

Usage:
  absctl snapshot-restore open [flags]

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

Restore Flags:
  -n, --namespace string   The namespace to release.
      --job-id string      Id of the aborted cold restore job that holds the namespace.
      --async              Return as soon as the cluster accepts the request, without
                           waiting for the namespace to be released.

Secret Agent Flags:
Options pertaining to the Aerospike Secret Agent.
See documentation here: https://aerospike.com/docs/tools/secret-agent.
Both backup and restore commands support getting all the cloud configuration parameters
from the Aerospike Secret Agent.
To use a secret as an option, use this format: 'secrets:<resource_name>:<secret_name>'
Example: absctl snapshot-restore --azure-account-name secret:resource1:azaccount
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

## clear-storage

Clear the namespace storage for a cold restore

Clear the storage of the namespace a cold restore is preparing, on
every node that asks for it, and wait until the namespace is ready
for the restore. Clearing erases all data of the namespace on those
nodes. With --async the command returns as soon as the nodes accept
the request.

### Supported flags
```bash

Usage:
  absctl snapshot-restore clear-storage [flags]

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

Restore Flags:
  -n, --namespace string   The namespace to clear.
      --job-id string      Id of the cold restore job being prepared.
      --async              Return as soon as the nodes accept the clear, without waiting
                           for the namespace to become ready for the restore.

Secret Agent Flags:
Options pertaining to the Aerospike Secret Agent.
See documentation here: https://aerospike.com/docs/tools/secret-agent.
Both backup and restore commands support getting all the cloud configuration parameters
from the Aerospike Secret Agent.
To use a secret as an option, use this format: 'secrets:<resource_name>:<secret_name>'
Example: absctl snapshot-restore --azure-account-name secret:resource1:azaccount
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

## Configuration file schema with example values

A single file describes the whole `absctl snapshot-restore` command tree. Pass it to any subcommand with `--config`:
only the section named after that subcommand is read, the others are ignored.
When `--config` is set it is the single source of truth — all other flags are ignored.

```bash
absctl snapshot-restore start --config cfg.yaml
```

```yaml
app:
  # Enable more detailed logging.
  verbose: false
  # Determine log level for verbose output. Log levels are: debug, info, warn, error.
  log-level: debug
  # Set output in JSON format for parsing by external tools.
  log-json: false
  # Path to log file. If empty, logs will be printed to stderr.
  log-file: ""
cluster:
  seeds:
    - host: 127.0.0.1
      tls-name: ""
      port: 3000
  # The Aerospike user for the connection to the Aerospike cluster.
  user: db_user
  # The Aerospike password for the connection to the Aerospike
  # cluster.
  password: db_password
  # The authentication mode used by the Aerospike server. INTERNAL
  # uses standard user/pass. EXTERNAL uses external methods (like LDAP)
  # which are configured on the server. EXTERNAL requires TLS. PKI allows
  # TLS authentication and authorization based on a certificate. No
  # username needs to be configured.
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
    # Enable TLS authentication with Aerospike. If false, other TLS
    # options are ignored.
    enable: true
    # Set the TLS protocol selection criteria. This format is the same
    # as Apache's SSLProtocol documented at
    # https://httpd.apache.org/docs/current/mod/mod_ssl.html#sslprotocol
    protocols: +TLSv1.2
    # The CA used when connecting to Aerospike.
    cafile: ""
    # A path containing CAs for connecting to Aerospike.
    capath: ""
    # The certificate file for mutual TLS authentication with
    # Aerospike.
    certfile: ""
    # The key file used for mutual TLS authentication with Aerospike.
    keyfile: ""
    # The password used to decrypt the key file if encrypted.
    keyfile-password: ""
restore:
  # The namespace to restore.
  namespace: source-ns1
  # Comma separated list of the ids of the backups to restore from.
  backup-ids:
    - backup-id-1
    - backup-id-2
  # Id of the restore job. A strict (cold) restore must use the job id
  # passed to the restore preparation. If empty, the id of the single backup
  # being restored is used, so it is required when several backup ids are given.
  job-id: restore-job-1
  # Key prefix in the bucket the backup was written under.
  path: backup_dir
  # Key prefix in the bucket the incremental backups, every backup id
  # after the first one, were written under. If empty, path is used.
  # Requires more than one backup id.
  incremental-path: ""
  # Only restore the given sets from the backup.
  # Default: restore all sets.
  set-list: []
  # Base64 encoded filter expression. Strict policy only.
  filter-exp: ""
  # Don't restore any secondary indexes.
  no-indexes: false
  # Don't restore any UDFs.
  no-udfs: false
  # Return as soon as the cluster accepts the restore, without
  # following its progress. Use the progress command to follow
  # the restore later.
  async: false
  # Number of threads per node that download the backup files, from 1 to 128.
  parallel-read: 8
  # Number of threads per node that write the records, from 1 to 128.
  parallel-write: 8
  # Limit the records per second each node writes. If 0, no limit is applied.
  records-per-second: 0
  # Seconds to add to the expiry of every restored record that expires,
  # from 0 to 315360000 (10 years). Records that never expire are unchanged.
  extra-ttl: 0
  # Restore policy: strict or fuzzy. strict, the cold restore, hydrates the
  # partitions of a prepared namespace from strict backups only. fuzzy
  # writes the records of a single backup of any consistency into the
  # live namespace.
  # If empty, the server default is used, which is strict.
  # The flags below apply to the fuzzy policy only.
  policy: ""
  # Allow the restore even if the principal does not host the namespace.
  allow-unhosted: false
  # Maximum number of concurrent writes, up to 100000.
  max-inflight: 200
  # Base delay before a retry (in ms). 0 retries without a delay.
  retry-base-interval: 1000
  # Backoff multiplier of the retry delay. Must be at least 1.0.
  retry-multiplier: 1
  # Maximum number of attempts per record, the first one included.
  retry-max-attempts: 5
  # Continue the restore on errors of individual records.
  ignore-record-error: false
  # Create secondary indexes and UDFs after the records instead of before.
  apply-metadata-last: false
prepare:
  # The namespace to restore.
  namespace: source-ns1
  # Id of the restore job. The restore must be started with the same job id.
  job-id: restore-job-1
  # Restore all replicas. If false, only the master is restored and
  # the other replicas are filled by migrations.
  hydrate-replica: true
  # Return as soon as the cluster accepts the request, without
  # waiting for the namespace to become ready for the restore.
  async: false
progress:
  # The namespace the restore writes into.
  namespace: source-ns1
  # Id of the restore job to follow.
  job-id: restore-job-1
  # Watch the progress of the restore.
  watch: false
abort:
  # The namespace the restore is aborted for.
  namespace: source-ns1
  # Id of the restore job to abort.
  job-id: restore-job-1
  # Return as soon as the cluster accepts the abort, without
  # waiting for the restore to stop.
  async: false
open:
  # The namespace to release.
  namespace: source-ns1
  # Id of the aborted cold restore job that holds the namespace.
  job-id: restore-job-1
  # Return as soon as the cluster accepts the request, without
  # waiting for the namespace to be released.
  async: false
clear-storage:
  # The namespace to clear.
  namespace: source-ns1
  # Id of the cold restore job being prepared.
  job-id: restore-job-1
  # Return as soon as the nodes accept the clear, without waiting
  # for the namespace to become ready for the restore.
  async: false
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
  # Path to a client certificate file for mutual TLS authentication.
  cert-file: ""
  # Path to a client private key file for mutual TLS authentication.
  key-file: ""
  # TLS name (SNI) for encrypted connections.
  tls-name: ""
  # Whether Secret Agent responses are Base64 encoded.
  is-base64: false
aws:
  s3:
    # Existing S3 bucket name
    bucket-name: backup-bucket
    # The S3 region that the bucket(s) exist in.
    region: us-east-1
    # The S3 profile to use for credentials.
    profile: ""
    # An alternate URL endpoint to send S3 API calls to.
    endpoint-override: ""
    # S3 access key ID. If not set, profile auth info will be used.
    # Used locally by absctl only, it is not sent to the server.
    access-key-id: ""
    # S3 secret access key. If not set, profile auth info will be used.
    # Used locally by absctl only, it is not sent to the server.
    secret-access-key: ""
```
