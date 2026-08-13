# Aerospike Backup CLI

[![Tests](https://github.com/aerospike/absctl/actions/workflows/tests.yml/badge.svg)](https://github.com/aerospike/absctl/actions/workflows/tests.yml/badge.svg)
[![PkgGoDev](https://pkg.go.dev/badge/github.com/aerospike/absctl)](https://pkg.go.dev/github.com/aerospike/absctl)
[![codecov](https://codecov.io/gh/aerospike/absctl/graph/badge.svg?token=29G65BU7QX)](https://codecov.io/gh/aerospike/absctl)

`absctl` is a unified command-line tool for Aerospike backup and restore. It supports:

- **Scan-based backup and restore** — client-side scan of the cluster, writing `.asb` files to local disk or object storage.
- **Server-integrated snapshot backup and restore** — backup and restore work executed inside Aerospike Server and written to configured object storage (for example, AWS S3).

The tool is built on the [backup-go](https://github.com/aerospike/backup-go) library. Pre-built binaries for multiple platforms are available on [GitHub Releases](https://github.com/aerospike/absctl/releases).

## Table of Contents

- [Commands](#commands)
- [Installation](#installation)
- [Scan-Based Backup and Restore](#scan-based-backup-and-restore)
- [Server-Integrated Snapshot Backup and Restore](#server-integrated-snapshot-backup-and-restore)
- [Build from Source](#build-from-source)
- [Configuration](#configuration)
- [License](#license)
- [Support](#support)

## Commands

```
absctl [command] [flags]
```

| Command | Description |
|---------|-------------|
| `backup` | Scan the cluster and write backup data to local disk or object storage |
| `restore` | Restore scan-based backups into an Aerospike cluster |
| `snapshot-backup` | Manage server-integrated backups |
| `snapshot-restore` | Manage server-integrated restores |

Run `absctl <command> --help` for flags and usage details.

## Installation

### From Releases

Download pre-built binaries from [GitHub Releases](https://github.com/aerospike/absctl/releases):

```bash
# Linux x64
wget https://github.com/aerospike/absctl/releases/download/<version>/absctl-<version>-<arch>.tar.gz

# Extract
tar -xzvf absctl-<version>-<arch>.tar.gz

# Make executable
chmod +x absctl
```

#### Linux packages

**deb:**

```bash
wget https://github.com/aerospike/absctl/releases/download/<version>/absctl_<version>_<arch>.deb
sudo dpkg -i absctl_<version>_<arch>.deb
```

**rpm:**

```bash
wget https://github.com/aerospike/absctl/releases/download/<version>/absctl-<version>-<arch>.rpm
sudo rpm -i absctl-<version>-<arch>.rpm
```

#### Docker

```bash
docker pull aerospike/absctl:<version>

# Scan-based backup
docker run -v <host-path>:<container-path> aerospike/absctl:<version> \
  absctl backup -h <aerospike-address> -n <namespace> -d <container-path>

# Scan-based restore
docker run -v <host-path>:<container-path> aerospike/absctl:<version> \
  absctl restore -h <aerospike-address> -n <namespace> -d <container-path>

# Server-integrated backup
docker run aerospike/absctl:<version> absctl snapshot-backup start \
  -h <aerospike-address> \
  --namespace <namespace> \
  --object-storage-type aws-s3 \
  --s3-bucket-name <bucket>

# Server-integrated restore
docker run aerospike/absctl:<version> absctl snapshot-restore start \
  -h <aerospike-address> \
  --namespace <namespace> \
  --object-storage-type aws-s3 \
  --backup-id <backup-id> \
  --s3-bucket-name <bucket>
```

## Scan-Based Backup and Restore

Scan-based commands connect to the cluster as a client, scan records according to your scope, and serialize them into `.asb` backup files. Use these commands for flexible, client-driven backups to local paths or cloud storage.

### Features

**Standard operations**

- Full namespace or set backups
- Incremental backups with time-based filtering
- Parallel scan workers for higher throughput
- Resume interrupted backups from state files

**Advanced filtering**

- Set, bin, partition, and time-window filters
- Node and rack targeting

**Enterprise capabilities**

- ZSTD compression
- AES-128/256 encryption
- Direct backup to AWS S3, GCP Storage, or Azure Blob
- Aerospike Secret Agent integration
- Bandwidth and records-per-second rate limits

### Quick Start

**Backup a namespace to a local directory:**

```bash
absctl backup -h 127.0.0.1:3000 -n test -d /backup/test-namespace
```

**Restore from a backup directory:**

```bash
absctl restore -h 127.0.0.1:3000 -n test -d /backup/test-namespace
```

**Backup to S3:**

```bash
absctl backup \
  -h 127.0.0.1:3000 \
  -n test \
  --s3-bucket-name my-backup-bucket \
  --directory my-backups/test
```

For the full flag reference, configuration file schema, and examples, see:

- [Scan backup documentation](docs/scan/backup.md)
- [Scan restore documentation](docs/scan/restore.md)

## Server-Integrated Snapshot Backup and Restore

Server-integrated commands trigger backup and restore operations that run inside Aerospike Server. Data is written to and read from configured object storage. These commands are suited to namespace snapshot workflows coordinated through the cluster.

### Subcommands

#### `absctl snapshot-backup`

| Subcommand | Description |
|------------|-------------|
| `start` | Start a server-integrated backup on the Aerospike cluster |
| `list` | List available server-integrated backups from configured storage |
| `progress` | Show the progress of a running backup |
| `validate` | Validate server-integrated backups in configured storage |

#### `absctl snapshot-restore`

| Subcommand | Description |
|------------|-------------|
| `prepare` | Prepare a server-integrated restore on the Aerospike cluster |
| `start` | Start a server-integrated restore on the Aerospike cluster |
| `progress` | Show the progress of a running restore |

### Features

- **Backup start**: Trigger a namespace backup executed by Aerospike Server
- **Backup list**: Inspect available backups in object storage
- **Backup progress**: Monitor an in-progress backup
- **Backup validate**: Check backup integrity in object storage
- **Restore prepare**: Prepare the cluster for a server-integrated restore
- **Restore start**: Restore a namespace from a backup ID
- **Restore progress**: Monitor an in-progress restore
- **Object storage**: AWS S3 (including MinIO via `--s3-endpoint-override`)
- **Secret management**: Integration with Aerospike Secret Agent for credentials
- **TLS**: Secure connections to the Aerospike cluster

### Quick Start

**Start a backup:**

```bash
absctl snapshot-backup start \
  -h 127.0.0.1:3000 \
  --namespace test \
  --object-storage-type aws-s3 \
  --s3-bucket-name my-backup-bucket
```

**List backups:**

```bash
absctl snapshot-backup list \
  --s3-bucket-name my-backup-bucket
```

**Monitor backup progress:**

```bash
absctl snapshot-backup progress \
  -h 127.0.0.1:3000
```

**Restore from a backup:**

```bash
# Optional: prepare the cluster before restore
absctl snapshot-restore prepare \
  -h 127.0.0.1:3000 \
  --namespace test \
  --backup-id <backup-id>

# Start the restore
absctl snapshot-restore start \
  -h 127.0.0.1:3000 \
  --namespace test \
  --object-storage-type aws-s3 \
  --backup-id <backup-id> \
  --s3-bucket-name my-backup-bucket
```

For the full flag reference and configuration file schema, see:

- [Server backup documentation](docs/server/backup.md)
- [Server restore documentation](docs/server/restore.md)

## Build from Source

```bash
# Build release binaries (default)
make build

# Build debug binaries (includes pprof profiler on localhost:6060)
make build BUILD_MODE=debug

# Install to /usr/bin (Linux only)
make install

# Uninstall (Linux only)
make uninstall
```

### Linux Packages

To generate `.rpm` and `.deb` packages for supported Linux architectures (`linux/amd64`, `linux/arm64`):

```bash
make packages
```

The generated packages and their `sha256` checksum files are written to the `target/` directory.

## Configuration

Configuration can be supplied via command-line flags or a YAML file using `--config`.

- Scan-based commands: see [docs/scan/backup.md](docs/scan/backup.md) and [docs/scan/restore.md](docs/scan/restore.md)
- Server-integrated commands: see [docs/server/backup.md](docs/server/backup.md) and [docs/server/restore.md](docs/server/restore.md)

Run `absctl <command> --help` for the complete flag list.

## License

Apache License, Version 2.0. See [LICENSE](LICENSE) for details.

## Support

- **Documentation**: [Aerospike Documentation](https://aerospike.com/docs/tools/backup/)
- **Issues**: [GitHub Issues](https://github.com/aerospike/absctl/issues)
- **Community**: [Aerospike Community Forum](https://discuss.aerospike.com/)
