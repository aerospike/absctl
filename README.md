# Aerospike Backup Service Control
[![Tests](https://github.com/aerospike/absctl/actions/workflows/tests.yml/badge.svg)](https://github.com/aerospike/absctl/actions/workflows/tests.yml/badge.svg)
[![PkgGoDev](https://pkg.go.dev/badge/github.com/aerospike/absctl)](https://pkg.go.dev/github.com/aerospike/absctl)
[![codecov](https://codecov.io/gh/aerospike/absctl/graph/badge.svg?token=29G65BU7QX)](https://codecov.io/gh/aerospike/absctl)

The `absctl` CLI triggers and manages **server-integrated** backup and restore operations on an Aerospike cluster.
Backup and restore work is executed inside Aerospike Server and written to configured object storage (for example, AWS S3).
The tool is built using the [backup-go](https://github.com/aerospike/backup-go) library.

Binaries for various platforms are released alongside the library and can be found under
[releases](https://github.com/aerospike/absctl/releases).

## Commands

```
absctl [command] [flags]
```

| Command | Description |
|---------|-------------|
| `backup` | Manage server-integrated backups |
| `restore` | Manage server-integrated restores |

### `absctl backup`

| Subcommand | Description |
|------------|-------------|
| `start` | Start a server-integrated backup on the Aerospike cluster |
| `list` | List available server-integrated backups from configured storage |
| `progress` | Show the progress of a running backup |

### `absctl restore`

| Subcommand | Description |
|------------|-------------|
| `prepare` | Prepare a server-integrated restore on the Aerospike cluster |
| `start` | Start a server-integrated restore on the Aerospike cluster |

Run `absctl <command> --help` or `absctl <command> <subcommand> --help` for flags and usage details.

## Core Features

### Server-Integrated Operations
- **Backup start**: Trigger a namespace backup executed by Aerospike Server
- **Backup list**: Inspect available backups in object storage
- **Backup progress**: Monitor an in-progress backup
- **Restore prepare**: Prepare the cluster for a server-integrated restore
- **Restore start**: Restore a namespace from a backup ID

### Storage and Security
- **Object storage**: AWS S3 (including MinIO via `--s3-endpoint-override`)
- **Secret management**: Integration with Aerospike Secret Agent for credentials
- **TLS**: Secure connections to the Aerospike cluster

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
The generated packages and their `sha256` checksum files will be located in the `/target` directory.

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

Download linux pakages from [GitHub Releases](https://github.com/aerospike/absctl/releases):

deb:
```bash
# Linux x64
wget https://github.com/aerospike/absctl/releases/download/<version>/absctl_<version>_<arch>.deb

# Install
sudo dpkg -i absctl_<version>_<arch>.deb
```
rpm:
```bash
# Linux x64
wget https://github.com/aerospike/absctl/releases/download/<version>/absctl-<version>-<arch>.rpm

# Install
sudo rpm -i absctl-<version>-<arch>.rpm
```
docker:
```bash
# Pull
docker pull aerospike/absctl:<version>

# Start a server-integrated backup
docker run aerospike/absctl:<version> absctl backup start \
  -h <aerospike-address> \
  --namespace <namespace> \
  --object-storage-type aws-s3 \
  --s3-bucket-name <bucket>

# List backups in object storage
docker run aerospike/absctl:<version> absctl backup list \
  --s3-bucket-name <bucket>

# Start a server-integrated restore
docker run aerospike/absctl:<version> absctl restore start \
  -h <aerospike-address> \
  --namespace <namespace> \
  --object-storage-type aws-s3 \
  --backup-id <backup-id> \
  --s3-bucket-name <bucket>
```

## Quick Start

### Start a Backup
```bash
absctl backup start \
  -h 127.0.0.1:3000 \
  --namespace test \
  --object-storage-type aws-s3 \
  --s3-bucket-name my-backup-bucket
```

### List Backups
```bash
absctl backup list \
  --s3-bucket-name my-backup-bucket
```

### Monitor Backup Progress
```bash
absctl backup progress \
  -h 127.0.0.1:3000
```

### Restore from a Backup
```bash
# Optional: prepare the cluster before restore
absctl restore prepare \
  -h 127.0.0.1:3000 \
  --namespace test \
  --backup-id <backup-id>

# Start the restore
absctl restore start \
  -h 127.0.0.1:3000 \
  --namespace test \
  --object-storage-type aws-s3 \
  --backup-id <backup-id> \
  --s3-bucket-name my-backup-bucket
```

## Configuration Reference

Configuration can be supplied via command-line flags or a YAML file using `--config`.
Run `absctl backup start --help`, `absctl backup list --help`, `absctl restore start --help`, or `absctl restore prepare --help` for the full flag reference.

## License

Apache License, Version 2.0. See [LICENSE](LICENSE) file for details.

## Support

- **Documentation**: [Aerospike Documentation](https://aerospike.com/docs/tools/backup/)
- **Issues**: [GitHub Issues](https://github.com/aerospike/absctl/issues)
- **Community**: [Aerospike Community Forum](https://discuss.aerospike.com/)
