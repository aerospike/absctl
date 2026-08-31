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

### Running Tests

```bash
# Unit tests. No external services required.
make test
```

Some tests exercise a live Aerospike cluster and the S3, GCS and Azure Blob
backends. They skip unless `ABSCTL_INTEGRATION` is set, so `make test` passes on
a fresh checkout. Each skip message names the service it needs.

[docker-compose.test.yaml](docker-compose.test.yaml) provides those services —
Aerospike, MinIO, Azurite and fake-gcs-server — using the same images as CI:

```bash
# Start the services and wait until they are ready
make test-env-up

# Run everything, including the integration tests
make test-integration

# Stop the services and delete their data
make test-env-down
```

Coverage reported locally will be lower than the CI badge when these tests skip.

## Configuration

Configuration can be supplied via command-line flags or a YAML file using `--config`.

- Scan-based commands: see [docs/scan/backup.md](docs/scan/backup.md) and [docs/scan/restore.md](docs/scan/restore.md)
- Server-integrated commands: see [docs/server/backup.md](docs/server/backup.md) and [docs/server/restore.md](docs/server/restore.md)

Run `absctl <command> --help` for the complete flag list.

## Cutting a release

Releases move through JFrog's promotion stages (`DEV -> TEST -> STAGE -> PREVIEW -> PROD`) before anything is made public. The
GitHub Actions side is split into two workflows:
[`pre-release.yml`](.github/workflows/pre-release.yml) (developer owned, builds and promotes up to `TEST`) and
[`release.yml`](.github/workflows/release.yml) (run once the release is fully approved, publishes it).

### Regular release
1. Create a release branch from `dev` (e.g. `release/1.1.0`).
2. Prepare the release by updating the version files:
   ```bash
   NEXT_VERSION="<version>"  make release
   git add --all
   git commit -m "Release: "$(cat VERSION)""
   ```
3. Open a pull request from your release branch into `main` and merge it.
4. After the PR is merged, tag the release on `main`:
   ```bash
   git checkout main && git pull origin main
   git tag "$(cat VERSION)"
   git push origin main --tags
   ```

### Hotfix
1. Create a hotfix branch from `main` (e.g. `hotfix/1.0.1`).
2. Prepare the hotfix by updating the version files. Bump the **third digit** of the version (e.g. `1.0.0` -> `1.0.1`):
   ```bash
   NEXT_VERSION="<version>"  make release
   git add --all
   git commit -m "Release: "$(cat VERSION)""
   ```
3. **Do not merge** the hotfix branch into `main`. Tag and push the hotfix directly from the branch:
   ```bash
   git tag "$(cat VERSION)"
   git push origin hotfix/1.0.1 --tags
   ```

### Promotion and publication
The following steps apply to both regular releases and hotfixes:

1. Tagging the release commit triggers `pre-release.yml`, which:
   1. Runs GoReleaser to build and publish the cross-platform binary archives directly to GitHub (unchanged by the
      flow below — GoReleaser's output bypasses JFrog entirely).
   2. Builds the DEB/RPM packages and Docker image.
   3. Signs the packages and deploys everything to JFrog `DEV`.
   4. Creates a unified release bundle and automatically promotes it from `DEV` to `TEST`.
6. QE/developers pull the artifacts from JFrog `TEST` and validate them. Once they pass, the release bundle is
   promoted from `TEST` to `STAGE`, either by dispatching
   [`promote-to-preview.yml`](https://github.com/aerospike/absctl/actions/workflows/promote-to-preview.yml) with `environment: STAGE` or manually via the
   [JFrog UI](https://aerospike.jfrog.io/ui/artifactory/release-lifecycle/absctl?repoKey=database-release-bundles-v2).
7. A PM or EM reviews the release and promotes the release bundle from `STAGE` to `PREVIEW`, either by dispatching
   [`promote-to-preview.yml`](https://github.com/aerospike/absctl/actions/workflows/promote-to-preview.yml) with `environment: PREVIEW` or manually via the same
   [JFrog UI](https://aerospike.jfrog.io/ui/artifactory/release-lifecycle/absctl?repoKey=database-release-bundles-v2)
   link.
8. A PM or EM promotes the release bundle from `PREVIEW` to `PROD`, either by dispatching
   [`promote-to-prod.yml`](https://github.com/aerospike/absctl/actions/workflows/promote-to-prod.yml) or manually via the same JFrog UI link. This is
   the gate that makes a release public.
9. Once the bundle is on `PROD`:
   - Docker Hub mirroring happens automatically and externally (JFrog's existing promotion webhook feeds
     `artifact-publisher`) — nothing to trigger here.
   - A dev or PM/EM manually runs [`release.yml`](https://github.com/aerospike/absctl/actions/workflows/release.yml)
     (`workflow_dispatch`, with the release version as input). It verifies the bundle was actually promoted to
     `PROD`, then downloads the already-signed DEB/RPM artifacts straight from JFrog's `PROD`-public repos and
     publishes them as a new, immutable GitHub Release — nothing is rebuilt, re-signed, or re-checksummed at this
     point.
10. Post-release actions:
   1. **Snyk**:
      - Add the new version to the `aerospike-applications` Snyk org.
      - Remove the oldest maintenance version from the same org if no longer supported.
   2. **Slack**:
      - Post the release announcement to the internal **`#releases`** channel.
      - Use the link to the GitHub Release.
      - **Important**: Remove link previews before sending to keep the channel clean (hover over the preview and click the **'x'** in the top-right corner). See [this guide](https://aerospike.atlassian.net/wiki/spaces/RE/pages/2540339350/Message+Slack+releases+Internal+Channel) for more info.
   3. **Email**: Send the release announcement email. See [this guide](https://aerospike.atlassian.net/wiki/spaces/RE/pages/2543124552/Send+email+of+the+Release+Notes+to+the+releases+aerospike.com+distribution+list) for more info.
11. If the release added commits that exist only on `main` (for example a hotfix), back-merge `main` into `dev`.

## License

Apache License, Version 2.0. See [LICENSE](LICENSE) for details.

## Support

- **Documentation**: [Aerospike Documentation](https://aerospike.com/docs/tools/backup/)
- **Issues**: [GitHub Issues](https://github.com/aerospike/absctl/issues)
- **Community**: [Aerospike Community Forum](https://discuss.aerospike.com/)
