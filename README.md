# Aerospike Backup CLI

[![Tests](https://github.com/aerospike/absctl/actions/workflows/tests.yml/badge.svg)](https://github.com/aerospike/absctl/actions/workflows/tests.yml/badge.svg)
[![PkgGoDev](https://pkg.go.dev/badge/github.com/aerospike/absctl)](https://pkg.go.dev/github.com/aerospike/absctl)
[![codecov](https://codecov.io/gh/aerospike/absctl/graph/badge.svg?token=29G65BU7QX)](https://codecov.io/gh/aerospike/absctl)

`absctl` is a unified command-line tool for Aerospike backup and restore. It supports:

- **Scan-based backup and restore** — client-side scan of the cluster, writing `.asb` files to local disk or object storage.
- **Server-integrated snapshot backup and restore** — backup and restore work executed inside Aerospike Server and written to configured object storage (for example, AWS S3).

The tool is built on the [backup-go](https://github.com/aerospike/backup-go) library. DEB and RPM packages are available on [GitHub Releases](https://github.com/aerospike/absctl/releases), and a container image on [Docker Hub](https://hub.docker.com/r/aerospike/absctl).

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

`absctl` ships as DEB and RPM packages for Linux, a `.pkg` installer for macOS -- all attached to each
[GitHub Release](https://github.com/aerospike/absctl/releases) -- and as a Docker image. Every Linux
package is built per distribution, so pick the file whose distro tag matches your system: `ubuntu22.04`,
`ubuntu24.04`, `ubuntu26.04`, `debian11`, `debian12` or `debian13` for DEB, and `el8`, `el9`, `el10` or
`amzn2023` for RPM.

### Linux packages

**deb:**

```bash
wget https://github.com/aerospike/absctl/releases/download/v<version>/absctl_<version>_<arch>_<distro>.deb
sudo dpkg -i absctl_<version>_<arch>_<distro>.deb

# for example
wget https://github.com/aerospike/absctl/releases/download/v1.2.0/absctl_1.2.0_amd64_ubuntu24.04.deb
sudo dpkg -i absctl_1.2.0_amd64_ubuntu24.04.deb
```

**rpm:**

```bash
wget https://github.com/aerospike/absctl/releases/download/v<version>/absctl-<version>-1.<distro>.<arch>.rpm
sudo rpm -i absctl-<version>-1.<distro>.<arch>.rpm

# for example
wget https://github.com/aerospike/absctl/releases/download/v1.2.0/absctl-1.2.0-1.el9.x86_64.rpm
sudo rpm -i absctl-1.2.0-1.el9.x86_64.rpm
```

Each package is published with a `.sha256` checksum and a detached `.asc` GPG signature next to it.

### macOS

The installer is signed with Aerospike's Developer ID, notarized by Apple and stapled, so it installs
without a Gatekeeper prompt. It places `absctl` at `/usr/local/bin/absctl`.

Pick the package matching your architecture -- `arm64` for Apple Silicon, `x86_64` for Intel. There is
one package per architecture and it works on every supported macOS release, so the filename carries no
macOS version:

```bash
curl -LO https://github.com/aerospike/absctl/releases/download/v<version>/absctl-<version>-macos-<arch>.pkg
sudo installer -pkg absctl-<version>-macos-<arch>.pkg -target /

# for example, on Apple Silicon
curl -LO https://github.com/aerospike/absctl/releases/download/v1.2.0/absctl-1.2.0-macos-arm64.pkg
sudo installer -pkg absctl-1.2.0-macos-arm64.pkg -target /
```

Run `uname -m` if you are unsure which to download; it prints exactly the architecture token used in the
filename. To confirm the package is genuine before installing:

```bash
pkgutil --check-signature absctl-1.2.0-macos-arm64.pkg
xcrun stapler validate absctl-1.2.0-macos-arm64.pkg
```

To uninstall, remove the binary and forget the package receipt:

```bash
sudo rm /usr/local/bin/absctl
sudo pkgutil --forget com.aerospike.absctl
```

### Docker

Image tags carry no leading `v` (for example `1.2.0` for release `v1.2.0`):

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

### macOS Package

To build the macOS `.pkg` installers (requires macOS, for `pkgbuild`):

```bash
make mac-packages VERSION=v1.2.0
```

That produces one package per architecture in `dist/`. `make mac-pkg ARCH=arm64` builds a single one.
Both architectures are cross-compiled from whichever Mac you are on -- `absctl` is a pure-Go,
`CGO_ENABLED=0` binary, and `pkgbuild` only stages files. Locally built packages are unsigned; signing
and notarization happen in CI.

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
[`release.yml`](.github/workflows/release.yml) (run once the release is fully approved; publishes a GitHub pre-release
for final validation, then a PM/EM promotes it to GA manually).

The **tag is the only source of truth for the release version**: `pre-release.yml` triggers on `v*` tags
and derives the bundle version, package versions and image tags from `github.ref_name`, while the
`Makefile` stamps the binary from `git describe --tags`. There is no `VERSION` file to bump.

### Regular release
1. Create a release branch from `dev` (e.g. `release/1.1.0`).
2. Open a pull request from your release branch into `main` and merge it.
3. After the PR is merged, tag the release on `main`:
   ```bash
   git checkout main && git pull origin main
   git tag v<version>
   git push origin main --tags
   ```

### Hotfix
1. Create a hotfix branch from `main` (e.g. `hotfix/1.0.1`) and land the fix on it.
2. Choose the hotfix version by bumping the **third digit** (e.g. `1.0.0` -> `1.0.1`).
3. **Do not merge** the hotfix branch into `main`. Tag and push the hotfix directly from the branch:
   ```bash
   git tag v<version>
   git push origin hotfix/1.0.1 --tags
   ```

### Promotion and publication
The following steps apply to both regular releases and hotfixes:

1. Tagging the release commit triggers `pre-release.yml`, which:
   1. Runs GoReleaser (`goreleaser build`) as a cross-platform compile check only. `.goreleaser.yaml` sets
      `release.disable`, so this job archives, signs and publishes nothing — every artifact that ships comes
      from the JFrog legs below.
   2. Refuses to run at all if the version already has a published (non-draft, non-prerelease) GitHub Release,
      so a deleted-and-re-pushed tag cannot rebuild over artifacts customers already have.
   3. Builds the DEB/RPM packages, the macOS `.pkg` installers (one per architecture, on a `macos-15`
      runner) and the Docker image.
   4. Apple-signs and notarizes the macOS packages, then GPG-signs everything. Apple signing runs first
      because `productsign` rewrites the `.pkg` in place, which would invalidate a detached `.asc` created
      beforehand.
   5. Verifies every artifact carries both a valid detached `.asc` and a valid embedded deb/rpm signature,
      and that each `.pkg` is signed by the expected Developer ID, accepted by Gatekeeper and carries a
      stapled notarization ticket — before anything is deployed. Then deploys everything to JFrog `DEV`.
   6. Creates a unified release bundle and automatically promotes it from `DEV` to `TEST`.
2. QE/developers pull the artifacts from JFrog `TEST` and validate them. Once they pass, the release bundle is
   promoted from `TEST` to `STAGE`, either by dispatching
   [`promote-to-preview.yml`](https://github.com/aerospike/absctl/actions/workflows/promote-to-preview.yml) with `environment: STAGE` or manually via the
   [JFrog UI](https://aerospike.jfrog.io/ui/artifactory/release-lifecycle/absctl?repoKey=database-release-bundles-v2).
3. A PM or EM reviews the release and promotes the release bundle from `STAGE` to `PREVIEW`, either by dispatching
   [`promote-to-preview.yml`](https://github.com/aerospike/absctl/actions/workflows/promote-to-preview.yml) with `environment: PREVIEW` or manually via the same
   [JFrog UI](https://aerospike.jfrog.io/ui/artifactory/release-lifecycle/absctl?repoKey=database-release-bundles-v2)
   link.
4. A PM or EM promotes the release bundle from `PREVIEW` to `PROD`, either by dispatching
   [`promote-to-prod.yml`](https://github.com/aerospike/absctl/actions/workflows/promote-to-prod.yml) or manually via the same JFrog UI link. This is
   the gate that makes a release public.
5. Once the bundle is on `PROD`:
   - Docker Hub and every other public registry are published automatically and externally by the central
     `artifact-publisher` repository, off JFrog's `release_bundle_v2_promotion_completed` webhook — nothing
     to trigger here, and nothing in this repository ever pushes to an external registry
     ([strategy](https://aerospike.atlassian.net/wiki/spaces/DevOps/pages/4648566799)).
   - A dev or PM/EM manually runs [`release.yml`](https://github.com/aerospike/absctl/actions/workflows/release.yml)
     (`workflow_dispatch`, with the release version as input). It verifies the bundle was actually promoted to
     `PROD`, then downloads the already-signed DEB/RPM/PKG artifacts straight from JFrog's `PROD`-public
     repos and publishes them as a new, immutable GitHub **pre-release** — nothing is rebuilt, re-signed, or
     re-checksummed at this point. The macOS packages come from the generic repo, since `.pkg` has no
     dedicated artifact type in shared-workflows' deploy type registry.
6. When ready to announce GA, a PM/EM edits that GitHub Release and clears **Set as a pre-release** only.
   The GitHub **Set as the latest release** checkbox is unrelated to any registry tag and can be left
   unchecked. Until the pre-release flag is cleared, the release does not appear as GA on GitHub.
7. Post-release actions (after step 6):
   1. **Snyk**:
      - Add the new version to the `aerospike-applications` Snyk org.
      - Remove the oldest maintenance version from the same org if no longer supported.
   2. **Slack**:
      - Post the release announcement to the internal **`#releases`** channel.
      - Use the link to the GitHub Release.
      - **Important**: Remove link previews before sending to keep the channel clean (hover over the preview and click the **'x'** in the top-right corner). See [this guide](https://aerospike.atlassian.net/wiki/spaces/RE/pages/2540339350/Message+Slack+releases+Internal+Channel) for more info.
   3. **Email**: Send the release announcement email. See [this guide](https://aerospike.atlassian.net/wiki/spaces/RE/pages/2543124552/Send+email+of+the+Release+Notes+to+the+releases+aerospike.com+distribution+list) for more info.
8. Back-merge anything that is not yet on `dev`: `main` after a regular release, or the hotfix branch itself
   after a hotfix (a hotfix is tagged from its own branch and never merged into `main`, so `main` does not
   carry those commits).

## License

Apache License, Version 2.0. See [LICENSE](LICENSE) for details.

## Support

- **Documentation**: [Aerospike Documentation](https://aerospike.com/docs/tools/backup/)
- **Issues**: [GitHub Issues](https://github.com/aerospike/absctl/issues)
- **Community**: [Aerospike Community Forum](https://discuss.aerospike.com/)
