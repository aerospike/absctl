# Aerospike Backup Service Control
[![Tests](https://github.com/aerospike/absctl/actions/workflows/tests.yml/badge.svg)](https://github.com/aerospike/absctl/actions/workflows/tests.yml/badge.svg)
[![PkgGoDev](https://pkg.go.dev/badge/github.com/aerospike/absctl)](https://pkg.go.dev/github.com/aerospike/absctl)
[![codecov](https://codecov.io/gh/aerospike/absctl/graph/badge.svg?token=29G65BU7QX)](https://codecov.io/gh/aerospike/absctl)

The repository includes the [backup](docs/backup/readme.md) and [restore](docs/restore/readme.md) CLI tools,
built using [backup-go](https://github.com/aerospike/backup-go) library.
Refer to their respective README files for usage instructions.
Binaries for various platforms are released alongside the library and can be found under
[releases](https://github.com/aerospike/absctl/releases).

## Core Features

### Standard Operations
- **Full backups**: Complete namespace or set backups
- **Incremental backups**: Time-based filtering for changed records
- **Parallel processing**: Configurable workers for optimal performance
- **Resume capability**: Continue interrupted backups from state files

### Advanced Filtering
- **Set-based**: Backup specific sets within namespaces
- **Bin filtering**: Include only specified bins
- **Time windows**: Records modified within date ranges
- **Partition filtering**: Backup specific partition ranges
- **Node/Rack targeting**: Geographic or hardware-specific backups

### Enterprise Features
- **Compression**: ZSTD compression for reduced storage
- **Encryption**: AES-128/256 encryption for data security
- **Cloud storage**: Direct backup to AWS S3, GCP Storage, Azure Blob
- **Secret management**: Integration with Aerospike Secret Agent
- **Rate limiting**: Bandwidth and RPS controls

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

# Run backup
docker run -v <host-path>:<container-path>  aerospike/absctl:<version> absctl backup -h <aerospike-address>  -n <namespace> -d <container-path>

# Run restore
docker run -v <host-path>:<container-path>  aerospike/absctl:<version> absctl restore -h <aerospike-address>  -n <namespace> -d <container-path>
```

## Quick Start

### Basic Backup
```bash
# Simple namespace backup
absctl backup -h 127.0.0.1:3000 -n test -d /backup/test-namespace
```

### Basic Restore
```bash
# Restore from backup directory
absctl restore -h 127.0.0.1:3000 -n test -d /backup/test-namespace
```


## Configuration Reference

Please look at [backup](docs/backup/readme.md#configuration-file-schema-with-example-values) and [restore](docs/restore/readme.md#configuration-file-schema-with-example-values) readme files for details.

## Releasing

Releases move through JFrog's promotion stages (`DEV -> TEST -> STAGE -> PROD`) before anything is made public. The
GitHub Actions side is split into two workflows:
[`pre-release.yml`](.github/workflows/pre-release.yml) (developer owned, builds and promotes up to `TEST`) and
[`release.yml`](.github/workflows/release.yml) (run once the release is fully approved, publishes it).

1. Tag the release commit on `main` with a semver tag: `git tag v1.x.y && git push origin v1.x.y`. This triggers
   `pre-release.yml`, which:
   1. Runs GoReleaser to build and publish the cross-platform binary archives directly to GitHub (unchanged by the
      flow below — GoReleaser's output bypasses JFrog entirely).
   2. Builds the DEB/RPM packages and Docker image.
   3. Signs the packages and deploys everything to JFrog `DEV`.
   4. Creates a unified release bundle and automatically promotes it from `DEV` to `TEST`.
2. QE/developers pull the artifacts from JFrog `TEST` and validate them. Once they pass, the release bundle is
   promoted from `TEST` to `STAGE`, either by dispatching
   [`promote-to-stage.yml`](.github/workflows/promote-to-stage.yml) or manually via the
   [JFrog UI](https://aerospike.jfrog.io/ui/artifactory/release-lifecycle/absctl?repoKey=database-release-bundles-v2).
3. A PM or EM reviews the release and promotes the release bundle from `STAGE` to `PROD`, either by dispatching
   [`promote-to-prod.yml`](.github/workflows/promote-to-prod.yml) or manually via the same
   [JFrog UI](https://aerospike.jfrog.io/ui/artifactory/release-lifecycle/absctl?repoKey=database-release-bundles-v2)
   link. This is the gate that makes a release public; automation intentionally stops before this step.
4. Once the bundle is on `PROD`:
   - Docker Hub mirroring happens automatically and externally (JFrog's existing promotion webhook feeds
     `artifact-publisher`) — nothing to trigger here.
   - Someone with repo access (not necessarily the same PM/EM from step 3) manually runs `release.yml`
     (`workflow_dispatch`, with the release version as input). It verifies the bundle was actually promoted to
     `PROD`, then downloads the already-signed DEB/RPM artifacts straight from JFrog's `PROD`-public repos and
     publishes them as a new, immutable GitHub Release — nothing is rebuilt, re-signed, or re-checksummed at this
     point.

## License

Apache License, Version 2.0. See [LICENSE](LICENSE) file for details.

## Support

- **Documentation**: [Aerospike Documentation](https://aerospike.com/docs/tools/backup/)
- **Issues**: [GitHub Issues](https://github.com/aerospike/absctl/issues)
- **Community**: [Aerospike Community Forum](https://discuss.aerospike.com/)
