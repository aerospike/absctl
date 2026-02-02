# Aerospike Backup Service Control
[![Tests](https://github.com/aerospike/absctl/actions/workflows/tests.yml/badge.svg)](https://github.com/aerospike/absctl/actions/workflows/tests.yml/badge.svg)
[![PkgGoDev](https://pkg.go.dev/badge/github.com/aerospike/absctl)](https://pkg.go.dev/github.com/aerospike/absctl)
[![codecov](https://codecov.io/gh/aerospike/absctl/graph/badge.svg?token=29G65BU7QX)](https://codecov.io/gh/aerospike/absctl)

The repository includes the [backup](cmd/absctl/cmd/backup/readme.md) and [restore](cmd/absctl/cmd/restore/readme.md) CLI tools,
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
# Build binaries
make build

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

Please look at [backup](cmd/absctl/cmd/backup/readme.md) and [restore](cmd/absctl/cmd/restore/readme.md) readme files for details.

## License

Apache License, Version 2.0. See [LICENSE](LICENSE) file for details.

## Support

- **Documentation**: [Aerospike Documentation](https://aerospike.com/docs/tools/backup/)
- **Issues**: [GitHub Issues](https://github.com/aerospike/absctl/issues)
- **Community**: [Aerospike Community Forum](https://discuss.aerospike.com/)