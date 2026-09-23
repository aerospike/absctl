SHELL = bash
NAME = absctl
WORKSPACE = $(shell pwd)
VERSION ?= $(shell git describe --tags --exact-match 2>/dev/null || git rev-parse --abbrev-ref HEAD)
MAINTAINER = "Aerospike <info@aerospike.com>"
DESCRIPTION = "Aerospike Backup Tools"
HOMEPAGE = "https://www.aerospike.com"
VENDOR = "Aerospike INC"
LICENSE = "Apache License 2.0"

GO ?= $(shell which go || echo "/usr/local/go/bin/go")
NFPM ?= $(shell which nfpm)
COMPOSE ?= docker compose -f docker-compose.test.yaml
OS ?= $(shell $(GO) env GOOS)
ARCH ?= $(shell $(GO) env GOARCH)
REGISTRY ?= "docker.io"
GIT_COMMIT:=$(shell git rev-parse --short HEAD)
BUILD_MODE ?= release

ifeq ($(filter debug release,$(BUILD_MODE)),)
$(error BUILD_MODE must be 'debug' or 'release', got '$(BUILD_MODE)')
endif

LDFLAGS_COMMON = \
-X 'main.appVersion=$(VERSION)' \
-X 'main.commitHash=$(GIT_COMMIT)' \
-X 'main.buildTime=$(shell date -u +'%Y-%m-%dT%H:%M:%SZ')'

GOBUILD_debug   = GOOS=$(OS) GOARCH=$(ARCH) CGO_ENABLED=0 $(GO) build -tags debug -gcflags "all=-N -l" -ldflags="$(LDFLAGS_COMMON)"
GOBUILD_release = GOOS=$(OS) GOARCH=$(ARCH) CGO_ENABLED=0 $(GO) build -trimpath -ldflags="-s -w $(LDFLAGS_COMMON)"

GOBUILD = $(GOBUILD_$(BUILD_MODE))
GOTEST = $(GO) test
NPROC := $(shell nproc 2>/dev/null || getconf _NPROCESSORS_ONLN)
ARCHS ?= linux/amd64 linux/arm64
PACKAGERS ?= deb rpm
IMAGE_TAG ?= test
IMAGE_REPO ?= aerospike/absctl
IMAGE_CACHE_FROM ?=
IMAGE_CACHE_TO ?=
IMAGE_OUTPUT ?= type=image,push=true
BINARY_NAME = absctl
TARGET_DIR = $(WORKSPACE)/dist
BIN_DIR = $(WORKSPACE)/bin
PACKAGE_DIR= $(WORKSPACE)/scripts/package
CMD_DIR = $(WORKSPACE)/cmd/absctl

PREFIX ?= /usr
BINDIR ?= $(PREFIX)/bin
DESTDIR ?=

# macOS .pkg installer. The naming convention follows aerospike-admin's
# pkg/Makefile so a Homebrew cask can interpolate it: lower case, '-' separated,
# and <arch> spelled the way `uname -m` reports it (the token a cask's `arch`
# stanza emits). Deliberately no .dmg -- a CLI under /usr/local has nothing to
# drag, and a disk image would be a second container to sign, notarize and
# staple.
#
# One pkg per architecture, not per macOS generation: macOS is forward
# compatible, so a single build serves every supported release.
#
#     absctl-<version>-macos-<arch>.pkg
#
# VERSION is the git tag (v1.2.0); the package carries it without the leading
# 'v', matching what nfpm already does for the deb and rpm.
PKG_VERSION = $(patsubst v%,%,$(VERSION))
# Go spells the 64-bit Intel arch "amd64"; `uname -m` and the file name above
# say "x86_64".
MAC_ARCH = $(patsubst amd64,x86_64,$(ARCH))
# pkgbuild --version rejects hyphens and underscores, so collapse both to dots
# (1.2.0-rc1 -> 1.2.0.rc1). This is the CFBundleShortVersionString-style version
# *inside* the pkg only; the file name keeps the canonical form.
MAC_VERSION = $(subst _,.,$(subst -,.,$(PKG_VERSION)))
MAC_PKG_ID = com.aerospike.absctl
# Staged under TARGET_DIR so `clean` already removes it.
MAC_ROOT = $(TARGET_DIR)/mac-root-$(MAC_ARCH)
MAC_PKG = $(TARGET_DIR)/$(NAME)-$(PKG_VERSION)-macos-$(MAC_ARCH).pkg
# Both macOS packages are produced from one runner: absctl is a single
# CGO_ENABLED=0 binary, so GOARCH cross-compiles, and pkgbuild only stages
# files -- it does not care about the payload's architecture.
MAC_ARCHS ?= arm64 amd64

# Runs the unit tests. Tests that need a live Aerospike cluster, MinIO, Azurite
# or fake-gcs-server skip themselves; use test-integration to include them.
.PHONY: test
test:
	$(GOTEST) -parallel $(NPROC) -timeout=5m -count=1 -v ./...

# Runs the full suite, including the tests that need external services.
# Start them first with test-env-up.
.PHONY: test-integration
test-integration:
	ABSCTL_INTEGRATION=1 $(GOTEST) -parallel $(NPROC) -timeout=5m -count=1 -v ./...

# Starts the services the integration tests need and waits until they are ready.
# minio-init is run separately because `up --wait` treats a container that exits,
# even successfully, as a failure.
.PHONY: test-env-up
test-env-up:
	$(COMPOSE) up -d --wait
	$(COMPOSE) run --rm minio-init

# --profile init is needed for down to also clean up the minio-init container,
# since compose ignores services whose profile is not active.
.PHONY: test-env-down
test-env-down:
	$(COMPOSE) --profile init down -v

.PHONY: coverage
coverage:
	$(GOTEST) -parallel $(NPROC) -timeout=5m -count=1 ./... -coverprofile to_filter.cov -coverpkg ./...
	grep -v "test\|mocks" to_filter.cov > coverage.cov
	rm -f to_filter.cov
	$(GO) tool cover -func coverage.cov

.PHONY: clean
clean:
	rm -Rf $(BIN_DIR)
	rm -Rf $(TARGET_DIR)
	@find . -type f -name 'nfpm-linux-*.yaml' -exec rm -v {} +

# Build release locally.
.PHONY: release-test
release-test:
	@echo "Testing release with version $(VERSION)..."
	goreleaser build --snapshot

.PHONY: docker-build
docker-build:
	 DOCKER_BUILDKIT=1 docker build \
 	--progress=plain \
 	--tag $(IMAGE_REPO):$(IMAGE_TAG) \
 	--build-arg REGISTRY=$(REGISTRY) \
 	--build-arg BUILD_MODE=$(BUILD_MODE) \
 	--file $(WORKSPACE)/Dockerfile .

.PHONY: docker-buildx
docker-buildx:
		cd ./scripts && ./docker-buildx.sh \
    	--repo $(IMAGE_REPO) \
    	--tag $(IMAGE_TAG) \
    	--registry $(REGISTRY) \
    	--version $(VERSION) \
    	--platforms "$(ARCHS)" \
    	--cache-to "$(IMAGE_CACHE_TO)" \
    	--cache-from "$(IMAGE_CACHE_FROM)" \
    	--output "$(IMAGE_OUTPUT)" \
    	--build-mode "$(BUILD_MODE)"

.PHONY: build
build:
	mkdir -p "$(TARGET_DIR)"
	@echo "Building $(BINARY_NAME) with version $(VERSION)..."
	$(GOBUILD) -o $(TARGET_DIR)/$(BINARY_NAME)_$(OS)_$(ARCH) $(CMD_DIR)

# Build for aerospike-tools
.PHONY: tools-build
tools-build:
	mkdir -p "$(BIN_DIR)"
	@echo "Building $(BINARY_NAME) for tools with version $(VERSION)..."
	$(GOBUILD) -o $(BIN_DIR)/$(BINARY_NAME) $(CMD_DIR)

.PHONY: buildx
buildx:
	@for arch in $(ARCHS); do \
  		OS=$$(echo $$arch | cut -d/ -f1); \
  		ARCH=$$(echo $$arch | cut -d/ -f2); \
  		OS=$$OS ARCH=$$ARCH $(MAKE) build; \
  	done

.PHONY: install
install: build
	install -d $(DESTDIR)$(BINDIR)
	install -m 755 $(TARGET_DIR)/$(BINARY_NAME)_$(OS)_$(ARCH) $(DESTDIR)$(BINDIR)/$(BINARY_NAME)

.PHONY: uninstall
uninstall:
	rm -f $(DESTDIR)$(BINDIR)/$(BINARY_NAME)

# Build the macOS installer for a single architecture (ARCH=arm64|amd64).
# Guards first: a misnamed package is worse than a failed build, because the
# name is the customer-facing contract and downstream jobs select on it.
.PHONY: mac-pkg
mac-pkg:
	@test -n "$(PKG_VERSION)" \
		|| { echo "ERROR: empty version -- refusing to build an unnamed pkg" >&2; exit 1; }
	@echo '$(MAC_ARCH)' | grep -qE '^(arm64|x86_64)$$' \
		|| { echo "ERROR: architecture token is '$(MAC_ARCH)', not arm64 or x86_64 -- refusing to build a misnamed pkg" >&2; exit 1; }
	@command -v pkgbuild >/dev/null 2>&1 \
		|| { echo "ERROR: pkgbuild not found -- mac-pkg must run on macOS" >&2; exit 1; }
	$(MAKE) build OS=darwin ARCH=$(ARCH)
	rm -rf $(MAC_ROOT)
	install -d $(MAC_ROOT)/usr/local/bin
	install -m 755 $(TARGET_DIR)/$(BINARY_NAME)_darwin_$(ARCH) $(MAC_ROOT)/usr/local/bin/$(BINARY_NAME)
	pkgbuild \
		--root $(MAC_ROOT) \
		--identifier $(MAC_PKG_ID) \
		--version $(MAC_VERSION) \
		--install-location / \
		$(MAC_PKG)
	@echo "==> built $(MAC_PKG)"

# Build the macOS installer for every architecture in MAC_ARCHS.
.PHONY: mac-packages
mac-packages:
	@for arch in $(MAC_ARCHS); do \
		$(MAKE) mac-pkg ARCH=$$arch || exit 1; \
	done

.PHONY: packages
packages: buildx
	@for arch in $(ARCHS); do \
  		OS=$$(echo $$arch | cut -d/ -f1); \
  		ARCH=$$(echo $$arch | cut -d/ -f2); \
		OS=$$OS ARCH=$$ARCH \
		NAME=$(NAME) \
		VERSION=$(VERSION) \
		WORKSPACE=$(WORKSPACE) \
		MAINTAINER=$(MAINTAINER) \
		DESCRIPTION=$(DESCRIPTION) \
		HOMEPAGE=$(HOMEPAGE) \
		VENDOR=$(VENDOR) \
		LICENSE=$(LICENSE) \
		BINARY_NAME=$(BINARY_NAME) \
		envsubst '$$OS $$ARCH $$NAME $$VERSION $$WORKSPACE $$MAINTAINER $$DESCRIPTION $$HOMEPAGE $$VENDOR $$LICENSE $$BINARY_NAME' \
		< $(PACKAGE_DIR)/nfpm.tmpl.yaml > $(PACKAGE_DIR)/nfpm-$$OS-$$ARCH.yaml; \
		for packager in $(PACKAGERS); do \
			$(NFPM) package \
			--config $(PACKAGE_DIR)/nfpm-$$OS-$$ARCH.yaml \
			--packager $$(echo $$packager) \
			--target $(TARGET_DIR); \
			done; \
  	done; \

.PHONY: checksums
checksums:
	@find . -type f \
		\( -name '*.deb' -o -name '*.rpm' \) \
		-exec sh -c 'sha256sum "$$1" | cut -d" " -f1 > "$$1.sha256"' _ {} \;

.PHONY: vulnerability-scan
vulnerability-scan:
	snyk test --all-projects --policy-path=$(WORKSPACE)/.snyk --severity-threshold=high

.PHONY: vulnerability-scan-container
vulnerability-scan-container:
	snyk container test $(IMAGE_REPO):$(IMAGE_TAG) \
	--policy-path=$(WORKSPACE)/.snyk \
	--file=Dockerfile \
	--severity-threshold=high

.PHONY: docs-generate
docs-generate:
	@echo "Building documentation..."
	$(GO) run docs/docgen/main.go

.PHONY: api-generate
api-generate:
	@echo "Building ABS api client..."
	$(GO) generate ./...
