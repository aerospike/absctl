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
OS ?= $(shell $(GO) env GOOS)
ARCH ?= $(shell $(GO) env GOARCH)
REGISTRY ?= "docker.io"
GIT_COMMIT:=$(shell git rev-parse --short HEAD)
GOBUILD = GOOS=$(OS) GOARCH=$(ARCH) CGO_ENABLED=0 $(GO) build \
-ldflags="-s -w -X 'main.appVersion=$(VERSION)' -X 'main.commitHash=$(GIT_COMMIT)' -X 'main.buildTime=$(shell date -u +'%Y-%m-%dT%H:%M:%SZ')'"
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


.PHONY: test
test:
	$(GOTEST) -parallel $(NPROC) -timeout=5m -count=1 -v ./...

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
    	--output "$(IMAGE_OUTPUT)"

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
