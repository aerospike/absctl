# This make file is used for test.

# Default values
VERSION := $(shell git describe --tags --exact-match 2>/dev/null || git rev-parse --abbrev-ref HEAD)
COMMIT := $(shell git rev-parse --short HEAD)
LDFLAGS := -ldflags "-X 'main.appVersion=$(VERSION)' -X 'main.commitHash=$(COMMIT)' -X 'main.buildTime=$(shell date -u +'%Y-%m-%dT%H:%M:%SZ')'"
OUTPUT := absctl

# Build the application
build:
	@echo "Building $(OUTPUT) with version $(VERSION)..."
	go build $(LDFLAGS) -o $(OUTPUT) main.go

# Clean the build
clean:
	rm -f $(OUTPUT)
