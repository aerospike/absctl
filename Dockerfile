# syntax=docker/dockerfile:1.12.0

# Minor-version tag so plain `docker build` works even before a new Go patch image is
# published; release builds pass an exact GO_VERSION (see qa-build.yml).
ARG GO_VERSION=1.27
ARG REGISTRY="docker.io"

# Build-time only (never shipped), so pin exactly; bump tag and digest together.
# The golang builder is not digest-pinned: its tag comes from GO_VERSION, which release
# builds resolve per run.
FROM --platform=$BUILDPLATFORM ${REGISTRY}/tonistiigi/xx:1.10.0@sha256:40fe21f44bb8cddf1eb1226db190d0a984118f62ee02a91b77d99448d28264a6 AS xx
FROM --platform=$BUILDPLATFORM ${REGISTRY}/golang:${GO_VERSION} AS builder

ARG TARGETOS
ARG TARGETARCH
ARG VERSION
ARG BUILD_MODE=release

COPY --from=xx / /

WORKDIR /app/absctl
COPY . .

RUN xx-go --wrap

RUN --mount=type=secret,id=GOPROXY <<-EOF
    set -e
    if [ -s /run/secrets/GOPROXY ]; then
        export GOPROXY="$(cat /run/secrets/GOPROXY)"
    fi
    go mod download
EOF

RUN --mount=type=secret,id=GOPROXY <<-EOF
    set -e
    if [ -s /run/secrets/GOPROXY ]; then
        export GOPROXY="$(cat /run/secrets/GOPROXY)"
    fi
    OS="${TARGETOS}" ARCH="${TARGETARCH}" VERSION="${VERSION}" make build BUILD_MODE="${BUILD_MODE}"
    xx-verify /app/absctl/dist/absctl_${TARGETOS}_${TARGETARCH}
EOF

# Pinned to the Alpine 3.24 release line: the `3.24` tag always resolves to the
# newest 3.24.x patch release, so rebuilds pick up fixes without jumping to 3.25.
FROM ${REGISTRY}/alpine:3.24

ARG TARGETOS
ARG TARGETARCH

# Pick up package fixes published since the base image was built.
RUN apk upgrade --no-cache

# adduser and addgroup are provided by busybox (no shadow package needed)
RUN addgroup -g 65532 -S abtgroup && \
    adduser -S -u 65532 -G abtgroup -h /home/abtuser abtuser

# Owned by root so the runtime user cannot replace its own binary.
COPY --chown=0:0 --chmod=0755 --from=builder \
    /app/absctl/dist/absctl_${TARGETOS}_${TARGETARCH} \
    /usr/bin/absctl

# Run absctl by default. A leading "absctl" argument is dropped so the documented
# `docker run <image> absctl backup ...` form keeps working.
COPY --chmod=0755 <<-'EOF' /usr/local/bin/docker-entrypoint.sh
	#!/bin/sh
	case "$1" in
	    absctl|/usr/bin/absctl) shift ;;
	esac
	exec /usr/bin/absctl "$@"
EOF

USER abtuser

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
CMD ["--help"]
