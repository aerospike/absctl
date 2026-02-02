# syntax=docker/dockerfile:1.12.0

ARG GO_VERSION=1.23.10
ARG REGISTRY="docker.io"

FROM --platform=$BUILDPLATFORM ${REGISTRY}/tonistiigi/xx AS xx
FROM --platform=$BUILDPLATFORM ${REGISTRY}/golang:${GO_VERSION} AS builder

ARG TARGETOS
ARG TARGETARCH
ARG VERSION

COPY --from=xx / /

WORKDIR /app/absctl
COPY . .

RUN xx-go --wrap

RUN --mount=type=secret,id=GOPROXY <<-EOF
    if [ -s /run/secrets/GOPROXY ]; then
        export GOPROXY="$(cat /run/secrets/GOPROXY)"
    fi
    go mod download
EOF

RUN --mount=type=secret,id=GOPROXY <<-EOF
    if [ -s /run/secrets/GOPROXY ]; then
        export GOPROXY="$(cat /run/secrets/GOPROXY)"
    fi
    OS=${TARGETOS} ARCH=${TARGETARCH} VERSION=${VERSION} make build
    xx-verify /app/absctl/dist/absctl_${TARGETOS}_${TARGETARCH}
EOF

FROM ${REGISTRY}/alpine:3.23

ARG TARGETOS
ARG TARGETARCH

RUN apk update && \
    apk upgrade --no-cache

# adduser and addgroup are provided by busybox (no shadow package needed)
RUN addgroup -g 65532 -S abtgroup && \
    adduser -S -u 65532 -G abtgroup -h /home/abtuser abtuser

COPY --chown=abtuser:abtgroup --chmod=0755 --from=builder \
    /app/absctl/dist/absctl_${TARGETOS}_${TARGETARCH} \
    /usr/bin/absctl

USER abtuser
