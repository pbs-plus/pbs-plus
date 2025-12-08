FROM golang:1.25 AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG TARGETOS=linux
ARG TARGETARCH=amd64
ENV CGO_ENABLED=0
RUN GOOS=$TARGETOS GOARCH=$TARGETARCH \
  go build -trimpath -ldflags="-s -w" -o /out/pbs-plus-agent ./cmd/unix_agent

FROM alpine:3.20

ARG S6_OVERLAY_VERSION=v3.2.1.0
ARG TARGETARCH
ARG USER_NAME=pbsplus
ARG USER_UID=1999
ARG USER_GID=1999
ARG BIN_PATH=/usr/bin/pbs-plus-agent

# Map Docker TARGETARCH -> s6-overlay archive arch
# amd64 -> x86_64, arm64 -> aarch64, arm -> armhf, 386 -> i686
# Default to x86_64 as a safe fallback
RUN case "$TARGETARCH" in \
  amd64)  S6_ARCH="x86_64" ;; \
  arm64)  S6_ARCH="aarch64" ;; \
  arm)    S6_ARCH="armhf" ;; \
  386)    S6_ARCH="i686" ;; \
  *)      S6_ARCH="x86_64" ;; \
  esac && \
  echo "Using s6-overlay arch: ${S6_ARCH}" && \
  apk add --no-cache ca-certificates tzdata libcap shadow && \
  wget -O /tmp/s6-overlay-noarch.tar.xz \
  "https://github.com/just-containers/s6-overlay/releases/download/${S6_OVERLAY_VERSION}/s6-overlay-noarch.tar.xz" && \
  wget -O /tmp/s6-overlay-${S6_ARCH}.tar.xz \
  "https://github.com/just-containers/s6-overlay/releases/download/${S6_OVERLAY_VERSION}/s6-overlay-${S6_ARCH}.tar.xz" && \
  tar -C / -Jxpf /tmp/s6-overlay-noarch.tar.xz && \
  tar -C / -Jxpf /tmp/s6-overlay-${S6_ARCH}.tar.xz && \
  rm -f /tmp/s6-overlay-noarch.tar.xz /tmp/s6-overlay-${S6_ARCH}.tar.xz

RUN addgroup -g ${USER_GID} -S ${USER_NAME} && \
  adduser  -u ${USER_UID} -S -D -H -G ${USER_NAME} -s /sbin/nologin ${USER_NAME}

RUN mkdir -p /var/lib/pbs-plus-agent /var/log/pbs-plus-agent /run/pbs-plus-agent /etc/pbs-plus-agent && \
  chown -R ${USER_NAME}:${USER_NAME} /var/lib/pbs-plus-agent /var/log/pbs-plus-agent /run/pbs-plus-agent /etc/pbs-plus-agent && \
  chmod 0750 /var/lib/pbs-plus-agent /var/log/pbs-plus-agent /etc/pbs-plus-agent && \
  chmod 0755 /run/pbs-plus-agent

COPY --from=builder /out/pbs-plus-agent ${BIN_PATH}
RUN chmod 0755 ${BIN_PATH} && chown root:${USER_NAME} ${BIN_PATH}

COPY build/container/ /

ENV HOME=/var/lib/pbs-plus-agent \
  USER=${USER_NAME} \
  S6_BEHAVIOUR_IF_STAGE2_FAILS=2 \
  S6_CMD_WAIT_FOR_SERVICES=1 

ENTRYPOINT ["/init"]
