FROM golang:1.25 AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ENV CGO_ENABLED=0
RUN GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o /out/pbs-plus-agent ./cmd/unix_agent

FROM alpine:3.20

ARG S6_OVERLAY_VERSION=v3.1.6.2
ARG TARGETARCH=amd64
ARG USER_NAME=pbsplus
ARG USER_UID=1999
ARG USER_GID=1999
ARG BIN_PATH=/usr/bin/pbs-plus-agent

RUN apk add --no-cache ca-certificates tzdata libcap shadow

ADD https://github.com/just-containers/s6-overlay/releases/download/${S6_OVERLAY_VERSION}/s6-overlay-noarch.tar.xz /tmp/
ADD https://github.com/just-containers/s6-overlay/releases/download/${S6_OVERLAY_VERSION}/s6-overlay-${TARGETARCH}.tar.xz /tmp/
RUN tar -C / -Jxpf /tmp/s6-overlay-noarch.tar.xz && \
  tar -C / -Jxpf /tmp/s6-overlay-${TARGETARCH}.tar.xz && \
  rm -f /tmp/s6-overlay-noarch.tar.xz /tmp/s6-overlay-${TARGETARCH}.tar.xz

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
  S6_CMD_WAIT_FOR_SERVICES_MAXTIME=30000

ENTRYPOINT ["/init"]
