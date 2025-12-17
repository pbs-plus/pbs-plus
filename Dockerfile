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

FROM alpine:3.23

ARG USER_NAME=pbsplus
ARG USER_UID=1999
ARG USER_GID=1999
ARG BIN_PATH=/usr/bin/pbs-plus-agent

# Needed for adjusting UID/GID and general runtime
RUN apk add --no-cache ca-certificates tzdata libcap shadow su-exec

# Create group and user (system, no home population, no shell)
RUN addgroup -g ${USER_GID} -S ${USER_NAME} && \
  adduser -u ${USER_UID} -S -D -H -G ${USER_NAME} -s /sbin/nologin ${USER_NAME}

# Runtime dirs
RUN mkdir -p /var/lib/pbs-plus-agent /var/log/pbs-plus-agent /run/pbs-plus-agent /etc/pbs-plus-agent && \
  chown -R ${USER_NAME}:${USER_NAME} /var/lib/pbs-plus-agent /var/log/pbs-plus-agent /run/pbs-plus-agent /etc/pbs-plus-agent && \
  chmod 0750 /var/lib/pbs-plus-agent /var/log/pbs-plus-agent /etc/pbs-plus-agent && \
  chmod 0755 /run/pbs-plus-agent

# Binary
COPY --from=builder /out/pbs-plus-agent ${BIN_PATH}
RUN chmod 0755 ${BIN_PATH} && chown root:${USER_NAME} ${BIN_PATH}

# Copy init script
COPY build/container/init.sh /usr/local/bin/init.sh
RUN chmod 0755 /usr/local/bin/init.sh

ENV HOME=/var/lib/pbs-plus-agent \
  USER=${USER_NAME}

ENV PBS_PLUS__I_AM_INSIDE_CONTAINER=true
ENV PBS_PLUS_DISABLE_AUTO_UPDATE=true

# Require using the init script
ENTRYPOINT ["/usr/local/bin/init.sh"]
CMD ["pbs-plus-agent"]
