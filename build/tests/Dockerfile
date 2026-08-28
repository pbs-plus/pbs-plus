FROM golang:1.26 AS builder

WORKDIR /build

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN GOFIPS140=latest go build -o pbs-plus ./cmd/pbs-plus && \
    GOFIPS140=latest go build -o pxar-mount ./cmd/pxar-mount

FROM ghcr.io/pbs-plus/proxmox-backup-docker:latest

RUN rm -f /etc/apt/sources.list.d/*enterprise*.list /etc/apt/sources.list.d/*enterprise*.sources && \
    apt-get update && apt-get install -y --no-install-recommends jq && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/pbs-plus /usr/bin/pbs-plus
COPY --from=builder /build/pxar-mount /usr/bin/pxar-mount

RUN chmod 0755 /usr/bin/pbs-plus

RUN install -d -m 0755 -o root -g root /etc/proxmox-backup/pbs-plus

RUN printf '%s\n' \
  '#!/command/with-contenv bash' \
  'set -e' \
  '' \
  'echo "Initializing Proxmox Backup Server..."' \
  '' \
  'chown backup:backup /run/proxmox-backup' \
  'chmod 755 /run/proxmox-backup' \
  '' \
  'chown -R backup:backup /etc/proxmox-backup /var/lib/proxmox-backup /var/log/proxmox-backup' \
  'chmod 700 /etc/proxmox-backup' \
  '' \
  'if [ -n "$ROOT_PASSWORD" ]; then' \
  '    echo "Setting root password..."' \
  '    echo "root:$ROOT_PASSWORD" | chpasswd' \
  '    unset ROOT_PASSWORD' \
  'fi' \
  '' \
  'if [ ! -f /etc/proxmox-backup/authkey.key ]; then' \
  '    echo "Generating authentication keys..."' \
  '    openssl genrsa -out /etc/proxmox-backup/authkey.key 4096' \
  '    openssl rsa -in /etc/proxmox-backup/authkey.key -pubout -out /etc/proxmox-backup/authkey.pub' \
  '    chown backup:backup /etc/proxmox-backup/authkey.key /etc/proxmox-backup/authkey.pub' \
  '    chmod 600 /etc/proxmox-backup/authkey.key /etc/proxmox-backup/authkey.pub' \
  'fi' \
  '' \
  'if [ ! -f /etc/proxmox-backup/csrf.key ]; then' \
  '    echo "Generating CSRF key..."' \
  '    openssl rand -base64 32 > /etc/proxmox-backup/csrf.key' \
  '    chown backup:backup /etc/proxmox-backup/csrf.key' \
  '    chmod 600 /etc/proxmox-backup/csrf.key' \
  'fi' \
  '' \
  'if [ ! -f /etc/proxmox-backup/proxy.pem ]; then' \
  '    echo "Generating proxy certificates if needed"' \
  '    HOSTNAME=$(hostname -f 2>/dev/null || hostname)' \
  '    openssl req -x509 -newkey rsa:4096 -nodes \' \
  '        -keyout /etc/proxmox-backup/proxy.key \' \
  '        -out /etc/proxmox-backup/proxy.pem \' \
  '        -days 3650 -subj "/CN=${HOSTNAME}"' \
  '    chown backup:backup /etc/proxmox-backup/proxy.pem /etc/proxmox-backup/proxy.key' \
  '    chmod 640 /etc/proxmox-backup/proxy.pem /etc/proxmox-backup/proxy.key' \
  'fi' \
  '' \
  'if [ ! -f /etc/proxmox-backup/domains.cfg ]; then' \
  '    echo "Creating default domains config..."' \
  '    touch /etc/proxmox-backup/domains.cfg' \
  '    chown backup:backup /etc/proxmox-backup/domains.cfg' \
  '    chmod 640 /etc/proxmox-backup/domains.cfg' \
  'fi' \
  '' \
  'mkdir -p /etc/proxmox-backup/pbs-plus' \
  'if [ ! -f /etc/proxmox-backup/pbs-plus/pbs-plus.env ]; then' \
  '    echo "Creating PBS Plus environment configuration..."' \
  '    cat > /etc/proxmox-backup/pbs-plus/pbs-plus.env <<EOF' \
  '# PBS Plus environment configuration' \
  '# This file is sourced by the pbs-plus service.' \
  '# Format: KEY=VALUE, no quotes unless needed for spaces.' \
  '' \
  '# Required variables:' \
  'PBS_PLUS_HOSTNAME=pbs-plus-test' \
  'EOF' \
  '    chmod 0644 /etc/proxmox-backup/pbs-plus/pbs-plus.env' \
  'fi' \
  '' \
  'if [ -n "$PBS_PLUS_HOSTNAME" ]; then' \
  '    echo "Configuring PBS Plus hostname..."' \
  '    if grep -q "^PBS_PLUS_HOSTNAME=" /etc/proxmox-backup/pbs-plus/pbs-plus.env; then' \
  '        sed -i "s|^PBS_PLUS_HOSTNAME=.*|PBS_PLUS_HOSTNAME=${PBS_PLUS_HOSTNAME}|" /etc/proxmox-backup/pbs-plus/pbs-plus.env' \
  '    else' \
  '        echo "PBS_PLUS_HOSTNAME=${PBS_PLUS_HOSTNAME}" >> /etc/proxmox-backup/pbs-plus/pbs-plus.env' \
  '    fi' \
  'fi' \
  '' \
  'for var in $(compgen -e | grep "^PBS_PLUS_"); do' \
  '    key="$var"' \
  '    value="${!var}"' \
  '    if [ -n "$value" ] && [ "$key" != "PBS_PLUS_HOSTNAME" ]; then' \
  '        echo "Setting PBS Plus config: $key"' \
  '        if grep -q "^${key}=" /etc/proxmox-backup/pbs-plus/pbs-plus.env; then' \
  '            sed -i "s|^${key}=.*|${key}=${value}|" /etc/proxmox-backup/pbs-plus/pbs-plus.env' \
  '        else' \
  '            echo "${key}=${value}" >> /etc/proxmox-backup/pbs-plus/pbs-plus.env' \
  '        fi' \
  '    fi' \
  'done' \
  '' \
  'echo "Initialization complete."' \
  > /etc/s6-overlay/scripts/pbs-init.sh && \
  chmod +x /etc/s6-overlay/scripts/pbs-init.sh

RUN install -d -m 0755 /etc/s6-overlay/s6-rc.d/pbs-plus && \
    printf '%s\n' \
        '#!/command/execlineb -P' \
        'with-contenv' \
        'envfile /etc/proxmox-backup/pbs-plus/pbs-plus.env' \
        's6-setuidgid root' \
        '/usr/bin/pbs-plus' \
        > /etc/s6-overlay/s6-rc.d/pbs-plus/run && \
    chmod +x /etc/s6-overlay/s6-rc.d/pbs-plus/run && \
    echo 'longrun' > /etc/s6-overlay/s6-rc.d/pbs-plus/type && \
    printf '%s\n' 'pbs-init' 'proxmox-backup-proxy' > /etc/s6-overlay/s6-rc.d/pbs-plus/dependencies && \
    ln -s ../pbs-plus /etc/s6-overlay/s6-rc.d/user/contents.d/pbs-plus

RUN set -eu; \
  dir="/usr/local/libexec/systemd-stubs"; \
  mkdir -p "$dir"; \
  for b in systemctl systemd-run loginctl hostnamectl timedatectl localectl; do \
    printf '%s\n' '#!/bin/sh' 'exit 1' >"$dir/$b"; \
    chmod +x "$dir/$b"; \
  done; \
  printf 'export PATH="%s:$PATH"\n' "$dir" > /etc/profile.d/00-systemd-stubs.sh

ENV PATH="/usr/local/libexec/systemd-stubs:${PATH}"

EXPOSE 8017

VOLUME ["/var/lib/proxmox-backup", "/etc/proxmox-backup", "/var/log/proxmox-backup"]

EXPOSE 8007

ENTRYPOINT ["/init"]
