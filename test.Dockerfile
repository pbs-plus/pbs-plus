FROM golang:1.25 AS builder

WORKDIR /build

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN GOFIPS140=latest go build -o pbs-plus ./cmd/pbs_plus

FROM ghcr.io/pbs-plus/proxmox-backup-docker:latest

COPY --from=builder /build/pbs-plus /usr/bin/pbs-plus
RUN chmod 0755 /usr/bin/pbs-plus

# Create pbs-plus configuration directory
RUN install -d -m 0755 -o root -g root /etc/proxmox-backup/pbs-plus

# Update initialization script to include pbs-plus setup
RUN printf '%s\n' \
  '#!/command/with-contenv bash' \
  'set -e' \
  '' \
  'echo "Initializing Proxmox Backup Server..."' \
  '' \
  '# Check if /run/proxmox-backup exists and is on tmpfs' \
  'if [ ! -d /run/proxmox-backup ]; then' \
  '    echo "ERROR: /run/proxmox-backup directory does not exist!"' \
  '    echo "Please create it as a tmpfs mount with:"' \
  '    echo "  docker run --tmpfs /run/proxmox-backup:size=64M ..."' \
  '    exit 1' \
  'fi' \
  '' \
  '# Verify it is on tmpfs' \
  'FS_TYPE=$(stat -f -c %T /run/proxmox-backup 2>/dev/null || echo "unknown")' \
  'if [ "$FS_TYPE" != "tmpfs" ]; then' \
  '    echo "ERROR: /run/proxmox-backup is not on tmpfs (found: $FS_TYPE)!"' \
  '    echo "Proxmox Backup Server requires this directory to be on tmpfs."' \
  '    echo "Please run the container with:"' \
  '    echo "  docker run --tmpfs /run/proxmox-backup:size=64M ..."' \
  '    exit 1' \
  'fi' \
  '' \
  'chown backup:backup /run/proxmox-backup' \
  'chmod 755 /run/proxmox-backup' \
  '' \
  '# Fix permissions on mounted volumes' \
  'chown -R backup:backup /etc/proxmox-backup /var/lib/proxmox-backup /var/log/proxmox-backup' \
  'chmod 700 /etc/proxmox-backup' \
  '' \
  '# Set root password if ROOT_PASSWORD is provided' \
  'if [ -n "$ROOT_PASSWORD" ]; then' \
  '    echo "Setting root password..."' \
  '    echo "root:$ROOT_PASSWORD" | chpasswd' \
  '    unset ROOT_PASSWORD' \
  'fi' \
  '' \
  '# Create additional PAM users if specified' \
  'for var in $(compgen -e | grep "^PBS_USER_"); do' \
  '    username=$(echo "$var" | sed "s/^PBS_USER_//" | tr "[:upper:]" "[:lower:]")' \
  '    password="${!var}"' \
  '    if [ -n "$password" ] && [ -n "$username" ]; then' \
  '        if ! id "$username" >/dev/null 2>&1; then' \
  '            echo "Creating PAM user: $username"' \
  '            useradd -m -s /bin/bash "$username"' \
  '        else' \
  '            echo "Updating password for PAM user: $username"' \
  '        fi' \
  '        echo "$username:$password" | chpasswd' \
  '        unset "$var"' \
  '    fi' \
  'done' \
  '' \
  '# Generate RSA key pair for authentication if needed' \
  'if [ ! -f /etc/proxmox-backup/authkey.key ]; then' \
  '    echo "Generating authentication keys..."' \
  '    openssl genrsa -out /etc/proxmox-backup/authkey.key 4096' \
  '    openssl rsa -in /etc/proxmox-backup/authkey.key -pubout -out /etc/proxmox-backup/authkey.pub' \
  '    chown backup:backup /etc/proxmox-backup/authkey.key /etc/proxmox-backup/authkey.pub' \
  '    chmod 600 /etc/proxmox-backup/authkey.key /etc/proxmox-backup/authkey.pub' \
  'fi' \
  '' \
  '# Generate CSRF key if needed' \
  'if [ ! -f /etc/proxmox-backup/csrf.key ]; then' \
  '    echo "Generating CSRF key..."' \
  '    openssl rand -base64 32 > /etc/proxmox-backup/csrf.key' \
  '    chown backup:backup /etc/proxmox-backup/csrf.key' \
  '    chmod 600 /etc/proxmox-backup/csrf.key' \
  'fi' \
  '' \
  '# Generate proxy certificates if needed' \
  'if [ ! -f /etc/proxmox-backup/proxy.pem ]; then' \
  '    echo "Generating proxy certificates..."' \
  '    HOSTNAME=$(hostname -f 2>/dev/null || hostname)' \
  '    openssl req -x509 -newkey rsa:4096 -nodes \' \
  '        -keyout /etc/proxmox-backup/proxy.key \' \
  '        -out /etc/proxmox-backup/proxy.pem \' \
  '        -days 3650 -subj "/CN=${HOSTNAME}"' \
  '    chown backup:backup /etc/proxmox-backup/proxy.pem /etc/proxmox-backup/proxy.key' \
  '    chmod 640 /etc/proxmox-backup/proxy.pem /etc/proxmox-backup/proxy.key' \
  'fi' \
  '' \
  '# Create default domains config if needed' \
  'if [ ! -f /etc/proxmox-backup/domains.cfg ]; then' \
  '    echo "Creating default domains config..."' \
  '    touch /etc/proxmox-backup/domains.cfg' \
  '    chown backup:backup /etc/proxmox-backup/domains.cfg' \
  '    chmod 640 /etc/proxmox-backup/domains.cfg' \
  'fi' \
  '' \
  '# Setup PBS Plus configuration' \
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
  '# Apply environment variables to pbs-plus.env if provided' \
  'if [ -n "$PBS_PLUS_HOSTNAME" ]; then' \
  '    echo "Configuring PBS Plus hostname..."' \
  '    if grep -q "^PBS_PLUS_HOSTNAME=" /etc/proxmox-backup/pbs-plus/pbs-plus.env; then' \
  '        sed -i "s|^PBS_PLUS_HOSTNAME=.*|PBS_PLUS_HOSTNAME=${PBS_PLUS_HOSTNAME}|" /etc/proxmox-backup/pbs-plus/pbs-plus.env' \
  '    else' \
  '        echo "PBS_PLUS_HOSTNAME=${PBS_PLUS_HOSTNAME}" >> /etc/proxmox-backup/pbs-plus/pbs-plus.env' \
  '    fi' \
  'fi' \
  '' \
  '# Apply any PBS_PLUS_* environment variables to the config file' \
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

# Setup s6 service for pbs-plus
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
    # PBS Plus should start after PBS proxy
    printf '%s\n' 'pbs-init' 'proxmox-backup-proxy' > /etc/s6-overlay/s6-rc.d/pbs-plus/dependencies && \
    # Enable the service
    ln -s ../pbs-plus /etc/s6-overlay/s6-rc.d/user/contents.d/pbs-plus

EXPOSE 8017

VOLUME ["/var/lib/proxmox-backup", "/etc/proxmox-backup", "/var/log/proxmox-backup"]

EXPOSE 8007

ENTRYPOINT ["/init"]
