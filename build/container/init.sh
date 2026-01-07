#!/bin/sh
set -eu

USER_NAME="${USER:-pbsplus}"
BIN_PATH="/usr/bin/pbs-plus-agent"

chmod +x "$BIN_PATH"

# Registry paths (lowercase per new Unix-native layout)
REG_BASE="/etc/pbs-plus-agent/registry"
REG_DIR_AGENT="$REG_BASE/software/pbsplus/config"
REG_SERVER_URL_FILE="$REG_DIR_AGENT/serverurl.value"
REG_BOOTSTRAP_TOKEN_FILE="$REG_DIR_AGENT/bootstraptoken.value"

# Require PBS_PLUS_HOSTNAME
if [ "${PBS_PLUS_HOSTNAME:-}" = "" ]; then
  echo "ERROR: PBS_PLUS_HOSTNAME environment variable is required." >&2
  exit 1
fi

PUID="${PUID:-}"
PGID="${PGID:-}"

# Current IDs
CURRENT_UID="$(id -u "$USER_NAME")"
CURRENT_GID="$(id -g "$USER_NAME")"

TARGET_GROUP="$USER_NAME"

# If PGID provided, reconcile group
if [ "$PGID" != "" ] && [ "$PGID" != "$CURRENT_GID" ]; then
  if getent group "$PGID" >/dev/null 2>&1; then
    TARGET_GROUP="$(getent group "$PGID" | cut -d: -f1)"
  else
    # Change existing group gid or create
    if getent group "$USER_NAME" >/dev/null 2>&1; then
      groupmod -g "$PGID" "$USER_NAME"
    else
      addgroup -g "$PGID" -S "$USER_NAME"
    fi
    TARGET_GROUP="$USER_NAME"
  fi
fi

# If PUID provided, reconcile user
if [ "$PUID" != "" ] && [ "$PUID" != "$CURRENT_UID" ]; then
  usermod -u "$PUID" "$USER_NAME"
fi

# Ensure primary group if TARGET_GROUP changed
if [ "$TARGET_GROUP" != "$USER_NAME" ]; then
  usermod -g "$TARGET_GROUP" "$USER_NAME"
elif [ "$PGID" != "" ] && [ "$PGID" != "$CURRENT_GID" ]; then
  # group name same as user_name but gid changed already above
  usermod -g "$USER_NAME" "$USER_NAME"
fi

# Fix ownerships
fix_ownership() {
  path="$1"
  if [ -e "$path" ]; then
    chown -R "$USER_NAME:$TARGET_GROUP" "$path"
  fi
}
fix_ownership "/var/lib/pbs-plus-agent"
fix_ownership "/var/log/pbs-plus-agent"
fix_ownership "/run/pbs-plus-agent"
fix_ownership "/etc/pbs-plus-agent"

# Ensure registry directory exists (for writes below)
mkdir -p "$REG_DIR_AGENT"
chown -R "$USER_NAME:$TARGET_GROUP" "$REG_BASE"
chmod 0755 "$REG_BASE" 2>/dev/null || true

# Check existing registry values; if missing, require env vars and set them
need_server_url=0
need_bootstrap_token=0

if [ ! -s "$REG_SERVER_URL_FILE" ]; then
  need_server_url=1
fi
if [ ! -s "$REG_BOOTSTRAP_TOKEN_FILE" ]; then
  need_bootstrap_token=1
fi

if [ "$need_server_url" -eq 1 ] || [ "$need_bootstrap_token" -eq 1 ]; then
  # Require both env vars to initialize
  if [ "${PBS_PLUS_INIT_SERVER_URL:-}" = "" ] || [ "${PBS_PLUS_INIT_BOOTSTRAP_TOKEN:-}" = "" ]; then
    echo "ERROR: Initial registry missing. Please set PBS_PLUS_INIT_SERVER_URL and PBS_PLUS_INIT_BOOTSTRAP_TOKEN." >&2
    echo "Missing: $( [ "$need_server_url" -eq 1 ] && printf "ServerURL " || true)$( [ "$need_bootstrap_token" -eq 1 ] && printf "BootstrapToken" || true )" >&2
    exit 1
  fi

  # Write ServerURL (plaintext)
  if [ "$need_server_url" -eq 1 ]; then
    umask 022
    tmp="$(mktemp "${REG_DIR_AGENT}/.tmp-serverurl.XXXXXX")"
    printf "%s" "$PBS_PLUS_INIT_SERVER_URL" >"$tmp"
    chmod 0640 "$tmp"
    chown "$USER_NAME:$TARGET_GROUP" "$tmp"
    mv -f "$tmp" "$REG_SERVER_URL_FILE"
  fi

  # Write BootstrapToken (plaintext here; agent library may encrypt on first read/write)
  if [ "$need_bootstrap_token" -eq 1 ]; then
    umask 077
    tmp="$(mktemp "${REG_DIR_AGENT}/.tmp-bootstraptoken.XXXXXX")"
    printf "%s" "$PBS_PLUS_INIT_BOOTSTRAP_TOKEN" >"$tmp"
    chmod 0600 "$tmp"
    chown "$USER_NAME:$TARGET_GROUP" "$tmp"
    mv -f "$tmp" "$REG_BOOTSTRAP_TOKEN_FILE"
  fi
fi

if [ -e /proc/sys/kernel/cap_last_cap ]; then
  # This requires the container to be run with --cap-add
  # It's a no-op if the capability isn't granted by Docker
  setcap 'cap_dac_read_search+ep' "$BIN_PATH" 2>/dev/null || true
fi

# Exec
if [ "${1:-}" = "" ] || [ "$1" = "pbs-plus-agent" ] || [ "$1" = "$BIN_PATH" ]; then
  exec su-exec "$USER_NAME:$TARGET_GROUP" "$BIN_PATH"
else
  exec su-exec "$USER_NAME:$TARGET_GROUP" "$@"
fi
