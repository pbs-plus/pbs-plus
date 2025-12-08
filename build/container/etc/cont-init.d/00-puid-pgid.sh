#!/bin/sh
set -eu

# Defaults mirror your Dockerfile args
USER_NAME="${USER_NAME:-pbsplus}"
PUID="${PUID:-1999}"
PGID="${PGID:-1999}"

# If PGID exists on the system, reuse; else modify/create the group
existing_gid="$(getent group "${USER_NAME}" 2>/dev/null | awk -F: '{print $3}' || true)"
if [ -n "${existing_gid:-}" ] && [ "$existing_gid" -ne "$PGID" ] 2>/dev/null; then
  # Group exists but with a different gid; try to modify if possible
  if command -v groupmod >/dev/null 2>&1; then
    groupmod -o -g "$PGID" "$USER_NAME" 2>/dev/null || true
  fi
elif ! getent group "$PGID" >/dev/null 2>&1; then
  # Create the group if not present
  if ! getent group "$USER_NAME" >/dev/null 2>&1; then
    addgroup -g "$PGID" -S "$USER_NAME" 2>/dev/null || addgroup -g "$PGID" "$USER_NAME" 2>/dev/null || true
  fi
fi

# Now ensure the user has the requested UID and primary GID
existing_uid="$(getent passwd "${USER_NAME}" 2>/dev/null | awk -F: '{print $3}' || true)"
if [ -n "${existing_uid:-}" ]; then
  # Adjust uid/gid if needed
  if [ "$existing_uid" -ne "$PUID" ] 2>/dev/null; then
    if command -v usermod >/dev/null 2>&1; then
      usermod -o -u "$PUID" "$USER_NAME" 2>/dev/null || true
    fi
  fi
  # Ensure primary group id matches
  if command -v usermod >/dev/null 2>&1; then
    usermod -g "$PGID" "$USER_NAME" 2>/dev/null || true
  fi
else
  # Create user if missing
  adduser -u "$PUID" -S -D -H -G "$USER_NAME" -s /sbin/nologin "$USER_NAME" 2>/dev/null || true
fi

# Fix ownership on writable paths (idempotent)
chown -R "$USER_NAME:$USER_NAME" \
  /var/lib/pbs-plus-agent \
  /var/log/pbs-plus-agent \
  /run/pbs-plus-agent \
  /etc/pbs-plus-agent 2>/dev/null || true
