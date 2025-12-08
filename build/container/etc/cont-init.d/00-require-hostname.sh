#!/bin/sh
set -eu

# Choose the env var your app uses
HOST_ENV="${AGENT_HOSTNAME:-}"

if [ -z "$HOST_ENV" ]; then
  echo "Error: AGENT_HOSTNAME must be set" >&2
  exit 1
fi
