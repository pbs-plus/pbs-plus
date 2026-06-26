#!/usr/bin/env bash

# This script was previously used to build and embed agent binaries into the
# server binary. Agent binaries are no longer embedded — the server proxies
# all downloads from GitHub and verifies them against embedded checksums
# (agentbin/checksums.txt, populated by generate-embedded-checksums.sh).
#
# Kept as a goreleaser before-hook for backward compatibility (no-op for
# non-server builds). Does nothing.

set -euo pipefail

echo "build-agent-binaries.sh: no-op (binaries are no longer embedded)"
