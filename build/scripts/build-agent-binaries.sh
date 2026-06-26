#!/usr/bin/env bash

set -euo pipefail

VERSION="${1:?Usage: $0 <version|clean>}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
AGENTBIN_DIR="$PROJECT_ROOT/internal/server/web/api/agentbin"

if [ "$VERSION" = "clean" ]; then
    echo "Cleaning embedded agent bin dir..."
    find "$AGENTBIN_DIR" -type f ! -name VERSION ! -name .gitignore ! -name checksums.txt -delete
    echo "Done."
    exit 0
fi

# Write the version file (used by embeddedVersion()).
echo "$VERSION" > "$AGENTBIN_DIR/VERSION"

# NOTE: Agent binaries are no longer embedded in the server binary.
# The server proxies all downloads from GitHub and verifies them against
# embedded checksums (agentbin/checksums.txt), which are populated by the
# release pipeline after all artifacts (agents + MSI) are built.

echo "VERSION file written: $VERSION"
