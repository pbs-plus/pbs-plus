#!/usr/bin/env bash

set -euo pipefail

VERSION="${1:?Usage: $0 <version|clean>}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
AGENTBIN_DIR="$PROJECT_ROOT/internal/server/web/api/agentbin"

if [ "$VERSION" = "clean" ]; then
    echo "Cleaning embedded agent binaries..."
    find "$AGENTBIN_DIR" -type f ! -name VERSION ! -name .gitignore -delete
    echo "Done."
    exit 0
fi

echo "Building agent binaries for embedding (version: $VERSION)..."

mkdir -p "$AGENTBIN_DIR"

echo "$VERSION" > "$AGENTBIN_DIR/VERSION"

LDFLAGS="-X 'main.Version=$VERSION'"
if [ -n "${ECDSA_PUBLIC_KEY_B64:-}" ]; then
    LDFLAGS="$LDFLAGS -X 'github.com/pbs-plus/pbs-plus/internal/agent/updater.ECDSAPublicKeyB64=$ECDSA_PUBLIC_KEY_B64'"
fi
if [ -n "${ED25519_PUBLIC_KEY_B64:-}" ]; then
    LDFLAGS="$LDFLAGS -X 'github.com/pbs-plus/pbs-plus/internal/agent/updater.Ed25519PublicKeyB64=$ED25519_PUBLIC_KEY_B64'"
fi

PLATFORMS=(
    "linux/amd64"
    "linux/arm64"
    "windows/amd64"
)

for PLATFORM in "${PLATFORMS[@]}"; do
    GOOS="${PLATFORM%%/*}"
    GOARCH="${PLATFORM#*/}"
    EXT=""
    if [ "$GOOS" = "windows" ]; then
        EXT=".exe"
    fi

    BINARY_NAME="pbs-plus-agent-${VERSION}-${GOOS}-${GOARCH}${EXT}"
    OUTPUT="$AGENTBIN_DIR/$BINARY_NAME"

    echo "Building $BINARY_NAME..."
    CGO_ENABLED=0 GOOS="$GOOS" GOARCH="$GOARCH" go build \
        -tags=agent,netgo,osusergo \
        -installsuffix=netgo \
        -ldflags "$LDFLAGS -extldflags '-static'" \
        -o "$OUTPUT" \
        ./cmd/agent

    echo "Computing SHA-256 for $BINARY_NAME..."
    sha256sum "$OUTPUT" | awk '{print $1}' > "${OUTPUT}.sha256"
done

echo "Agent binaries built successfully."
echo "Embedded files:"
ls -la "$AGENTBIN_DIR/"