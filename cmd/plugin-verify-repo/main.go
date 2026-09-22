package main

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"os"

	"github.com/pbs-plus/pbs-plus/internal/targetplugin"
)

// Verifies a hosted plugin repository end to end (index signature, manifest digest,
// artifact digest+signature) exactly as the host install path does.
func main() {
	indexURL := os.Args[1]

	block, _ := pem.Decode([]byte(os.Args[2]))
	if block == nil {
		fmt.Fprintln(os.Stderr, "no PEM block")
		os.Exit(1)
	}
	parsed, err := x509.ParseECPrivateKey(block.Bytes)
	if err != nil {
		fmt.Fprintln(os.Stderr, "parse key:", err)
		os.Exit(1)
	}
	publicKey := &parsed.PublicKey
	fingerprint, err := targetplugin.PublicKeyFingerprint(publicKey)
	if err != nil {
		fmt.Fprintln(os.Stderr, "fingerprint:", err)
		os.Exit(1)
	}

	ctx := context.Background()
	fetcher := targetplugin.Fetcher{}
	document, err := fetcher.Index(ctx, indexURL, targetplugin.PluginRepositoryCache{})
	if err != nil {
		fmt.Fprintln(os.Stderr, "fetch index:", err)
		os.Exit(1)
	}
	index, err := targetplugin.ParseRepositoryIndex(document.Index, document.Signature, publicKey)
	if err != nil {
		fmt.Fprintln(os.Stderr, "verify index:", err)
		os.Exit(1)
	}
	if err := index.Validate(); err != nil {
		fmt.Fprintln(os.Stderr, "validate index:", err)
		os.Exit(1)
	}
	fmt.Println("index OK:", index.RepositoryID, len(index.Releases), "releases, fingerprint", fingerprint)

	release := index.Releases[0]
	manifest, err := fetcher.Manifest(ctx, document.URL, release.ManifestURL)
	if err != nil {
		fmt.Fprintln(os.Stderr, "fetch manifest:", err)
		os.Exit(1)
	}
	parsedManifest, err := targetplugin.ParsePluginManifest(manifest, release.ManifestSHA256)
	if err != nil {
		fmt.Fprintln(os.Stderr, "verify manifest:", err)
		os.Exit(1)
	}
	fmt.Println("manifest OK:", parsedManifest.PluginID, parsedManifest.Version)

	stream, err := fetcher.Artifact(ctx, document.URL, release.Artifacts[0].URL)
	if err != nil {
		fmt.Fprintln(os.Stderr, "fetch artifact:", err)
		os.Exit(1)
	}
	defer stream.Close()
	var sink countingWriter
	if err := targetplugin.VerifyArtifact(&sink, stream, release.Artifacts[0], fingerprint, publicKey, 64<<20); err != nil {
		fmt.Fprintln(os.Stderr, "verify artifact:", err)
		os.Exit(1)
	}
	fmt.Println("artifact OK:", sink.n, "bytes")
	fmt.Println("GITHUB_HOSTING_OK")
}

type countingWriter struct{ n int64 }

func (w *countingWriter) Write(p []byte) (int, error) {
	w.n += int64(len(p))
	return len(p), nil
}
