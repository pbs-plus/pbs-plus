//go:build linux

package plugins

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"runtime"
	"strings"
	"testing"

	"github.com/pbs-plus/pbs-plus/internal/targetplugin"
)

func TestResolveRelease(t *testing.T) {
	hostArtifact := targetplugin.RepositoryArtifact{
		OS:     runtime.GOOS,
		Arch:   runtime.GOARCH,
		URL:    "artifacts/plugin",
		Size:   1024,
		SHA256: strings.Repeat("ab", 32),
	}
	release := targetplugin.RepositoryRelease{
		PluginID:        "org.pbs-plus.test",
		Version:         "1.0.0",
		ProtocolVersion: targetplugin.CurrentProtocolVersion,
		Artifacts:       []targetplugin.RepositoryArtifact{hostArtifact},
	}
	revoked := release
	revoked.Version = "1.1.0"
	revoked.Revoked = true
	revoked.RevocationReason = "key compromise"
	foreignProtocol := release
	foreignProtocol.Version = "2.0.0"
	foreignProtocol.ProtocolVersion = targetplugin.CurrentProtocolVersion + 1
	foreignPlatform := release
	foreignPlatform.Version = "3.0.0"
	foreignPlatform.Artifacts = []targetplugin.RepositoryArtifact{{OS: "plan9", Arch: "mips", URL: "artifacts/other"}}

	index := targetplugin.RepositoryIndex{
		FormatVersion: targetplugin.RepositoryFormatVersion,
		RepositoryID:  "org.pbs-plus.official",
		Releases:      []targetplugin.RepositoryRelease{release, revoked, foreignProtocol, foreignPlatform},
	}

	tests := []struct {
		name      string
		version   string
		wantError string
	}{
		{name: "installable release", version: "1.0.0"},
		{name: "revoked release", version: "1.1.0", wantError: "revoked"},
		{name: "foreign protocol", version: "2.0.0", wantError: "needs protocol"},
		{name: "foreign platform", version: "3.0.0", wantError: "artifact"},
		{name: "missing release", version: "9.9.9", wantError: "not in repository"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			resolved, err := resolveRelease(index, "org.pbs-plus.test", test.version)
			if test.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantError) {
					t.Fatalf("resolveRelease error = %v, want %q", err, test.wantError)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveRelease: %v", err)
			}
			if resolved.Release.Version != test.version || resolved.Artifact != hostArtifact {
				t.Fatalf("resolved = %#v", resolved)
			}
		})
	}
}

func TestParsePublisherKey(t *testing.T) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	der, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		t.Fatalf("MarshalPKIXPublicKey: %v", err)
	}
	parsed, err := parsePublisherKey(der)
	if err != nil {
		t.Fatalf("parsePublisherKey: %v", err)
	}
	if !parsed.Equal(&key.PublicKey) {
		t.Fatal("parsed key does not match")
	}
	if _, err := parsePublisherKey([]byte("not-a-key")); err == nil {
		t.Fatal("parsePublisherKey succeeded on invalid input")
	}
}
