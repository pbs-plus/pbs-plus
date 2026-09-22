package targetplugin

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestParseRepositoryIndex(t *testing.T) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	artifactSignature := signP256(t, key, []byte("artifact"))
	indexBytes := []byte(fmt.Sprintf(`format_version = 1
repository_id = "org.pbs-plus.official"

[[release]]
plugin_id = "org.pbs-plus.filesystem"
version = "1.0.0"
publisher = "PBS Plus"
publisher_key_fingerprint = "%s"
minimum_host_version = "1.0.0"
maximum_host_version = "2.0.0"
protocol = 1
target_types = ["filesystem"]
manifest_url = "plugins/filesystem/1.0.0/manifest.toml"
manifest_sha256 = "%s"
channel = "stable"
revoked = false

[[release.artifact]]
os = "linux"
arch = "amd64"
url = "plugins/filesystem/1.0.0/linux-amd64"
size = 1024
sha256 = "%s"
signature = "%s"
`, strings.Repeat("ab", sha256.Size), strings.Repeat("cd", sha256.Size), strings.Repeat("ef", sha256.Size), artifactSignature))

	index, err := ParseRepositoryIndex(indexBytes, []byte(signP256(t, key, indexBytes)), &key.PublicKey)
	if err != nil {
		t.Fatalf("ParseRepositoryIndex: %v", err)
	}
	if index.RepositoryID != "org.pbs-plus.official" || len(index.Releases) != 1 {
		t.Fatalf("unexpected repository index: %+v", index)
	}

	mutated := bytes.Clone(indexBytes)
	position := bytes.Index(mutated, []byte("official"))
	if position < 0 {
		t.Fatal("fixture does not contain repository ID")
	}
	mutated[position] = 'x'
	if _, err := ParseRepositoryIndex(mutated, []byte(signP256(t, key, indexBytes)), &key.PublicKey); !errors.Is(err, ErrInvalidRepositorySignature) {
		t.Fatalf("mutated index error = %v, want ErrInvalidRepositorySignature", err)
	}

	malformed := []byte("format_version = [")
	if _, err := ParseRepositoryIndex(malformed, []byte("invalid"), &key.PublicKey); !errors.Is(err, ErrInvalidRepositorySignature) {
		t.Fatalf("unverified malformed index error = %v, want signature failure", err)
	}
	if _, err := ParseRepositoryIndex(malformed, []byte(signP256(t, key, malformed)), &key.PublicKey); err == nil || !strings.Contains(err.Error(), "decode repository index") {
		t.Fatalf("verified malformed index error = %v, want decode failure", err)
	}

	unknown := append(bytes.Clone(indexBytes), []byte("unknown = true\n")...)
	if _, err := ParseRepositoryIndex(unknown, []byte(signP256(t, key, unknown)), &key.PublicKey); err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("unknown field error = %v", err)
	}
}

func TestRepositoryIndexValidate(t *testing.T) {
	tests := []struct {
		name      string
		mutate    func(*RepositoryIndex)
		wantError string
	}{
		{
			name:      "valid",
			mutate:    func(*RepositoryIndex) {},
			wantError: "",
		},
		{
			name: "duplicate release",
			mutate: func(index *RepositoryIndex) {
				index.Releases = append(index.Releases, index.Releases[0])
			},
			wantError: "duplicate release",
		},
		{
			name: "duplicate platform",
			mutate: func(index *RepositoryIndex) {
				index.Releases[0].Artifacts = append(index.Releases[0].Artifacts, index.Releases[0].Artifacts[0])
			},
			wantError: "duplicate artifact platform",
		},
		{
			name: "invalid digest",
			mutate: func(index *RepositoryIndex) {
				index.Releases[0].Artifacts[0].SHA256 = "nope"
			},
			wantError: "must contain 64 hexadecimal characters",
		},
		{
			name: "insecure URL",
			mutate: func(index *RepositoryIndex) {
				index.Releases[0].Artifacts[0].URL = "http://plugins.example/file"
			},
			wantError: "must use HTTPS",
		},
		{
			name: "revocation reason",
			mutate: func(index *RepositoryIndex) {
				index.Releases[0].Revoked = true
			},
			wantError: "revocation reason is required",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			index := validRepositoryIndex(t)
			test.mutate(&index)
			err := index.Validate()
			if test.wantError == "" && err != nil {
				t.Fatalf("Validate: %v", err)
			}
			if test.wantError != "" && (err == nil || !strings.Contains(err.Error(), test.wantError)) {
				t.Fatalf("Validate error = %v, want %q", err, test.wantError)
			}
		})
	}
}

func validRepositoryIndex(t *testing.T) RepositoryIndex {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	return RepositoryIndex{
		FormatVersion: RepositoryFormatVersion,
		RepositoryID:  "org.pbs-plus.official",
		Releases: []RepositoryRelease{
			{
				PluginID:                "org.pbs-plus.filesystem",
				Version:                 "1.0.0",
				Publisher:               "PBS Plus",
				PublisherKeyFingerprint: strings.Repeat("ab", sha256.Size),
				MinimumHostVersion:      "1.0.0",
				MaximumHostVersion:      "2.0.0",
				ProtocolVersion:         CurrentProtocolVersion,
				TargetTypes:             []string{"filesystem"},
				ManifestURL:             "plugins/filesystem/1.0.0/manifest.toml",
				ManifestSHA256:          strings.Repeat("cd", sha256.Size),
				Channel:                 "stable",
				Artifacts: []RepositoryArtifact{
					{
						OS:        "linux",
						Arch:      "amd64",
						URL:       "plugins/filesystem/1.0.0/linux-amd64",
						Size:      1024,
						SHA256:    strings.Repeat("ef", sha256.Size),
						Signature: signP256(t, key, []byte("artifact")),
					},
				},
			},
		},
	}
}

func signP256(t *testing.T, key *ecdsa.PrivateKey, message []byte) string {
	t.Helper()
	digest := sha256.Sum256(message)
	signature, err := ecdsa.SignASN1(rand.Reader, key, digest[:])
	if err != nil {
		t.Fatalf("SignASN1: %v", err)
	}
	return base64.StdEncoding.EncodeToString(signature)
}
