package targetplugin

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"testing"
)

func TestVerifyArtifact(t *testing.T) {
	payload := []byte("plugin executable bytes")
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	fingerprint, err := p256Fingerprint(&key.PublicKey)
	if err != nil {
		t.Fatalf("p256Fingerprint: %v", err)
	}
	artifact := signedArtifact(t, key, payload)

	var staging bytes.Buffer
	if err := VerifyArtifact(&staging, bytes.NewReader(payload), artifact, fingerprint, &key.PublicKey, 1024); err != nil {
		t.Fatalf("VerifyArtifact: %v", err)
	}
	if !bytes.Equal(staging.Bytes(), payload) {
		t.Fatalf("staged artifact = %q, want %q", staging.Bytes(), payload)
	}
}

func TestVerifyArtifactRejectsInvalidInput(t *testing.T) {
	payload := []byte("plugin executable bytes")
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	fingerprint, err := p256Fingerprint(&key.PublicKey)
	if err != nil {
		t.Fatalf("p256Fingerprint: %v", err)
	}

	tests := []struct {
		name      string
		source    []byte
		configure func(*RepositoryArtifact) (*ecdsa.PublicKey, string, uint64)
		wantError string
	}{
		{
			name:   "altered bytes",
			source: []byte("plugin executable bytez"),
			configure: func(*RepositoryArtifact) (*ecdsa.PublicKey, string, uint64) {
				return &key.PublicKey, fingerprint, 1024
			},
			wantError: "SHA-256 mismatch",
		},
		{
			name:   "truncated",
			source: payload[:len(payload)-1],
			configure: func(*RepositoryArtifact) (*ecdsa.PublicKey, string, uint64) {
				return &key.PublicKey, fingerprint, 1024
			},
			wantError: "does not match declared size",
		},
		{
			name:   "oversized download",
			source: append(bytes.Clone(payload), 'x'),
			configure: func(*RepositoryArtifact) (*ecdsa.PublicKey, string, uint64) {
				return &key.PublicKey, fingerprint, 1024
			},
			wantError: "does not match declared size",
		},
		{
			name:   "declared size above limit",
			source: payload,
			configure: func(artifact *RepositoryArtifact) (*ecdsa.PublicKey, string, uint64) {
				artifact.Size++
				return &key.PublicKey, fingerprint, uint64(len(payload))
			},
			wantError: "exceeds limit",
		},
		{
			name:   "publisher fingerprint",
			source: payload,
			configure: func(*RepositoryArtifact) (*ecdsa.PublicKey, string, uint64) {
				wrongKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
				if err != nil {
					t.Fatalf("GenerateKey: %v", err)
				}
				return &wrongKey.PublicKey, fingerprint, 1024
			},
			wantError: "publisher key fingerprint mismatch",
		},
		{
			name:   "publisher signature",
			source: payload,
			configure: func(artifact *RepositoryArtifact) (*ecdsa.PublicKey, string, uint64) {
				wrongKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
				if err != nil {
					t.Fatalf("GenerateKey: %v", err)
				}
				artifact.Signature = signP256(t, wrongKey, payload)
				return &key.PublicKey, fingerprint, 1024
			},
			wantError: "signature mismatch",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			artifact := signedArtifact(t, key, payload)
			publicKey, expectedFingerprint, maxBytes := test.configure(&artifact)
			err := VerifyArtifact(&bytes.Buffer{}, bytes.NewReader(test.source), artifact, expectedFingerprint, publicKey, maxBytes)
			if !errors.Is(err, ErrInvalidArtifact) || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("VerifyArtifact error = %v, want ErrInvalidArtifact containing %q", err, test.wantError)
			}
		})
	}
}

func TestVerifyArtifactWriterError(t *testing.T) {
	payload := []byte("plugin executable bytes")
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	fingerprint, err := p256Fingerprint(&key.PublicKey)
	if err != nil {
		t.Fatalf("p256Fingerprint: %v", err)
	}
	writeError := errors.New("disk full")
	err = VerifyArtifact(failingWriter{err: writeError}, bytes.NewReader(payload), signedArtifact(t, key, payload), fingerprint, &key.PublicKey, 1024)
	if !errors.Is(err, writeError) {
		t.Fatalf("VerifyArtifact error = %v, want writer error", err)
	}
}

type failingWriter struct {
	err error
}

func (writer failingWriter) Write([]byte) (int, error) {
	return 0, writer.err
}

func signedArtifact(t *testing.T, key *ecdsa.PrivateKey, payload []byte) RepositoryArtifact {
	t.Helper()
	digest := sha256.Sum256(payload)
	return RepositoryArtifact{
		OS:        "linux",
		Arch:      "amd64",
		URL:       "plugin",
		Size:      uint64(len(payload)),
		SHA256:    hex.EncodeToString(digest[:]),
		Signature: signP256(t, key, payload),
	}
}
