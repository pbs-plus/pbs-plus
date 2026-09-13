package targetplugin

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
)

// ErrInvalidArtifact reports failed plugin artifact integrity verification.
var ErrInvalidArtifact = errors.New("invalid plugin artifact")

// VerifyArtifact copies an artifact to staging and authenticates it. Callers must discard staging on any error.
func VerifyArtifact(dst io.Writer, src io.Reader, artifact RepositoryArtifact, publisherFingerprint string, publisherKey *ecdsa.PublicKey, maxBytes uint64) error {
	if maxBytes == 0 {
		return errors.New("artifact byte limit is required")
	}
	if artifact.Size == 0 {
		return fmt.Errorf("%w: artifact size is required", ErrInvalidArtifact)
	}
	if artifact.Size > maxBytes {
		return fmt.Errorf("%w: declared size %d exceeds limit %d", ErrInvalidArtifact, artifact.Size, maxBytes)
	}
	if artifact.Size >= math.MaxInt64 {
		return fmt.Errorf("%w: declared size is too large", ErrInvalidArtifact)
	}
	if err := validateSHA256("artifact SHA-256", artifact.SHA256); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidArtifact, err)
	}
	if err := validateSHA256("publisher key fingerprint", publisherFingerprint); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidArtifact, err)
	}
	fingerprint, err := p256Fingerprint(publisherKey)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidArtifact, err)
	}
	if !strings.EqualFold(fingerprint, publisherFingerprint) {
		return fmt.Errorf("%w: publisher key fingerprint mismatch", ErrInvalidArtifact)
	}

	hash := sha256.New()
	written, err := io.Copy(io.MultiWriter(dst, hash), io.LimitReader(src, int64(artifact.Size)+1))
	if err != nil {
		return fmt.Errorf("write staging artifact: %w", err)
	}
	if written != int64(artifact.Size) {
		return fmt.Errorf("%w: downloaded size %d does not match declared size %d", ErrInvalidArtifact, written, artifact.Size)
	}

	expectedDigest, err := hex.DecodeString(artifact.SHA256)
	if err != nil {
		return fmt.Errorf("%w: decode artifact SHA-256: %v", ErrInvalidArtifact, err)
	}
	actualDigest := hash.Sum(nil)
	if !bytes.Equal(actualDigest, expectedDigest) {
		return fmt.Errorf("%w: SHA-256 mismatch", ErrInvalidArtifact)
	}
	if err := verifyP256Digest(actualDigest, []byte(artifact.Signature), publisherKey); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidArtifact, err)
	}
	return nil
}

func p256Fingerprint(publicKey *ecdsa.PublicKey) (string, error) {
	if err := validateP256PublicKey(publicKey); err != nil {
		return "", err
	}
	der, err := x509.MarshalPKIXPublicKey(publicKey)
	if err != nil {
		return "", fmt.Errorf("encode publisher public key: %w", err)
	}
	digest := sha256.Sum256(der)
	return hex.EncodeToString(digest[:]), nil
}
