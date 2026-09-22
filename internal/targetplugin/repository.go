package targetplugin

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/sha256"
	"encoding/asn1"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"net/url"
	"unicode"

	"github.com/BurntSushi/toml"
	"github.com/Masterminds/semver"
)

const (
	RepositoryFormatVersion     uint16 = 1
	MaxRepositoryIndexBytes            = 4 << 20
	MaxRepositorySignatureBytes        = 1024
	maxRepositoryReleases              = 4096
	maxRepositoryArtifacts             = 32
	maxRepositoryTextBytes             = 255
)

// ErrInvalidRepositorySignature reports failed repository authentication.
var ErrInvalidRepositorySignature = errors.New("invalid repository signature")

// RepositoryIndex is the signed top-level repository catalog.
type RepositoryIndex struct {
	FormatVersion uint16              `toml:"format_version"`
	RepositoryID  string              `toml:"repository_id"`
	Releases      []RepositoryRelease `toml:"release"`
}

// RepositoryRelease identifies one installable plugin version.
type RepositoryRelease struct {
	PluginID                string               `toml:"plugin_id"`
	Version                 string               `toml:"version"`
	Publisher               string               `toml:"publisher"`
	PublisherKeyFingerprint string               `toml:"publisher_key_fingerprint"`
	MinimumHostVersion      string               `toml:"minimum_host_version"`
	MaximumHostVersion      string               `toml:"maximum_host_version"`
	ProtocolVersion         uint16               `toml:"protocol"`
	TargetTypes             []string             `toml:"target_types"`
	ManifestURL             string               `toml:"manifest_url"`
	ManifestSHA256          string               `toml:"manifest_sha256"`
	Channel                 string               `toml:"channel"`
	ReplacedBy              string               `toml:"replaced_by"`
	Revoked                 bool                 `toml:"revoked"`
	RevocationReason        string               `toml:"revocation_reason"`
	Artifacts               []RepositoryArtifact `toml:"artifact"`
}

// RepositoryArtifact identifies one signed platform executable.
type RepositoryArtifact struct {
	OS        string `toml:"os"`
	Arch      string `toml:"arch"`
	URL       string `toml:"url"`
	Size      uint64 `toml:"size"`
	SHA256    string `toml:"sha256"`
	Signature string `toml:"signature"`
}

// ParseRepositoryIndex authenticates exact TOML bytes before strict decoding.
func ParseRepositoryIndex(indexBytes, signature []byte, publicKey *ecdsa.PublicKey) (RepositoryIndex, error) {
	if len(indexBytes) == 0 {
		return RepositoryIndex{}, errors.New("repository index is empty")
	}
	if len(indexBytes) > MaxRepositoryIndexBytes {
		return RepositoryIndex{}, fmt.Errorf("repository index exceeds %d bytes", MaxRepositoryIndexBytes)
	}
	if err := verifyP256Signature(indexBytes, signature, publicKey); err != nil {
		return RepositoryIndex{}, fmt.Errorf("verify repository index: %w", err)
	}

	var index RepositoryIndex
	metadata, err := toml.Decode(string(indexBytes), &index)
	if err != nil {
		return RepositoryIndex{}, fmt.Errorf("decode repository index: %w", err)
	}
	if undecoded := metadata.Undecoded(); len(undecoded) > 0 {
		return RepositoryIndex{}, fmt.Errorf("repository index contains unknown field %q", undecoded[0].String())
	}
	if err := index.Validate(); err != nil {
		return RepositoryIndex{}, fmt.Errorf("validate repository index: %w", err)
	}
	return index, nil
}

// Validate checks repository identities, compatibility, digests, and artifacts.
func (index RepositoryIndex) Validate() error {
	if index.FormatVersion != RepositoryFormatVersion {
		return fmt.Errorf("unsupported repository format %d", index.FormatVersion)
	}
	if err := validateIdentifier("repository ID", index.RepositoryID, maxPluginIDLength); err != nil {
		return err
	}
	if len(index.Releases) > maxRepositoryReleases {
		return fmt.Errorf("repository contains more than %d releases", maxRepositoryReleases)
	}

	seen := make(map[string]struct{}, len(index.Releases))
	for releaseIndex := range index.Releases {
		release := &index.Releases[releaseIndex]
		if err := release.validate(); err != nil {
			return fmt.Errorf("release %d: %w", releaseIndex, err)
		}
		key := release.PluginID + "@" + release.Version
		if _, ok := seen[key]; ok {
			return fmt.Errorf("duplicate release %q", key)
		}
		seen[key] = struct{}{}
	}
	return nil
}

func (release RepositoryRelease) validate() error {
	if err := validateIdentifier("plugin ID", release.PluginID, maxPluginIDLength); err != nil {
		return err
	}
	if len(release.Version) > maxVersionLength {
		return fmt.Errorf("plugin version exceeds %d bytes", maxVersionLength)
	}
	if _, err := semver.NewVersion(release.Version); err != nil {
		return fmt.Errorf("invalid plugin version: %w", err)
	}
	if err := validateText("publisher", release.Publisher, maxRepositoryTextBytes); err != nil {
		return err
	}
	if err := validateSHA256("publisher key fingerprint", release.PublisherKeyFingerprint); err != nil {
		return err
	}
	minimum, err := semver.NewVersion(release.MinimumHostVersion)
	if err != nil {
		return fmt.Errorf("invalid minimum host version: %w", err)
	}
	if release.MaximumHostVersion != "" {
		maximum, err := semver.NewVersion(release.MaximumHostVersion)
		if err != nil {
			return fmt.Errorf("invalid maximum host version: %w", err)
		}
		if minimum.GreaterThan(maximum) {
			return errors.New("minimum host version exceeds maximum host version")
		}
	}
	if release.ProtocolVersion == 0 {
		return errors.New("plugin protocol is required")
	}
	if len(release.TargetTypes) == 0 {
		return errors.New("release must provide at least one target type")
	}
	if len(release.TargetTypes) > maxTargetTypes {
		return fmt.Errorf("release provides more than %d target types", maxTargetTypes)
	}
	targetTypes := make(map[string]struct{}, len(release.TargetTypes))
	for _, targetType := range release.TargetTypes {
		if err := validateIdentifier("target type", targetType, maxTargetTypeLength); err != nil {
			return err
		}
		if _, ok := targetTypes[targetType]; ok {
			return fmt.Errorf("duplicate target type %q", targetType)
		}
		targetTypes[targetType] = struct{}{}
	}
	if err := validateRepositoryURL("manifest URL", release.ManifestURL); err != nil {
		return err
	}
	if err := validateSHA256("manifest SHA-256", release.ManifestSHA256); err != nil {
		return err
	}
	if err := validateIdentifier("release channel", release.Channel, maxRepositoryTextBytes); err != nil {
		return err
	}
	if release.ReplacedBy != "" {
		if err := validateIdentifier("replacement plugin ID", release.ReplacedBy, maxPluginIDLength); err != nil {
			return err
		}
	}
	if release.Revoked && release.RevocationReason == "" {
		return errors.New("revocation reason is required")
	}
	if release.RevocationReason != "" {
		if err := validateText("revocation reason", release.RevocationReason, maxRepositoryTextBytes); err != nil {
			return err
		}
	}
	if len(release.Artifacts) == 0 {
		return errors.New("release must contain at least one artifact")
	}
	if len(release.Artifacts) > maxRepositoryArtifacts {
		return fmt.Errorf("release contains more than %d artifacts", maxRepositoryArtifacts)
	}

	platforms := make(map[string]struct{}, len(release.Artifacts))
	for artifactIndex := range release.Artifacts {
		artifact := &release.Artifacts[artifactIndex]
		if err := artifact.validate(); err != nil {
			return fmt.Errorf("artifact %d: %w", artifactIndex, err)
		}
		platform := artifact.OS + "/" + artifact.Arch
		if _, ok := platforms[platform]; ok {
			return fmt.Errorf("duplicate artifact platform %q", platform)
		}
		platforms[platform] = struct{}{}
	}
	return nil
}

func (artifact RepositoryArtifact) validate() error {
	if err := validateIdentifier("artifact OS", artifact.OS, maxRepositoryTextBytes); err != nil {
		return err
	}
	if err := validateIdentifier("artifact architecture", artifact.Arch, maxRepositoryTextBytes); err != nil {
		return err
	}
	if err := validateRepositoryURL("artifact URL", artifact.URL); err != nil {
		return err
	}
	if artifact.Size == 0 {
		return errors.New("artifact size is required")
	}
	if err := validateSHA256("artifact SHA-256", artifact.SHA256); err != nil {
		return err
	}
	if _, err := decodeP256Signature([]byte(artifact.Signature)); err != nil {
		return fmt.Errorf("invalid artifact signature: %w", err)
	}
	return nil
}

func verifyP256Signature(message, signature []byte, publicKey *ecdsa.PublicKey) error {
	digest := sha256.Sum256(message)
	if err := verifyP256Digest(digest[:], signature, publicKey); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidRepositorySignature, err)
	}
	return nil
}

func verifyP256Digest(digest, signature []byte, publicKey *ecdsa.PublicKey) error {
	if err := validateP256PublicKey(publicKey); err != nil {
		return err
	}
	decoded, err := decodeP256Signature(signature)
	if err != nil {
		return err
	}
	if !ecdsa.Verify(publicKey, digest, decoded.R, decoded.S) {
		return errors.New("signature mismatch")
	}
	return nil
}

func validateP256PublicKey(publicKey *ecdsa.PublicKey) error {
	if publicKey == nil || publicKey.Curve != elliptic.P256() {
		return errors.New("invalid P-256 public key")
	}
	if _, err := publicKey.ECDH(); err != nil {
		return errors.New("invalid P-256 public key")
	}
	return nil
}

type p256Signature struct {
	R *big.Int
	S *big.Int
}

func decodeP256Signature(signature []byte) (p256Signature, error) {
	if len(signature) == 0 {
		return p256Signature{}, errors.New("signature is empty")
	}
	if len(signature) > MaxRepositorySignatureBytes {
		return p256Signature{}, fmt.Errorf("signature exceeds %d bytes", MaxRepositorySignatureBytes)
	}
	der, err := base64.StdEncoding.DecodeString(string(bytes.TrimSpace(signature)))
	if err != nil {
		return p256Signature{}, fmt.Errorf("decode base64: %w", err)
	}
	var decoded p256Signature
	rest, err := asn1.Unmarshal(der, &decoded)
	if err != nil {
		return p256Signature{}, fmt.Errorf("decode ASN.1: %w", err)
	}
	if len(rest) != 0 || decoded.R == nil || decoded.S == nil || decoded.R.Sign() <= 0 || decoded.S.Sign() <= 0 {
		return p256Signature{}, errors.New("invalid ASN.1 signature")
	}
	return decoded, nil
}

func validateSHA256(label, value string) error {
	if len(value) != sha256.Size*2 {
		return fmt.Errorf("%s must contain 64 hexadecimal characters", label)
	}
	if _, err := hex.DecodeString(value); err != nil {
		return fmt.Errorf("%s is not hexadecimal: %w", label, err)
	}
	return nil
}

func validateRepositoryURL(label, value string) error {
	parsed, err := url.Parse(value)
	if err != nil || value == "" {
		return fmt.Errorf("%s is invalid", label)
	}
	if parsed.User != nil || parsed.Fragment != "" {
		return fmt.Errorf("%s must not contain credentials or a fragment", label)
	}
	if parsed.IsAbs() {
		if parsed.Scheme != "https" || parsed.Host == "" {
			return fmt.Errorf("%s must use HTTPS", label)
		}
		return nil
	}
	if parsed.Host != "" {
		return fmt.Errorf("%s is invalid", label)
	}
	return nil
}

func validateText(label, value string, maxLength int) error {
	if value == "" {
		return fmt.Errorf("%s is required", label)
	}
	if len(value) > maxLength {
		return fmt.Errorf("%s exceeds %d bytes", label, maxLength)
	}
	for _, char := range value {
		if unicode.IsControl(char) {
			return fmt.Errorf("%s contains a control character", label)
		}
	}
	return nil
}
