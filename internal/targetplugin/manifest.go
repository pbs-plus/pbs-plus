package targetplugin

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/BurntSushi/toml"
)

const (
	ManifestFormatVersion uint16 = 1
	MaxManifestBytes             = 4 << 20
)

// ErrInvalidManifestDigest reports that downloaded manifest bytes do not match the repository index.
var ErrInvalidManifestDigest = errors.New("invalid plugin manifest digest")

// PluginManifest is the authenticated install metadata advertised by a repository release.
type PluginManifest struct {
	FormatVersion   uint16     `toml:"format_version"`
	ProtocolVersion uint16     `toml:"protocol"`
	PluginID        string     `toml:"plugin_id"`
	Version         string     `toml:"version"`
	TargetTypes     []string   `toml:"target_types"`
	SchemaSHA256    string     `toml:"schema_sha256"`
	TargetSchema    FormSchema `toml:"target_schema"`
	BackupSchema    FormSchema `toml:"backup_schema"`
	RestoreSchema   FormSchema `toml:"restore_schema"`
}

// ParsePluginManifest verifies exact downloaded bytes before strict TOML decoding.
func ParsePluginManifest(manifestBytes []byte, expectedSHA256 string) (PluginManifest, error) {
	if len(manifestBytes) == 0 {
		return PluginManifest{}, errors.New("plugin manifest is empty")
	}
	if len(manifestBytes) > MaxManifestBytes {
		return PluginManifest{}, fmt.Errorf("plugin manifest exceeds %d bytes", MaxManifestBytes)
	}
	if err := validateSHA256("expected manifest SHA-256", expectedSHA256); err != nil {
		return PluginManifest{}, err
	}
	digest := sha256.Sum256(manifestBytes)
	if !strings.EqualFold(hex.EncodeToString(digest[:]), expectedSHA256) {
		return PluginManifest{}, ErrInvalidManifestDigest
	}

	var manifest PluginManifest
	metadata, err := toml.Decode(string(manifestBytes), &manifest)
	if err != nil {
		return PluginManifest{}, fmt.Errorf("decode plugin manifest: %w", err)
	}
	if undecoded := metadata.Undecoded(); len(undecoded) > 0 {
		return PluginManifest{}, fmt.Errorf("plugin manifest contains unknown field %q", undecoded[0].String())
	}
	if err := manifest.Validate(); err != nil {
		return PluginManifest{}, fmt.Errorf("validate plugin manifest: %w", err)
	}
	return manifest, nil
}

// Validate checks manifest identity, form schemas, and the advertised schema digest.
func (manifest PluginManifest) Validate() error {
	if manifest.FormatVersion != ManifestFormatVersion {
		return fmt.Errorf("unsupported plugin manifest format %d", manifest.FormatVersion)
	}
	descriptor := manifest.descriptor()
	if err := descriptor.Validate(); err != nil {
		return err
	}
	if err := validateSHA256("schema SHA-256", manifest.SchemaSHA256); err != nil {
		return err
	}
	digest, err := descriptorSchemaDigest(descriptor)
	if err != nil {
		return err
	}
	if !strings.EqualFold(digest, manifest.SchemaSHA256) {
		return errors.New("schema SHA-256 does not match manifest forms")
	}
	return nil
}

// VerifyRelease checks that a manifest matches its authenticated repository entry.
func (manifest PluginManifest) VerifyRelease(release RepositoryRelease) error {
	if err := manifest.Validate(); err != nil {
		return fmt.Errorf("manifest: %w", err)
	}
	if err := release.validate(); err != nil {
		return fmt.Errorf("release: %w", err)
	}
	if manifest.ProtocolVersion != release.ProtocolVersion {
		return errors.New("manifest protocol does not match release")
	}
	if manifest.PluginID != release.PluginID {
		return errors.New("manifest plugin ID does not match release")
	}
	if manifest.Version != release.Version {
		return errors.New("manifest version does not match release")
	}
	if !slices.Equal(manifest.TargetTypes, release.TargetTypes) {
		return errors.New("manifest target types do not match release")
	}
	return nil
}

// VerifyDescriptor checks that an installed executable matches its authenticated manifest.
func (manifest PluginManifest) VerifyDescriptor(descriptor Descriptor) error {
	if err := manifest.Validate(); err != nil {
		return fmt.Errorf("manifest: %w", err)
	}
	if err := descriptor.Validate(); err != nil {
		return fmt.Errorf("descriptor: %w", err)
	}
	if descriptor.ProtocolVersion != manifest.ProtocolVersion {
		return errors.New("descriptor protocol does not match manifest")
	}
	if descriptor.PluginID != manifest.PluginID {
		return errors.New("descriptor plugin ID does not match manifest")
	}
	if descriptor.Version != manifest.Version {
		return errors.New("descriptor version does not match manifest")
	}
	if !slices.Equal(descriptor.TargetTypes, manifest.TargetTypes) {
		return errors.New("descriptor target types do not match manifest")
	}
	digest, err := descriptorSchemaDigest(descriptor)
	if err != nil {
		return err
	}
	if !strings.EqualFold(digest, manifest.SchemaSHA256) {
		return errors.New("descriptor schemas do not match manifest")
	}
	return nil
}

func (manifest PluginManifest) descriptor() Descriptor {
	return Descriptor{
		ProtocolVersion: manifest.ProtocolVersion,
		PluginID:        manifest.PluginID,
		Version:         manifest.Version,
		TargetTypes:     manifest.TargetTypes,
		TargetSchema:    manifest.TargetSchema,
		BackupSchema:    manifest.BackupSchema,
		RestoreSchema:   manifest.RestoreSchema,
	}
}

func descriptorSchemaDigest(descriptor Descriptor) (string, error) {
	schemas := struct {
		Target  FormSchema `cbor:"target"`
		Backup  FormSchema `cbor:"backup"`
		Restore FormSchema `cbor:"restore"`
	}{
		Target:  descriptor.TargetSchema,
		Backup:  descriptor.BackupSchema,
		Restore: descriptor.RestoreSchema,
	}
	encoded, err := MarshalProtocol(schemas)
	if err != nil {
		return "", fmt.Errorf("encode plugin schemas: %w", err)
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}
