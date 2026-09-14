package targetplugin

import (
	"errors"
	"fmt"

	"github.com/Masterminds/semver"
	"github.com/pbs-plus/pbs-plus/internal/proxmox"
)

const (
	SnapshotMetadataFormatVersion uint16 = 1
	SnapshotMetadataArchiveName          = proxmox.PluginMetadataArchiveName
	SnapshotMetadataFileName             = "metadata.cbor"
)

// SnapshotMetadata identifies the plugin contract needed to restore an archive.
type SnapshotMetadata struct {
	FormatVersion       uint16  `cbor:"format_version"`
	PluginID            string  `cbor:"plugin_id"`
	PluginVersion       string  `cbor:"plugin_version"`
	TargetType          string  `cbor:"target_type"`
	TargetSchemaVersion uint32  `cbor:"target_schema_version"`
	BackupSchemaVersion uint32  `cbor:"backup_schema_version"`
	Archive             Archive `cbor:"archive"`
}

// Validate checks the persisted plugin and archive identity.
func (metadata SnapshotMetadata) Validate() error {
	if metadata.FormatVersion != SnapshotMetadataFormatVersion {
		return fmt.Errorf("unsupported snapshot metadata format %d", metadata.FormatVersion)
	}
	if err := validateIdentifier("snapshot plugin ID", metadata.PluginID, maxPluginIDLength); err != nil {
		return err
	}
	if metadata.PluginVersion == "" {
		return errors.New("snapshot plugin version is required")
	}
	if len(metadata.PluginVersion) > maxVersionLength {
		return fmt.Errorf("snapshot plugin version exceeds %d bytes", maxVersionLength)
	}
	if _, err := semver.NewVersion(metadata.PluginVersion); err != nil {
		return fmt.Errorf("invalid snapshot plugin version: %w", err)
	}
	if err := validateIdentifier("snapshot target type", metadata.TargetType, maxTargetTypeLength); err != nil {
		return err
	}
	if metadata.TargetSchemaVersion == 0 {
		return errors.New("snapshot target schema version is required")
	}
	if metadata.BackupSchemaVersion == 0 {
		return errors.New("snapshot backup schema version is required")
	}
	return metadata.Archive.Validate()
}
