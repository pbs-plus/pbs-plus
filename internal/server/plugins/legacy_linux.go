//go:build linux

package plugins

import (
	"context"

	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
	"github.com/pbs-plus/pbs-plus/internal/targetplugin"
	"github.com/pbs-plus/pbs-plus/internal/targetplugin/filesystem"
)

// LegacyLocalSnapshotMetadata synthesizes plugin metadata for local filesystem
// snapshots written before plugin metadata archives existed. The host owns
// this fixed mapping; third-party plugins cannot claim legacy snapshots.
func LegacyLocalSnapshotMetadata(ctx context.Context, db *coredb.Store, target coredb.PluginTarget) (targetplugin.SnapshotMetadata, bool) {
	if target.PluginID != filesystem.PluginID || target.TargetType != filesystem.TargetTypeLocal {
		return targetplugin.SnapshotMetadata{}, false
	}
	manifest, _, err := loadActiveManifest(ctx, db, target.PluginID)
	if err != nil {
		return targetplugin.SnapshotMetadata{}, false
	}
	metadata := targetplugin.SnapshotMetadata{
		FormatVersion:       targetplugin.SnapshotMetadataFormatVersion,
		PluginID:            target.PluginID,
		PluginVersion:       manifest.Version,
		TargetType:          target.TargetType,
		TargetSchemaVersion: manifest.TargetSchema.Version,
		BackupSchemaVersion: manifest.BackupSchema.Version,
		Archive:             targetplugin.Archive{Type: filesystem.ArchiveType, FormatVersion: filesystem.ArchiveFormatVersion},
	}
	if err := metadata.Validate(); err != nil {
		return targetplugin.SnapshotMetadata{}, false
	}
	return metadata, true
}
