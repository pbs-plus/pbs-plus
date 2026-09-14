//go:build linux

package plugins

import (
	"context"

	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
	"github.com/pbs-plus/pbs-plus/internal/targetplugin"
	"github.com/pbs-plus/pbs-plus/internal/targetplugin/dovecot"
	"github.com/pbs-plus/pbs-plus/internal/targetplugin/filesystem"
	"github.com/pbs-plus/pbs-plus/internal/targetplugin/ldap"
	"github.com/pbs-plus/pbs-plus/internal/targetplugin/mysql"
	"github.com/pbs-plus/pbs-plus/internal/targetplugin/postgresql"
	"github.com/pbs-plus/pbs-plus/internal/targetplugin/s3"
)

// LegacySnapshotMetadata maps pre-plugin snapshots only to fixed first-party archive contracts.
func LegacySnapshotMetadata(ctx context.Context, db *coredb.Store, target coredb.PluginTarget) (targetplugin.SnapshotMetadata, bool) {
	var archive targetplugin.Archive
	switch {
	case target.PluginID == filesystem.PluginID && target.TargetType == filesystem.TargetTypeLocal:
		archive = targetplugin.Archive{Type: filesystem.ArchiveType, FormatVersion: filesystem.ArchiveFormatVersion}
	case target.PluginID == postgresql.PluginID && target.TargetType == postgresql.TargetType:
		archive = targetplugin.Archive{Type: postgresql.ArchiveType, FormatVersion: postgresql.ArchiveFormatVersion}
	case target.PluginID == mysql.PluginID && target.TargetType == mysql.TargetType:
		archive = targetplugin.Archive{Type: mysql.ArchiveType, FormatVersion: mysql.ArchiveFormatVersion}
	case target.PluginID == ldap.PluginID && target.TargetType == ldap.TargetType:
		archive = targetplugin.Archive{Type: ldap.ArchiveType, FormatVersion: ldap.ArchiveFormatVersion}
	case target.PluginID == dovecot.PluginID && target.TargetType == dovecot.TargetType:
		archive = targetplugin.Archive{Type: dovecot.ArchiveType, FormatVersion: dovecot.ArchiveFormatVersion}
	case target.PluginID == s3.PluginID && target.TargetType == s3.TargetType:
		archive = targetplugin.Archive{Type: s3.ArchiveType, FormatVersion: s3.ArchiveFormatVersion}
	default:
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
		Archive:             archive,
	}
	if err := metadata.Validate(); err != nil {
		return targetplugin.SnapshotMetadata{}, false
	}
	return metadata, true
}
