//go:build linux

package plugins

import (
	"context"
	"testing"

	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
	"github.com/pbs-plus/pbs-plus/internal/targetplugin/filesystem"
)

func TestLegacySnapshotMetadata(t *testing.T) {
	ctx := context.Background()
	db, err := coredb.Initialize(ctx, t.TempDir()+"/legacy-local.db")
	if err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	defer db.Close()

	if _, ok := LegacySnapshotMetadata(ctx, db, coredb.PluginTarget{
		PluginID: filesystem.PluginID, TargetType: filesystem.TargetTypeLocal,
	}); ok {
		t.Fatal("legacy metadata was synthesized without an installed plugin")
	}
	if _, ok := LegacySnapshotMetadata(ctx, db, coredb.PluginTarget{
		PluginID: "org.pbs-plus.other", TargetType: filesystem.TargetTypeLocal,
	}); ok {
		t.Fatal("legacy metadata was synthesized for a third-party plugin")
	}

	pluginID, version := installFilesystemPlugin(t, ctx, db, "1.0.0")
	legacy := createLocalTarget(t, ctx, db, "docs", "/srv/docs")
	if _, err := ImportLocalTargets(ctx, db); err != nil {
		t.Fatalf("ImportLocalTargets: %v", err)
	}

	metadata, ok := LegacySnapshotMetadata(ctx, db, coredb.PluginTarget{
		Name: legacy.Name, PluginID: pluginID, PluginVersion: version,
		TargetType: filesystem.TargetTypeLocal,
	})
	if !ok {
		t.Fatal("legacy metadata was not synthesized")
	}
	if err := metadata.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if metadata.PluginID != pluginID || metadata.PluginVersion != version || metadata.Archive.Type != filesystem.ArchiveType {
		t.Fatalf("legacy metadata = %#v", metadata)
	}
}
