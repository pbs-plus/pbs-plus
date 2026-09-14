//go:build linux

package plugins

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
	"github.com/pbs-plus/pbs-plus/internal/targetplugin"
	"github.com/pbs-plus/pbs-plus/internal/targetplugin/filesystem"
)

func TestImportLocalTargets(t *testing.T) {
	ctx := context.Background()
	db, err := coredb.Initialize(ctx, filepath.Join(t.TempDir(), "import-local.db"))
	if err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	defer db.Close()

	if _, err := ImportLocalTargets(ctx, db); err == nil || !strings.Contains(err.Error(), "not installed") {
		t.Fatalf("import without plugin = %v", err)
	}

	descriptor := filesystem.Descriptor()
	digest, err := targetplugin.SchemaDigest(descriptor)
	if err != nil {
		t.Fatalf("SchemaDigest: %v", err)
	}
	manifest, err := toml.Marshal(targetplugin.PluginManifest{
		FormatVersion:   targetplugin.ManifestFormatVersion,
		ProtocolVersion: descriptor.ProtocolVersion,
		PluginID:        descriptor.PluginID,
		Version:         descriptor.Version,
		TargetTypes:     descriptor.TargetTypes,
		SchemaSHA256:    digest,
		TargetSchema:    descriptor.TargetSchema,
		BackupSchema:    descriptor.BackupSchema,
		RestoreSchema:   descriptor.RestoreSchema,
	})
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	repository := coredb.PluginRepository{
		ID: "org.pbs-plus.filesystem-tests", Name: "Filesystem Tests",
		URL: "https://plugins.example.test/index.toml", PublicKey: []byte("key"), Enabled: true,
	}
	if err := db.CreatePluginRepository(ctx, repository); err != nil {
		t.Fatalf("CreatePluginRepository: %v", err)
	}
	if err := db.RegisterPluginVersion(ctx, repository.ID, coredb.InstalledPluginVersion{
		PluginID: descriptor.PluginID, Version: descriptor.Version, Platform: "linux/amd64",
		InstallPath: "/plugins/filesystem/1.0.0", Manifest: manifest,
		ArtifactSHA256: strings.Repeat("ab", 32), InstalledAt: time.Unix(1_700_000_000, 0),
		HealthState: coredb.PluginHealthUnknown,
	}, true); err != nil {
		t.Fatalf("RegisterPluginVersion: %v", err)
	}

	legacy := coredb.Target{
		Name: "docs", Type: coredb.TargetTypeFilesystem, Access: coredb.FilesystemAccessLocal, Path: "/srv/docs",
	}
	if err := db.CreateTarget(nil, legacy); err != nil {
		t.Fatalf("CreateTarget: %v", err)
	}

	imported, err := ImportLocalTargets(ctx, db)
	if err != nil || imported != 1 {
		t.Fatalf("ImportLocalTargets = %d, %v", imported, err)
	}
	pluginTarget, err := db.GetPluginTarget(ctx, legacy.Name)
	if err != nil || pluginTarget.PluginID != filesystem.PluginID ||
		pluginTarget.TargetType != filesystem.TargetTypeLocal || pluginTarget.SchemaVersion != descriptor.TargetSchema.Version {
		t.Fatalf("imported plugin target = %#v, %v", pluginTarget, err)
	}
	var config targetplugin.Values
	if err := targetplugin.UnmarshalProtocol(pluginTarget.Config, &config); err != nil {
		t.Fatalf("UnmarshalProtocol: %v", err)
	}
	if path, _ := config["path"].StringValue(); path != "/srv/docs" {
		t.Fatalf("imported config = %#v", config)
	}
	legacyAfter, err := db.GetTarget(legacy.Name)
	if err != nil || legacyAfter.Type != coredb.TargetTypeFilesystem || legacyAfter.Access != coredb.FilesystemAccessLocal || legacyAfter.Path != "/srv/docs" {
		t.Fatalf("legacy target after import = %#v, %v", legacyAfter, err)
	}

	again, err := ImportLocalTargets(ctx, db)
	if err != nil || again != 0 {
		t.Fatalf("second ImportLocalTargets = %d, %v", again, err)
	}
}
