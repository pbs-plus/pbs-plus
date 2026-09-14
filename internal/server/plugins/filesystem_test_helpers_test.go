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

// installFilesystemPlugin registers and activates the given filesystem plugin version.
func installFilesystemPlugin(t *testing.T, ctx context.Context, db *coredb.Store, version string) (pluginID, active string) {
	t.Helper()
	return installTestPlugin(t, ctx, db, filesystem.Descriptor(), version)
}

func installTestPlugin(t *testing.T, ctx context.Context, db *coredb.Store, descriptor targetplugin.Descriptor, version string) (pluginID, active string) {
	t.Helper()
	digest, err := targetplugin.SchemaDigest(descriptor)
	if err != nil {
		t.Fatalf("SchemaDigest: %v", err)
	}
	manifest, err := toml.Marshal(targetplugin.PluginManifest{
		FormatVersion:   targetplugin.ManifestFormatVersion,
		ProtocolVersion: descriptor.ProtocolVersion,
		PluginID:        descriptor.PluginID,
		Version:         version,
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
		ID: descriptor.PluginID + "-tests", Name: "Plugin Tests",
		URL: "https://plugins.example.test/index.toml", PublicKey: []byte("key"), Enabled: true,
	}
	if err := db.CreatePluginRepository(ctx, repository); err != nil {
		t.Fatalf("CreatePluginRepository: %v", err)
	}
	if err := db.RegisterPluginVersion(ctx, repository.ID, coredb.InstalledPluginVersion{
		PluginID: descriptor.PluginID, Version: version, Platform: "linux/amd64",
		InstallPath: filepath.Join("/plugins", descriptor.PluginID, version), Manifest: manifest,
		ArtifactSHA256: strings.Repeat("ab", 32), InstalledAt: time.Unix(1_700_000_000, 0),
		HealthState: coredb.PluginHealthUnknown,
	}, true); err != nil {
		t.Fatalf("RegisterPluginVersion: %v", err)
	}
	return descriptor.PluginID, version
}

func createLocalTarget(t *testing.T, ctx context.Context, db *coredb.Store, name, path string) coredb.Target {
	t.Helper()
	target := coredb.Target{
		Name: name, Type: coredb.TargetTypeFilesystem, Access: coredb.FilesystemAccessLocal, Path: path,
	}
	if err := db.CreateTarget(nil, target); err != nil {
		t.Fatalf("CreateTarget: %v", err)
	}
	return target
}
