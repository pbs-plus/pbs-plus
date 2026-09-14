//go:build linux

package plugins

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
	"github.com/pbs-plus/pbs-plus/internal/targetplugin"
)

func TestSelectRestorePlugin(t *testing.T) {
	ctx := context.Background()
	db, err := coredb.Initialize(ctx, filepath.Join(t.TempDir(), "restore-compat.db"))
	if err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	defer db.Close()

	const pluginID = "org.pbs-plus.compat"
	repository := coredb.PluginRepository{
		ID: "org.pbs-plus.compat-tests", Name: "Compat Tests",
		URL: "https://plugins.example.test/index.toml", PublicKey: []byte("key"), Enabled: true,
	}
	if err := db.CreatePluginRepository(ctx, repository); err != nil {
		t.Fatalf("CreatePluginRepository: %v", err)
	}
	for _, version := range []struct {
		version  string
		schema   uint32
		activate bool
	}{
		{version: "1.0.0", schema: 1},
		{version: "1.3.0", schema: 3, activate: true},
		{version: "2.0.0", schema: 4},
	} {
		if err := db.RegisterPluginVersion(ctx, repository.ID, coredb.InstalledPluginVersion{
			PluginID: pluginID, Version: version.version, Platform: "linux/amd64",
			InstallPath:    filepath.Join("/plugins", pluginID, version.version, "plugin"),
			Manifest:       compatManifest(t, pluginID, version.version, version.schema),
			ArtifactSHA256: strings.Repeat("ab", 32), InstalledAt: time.Unix(1_700_000_000, 0),
			HealthState: coredb.PluginHealthUnknown,
		}, version.activate); err != nil {
			t.Fatalf("RegisterPluginVersion %s: %v", version.version, err)
		}
	}

	metadata := targetplugin.SnapshotMetadata{
		FormatVersion: targetplugin.SnapshotMetadataFormatVersion,
		PluginID:      pluginID, PluginVersion: "1.1.0", TargetType: "compat",
		TargetSchemaVersion: 2, BackupSchemaVersion: 2,
		Archive: targetplugin.Archive{Type: "compat", FormatVersion: 1},
	}
	_, selected, err := SelectRestorePlugin(ctx, db, metadata)
	if err != nil || selected.Version != "1.3.0" {
		t.Fatalf("SelectRestorePlugin = %q, %v, want active 1.3.0", selected.Version, err)
	}

	olderThanActive := metadata
	olderThanActive.PluginVersion = "2.0.0"
	olderThanActive.TargetSchemaVersion = 4
	olderThanActive.BackupSchemaVersion = 4
	_, selected, err = SelectRestorePlugin(ctx, db, olderThanActive)
	if err != nil || selected.Version != "2.0.0" {
		t.Fatalf("SelectRestorePlugin = %q, %v, want 2.0.0", selected.Version, err)
	}

	tests := []struct {
		name     string
		mutate   func(*targetplugin.SnapshotMetadata)
		wantHint string
	}{
		{
			name:     "plugin not installed",
			mutate:   func(m *targetplugin.SnapshotMetadata) { m.PluginID = "org.pbs-plus.missing" },
			wantHint: "not installed",
		},
		{
			name:     "unknown target type",
			mutate:   func(m *targetplugin.SnapshotMetadata) { m.TargetType = "other" },
			wantHint: "compatible archive contract",
		},
		{
			name:     "schema newer than every installed version",
			mutate:   func(m *targetplugin.SnapshotMetadata) { m.BackupSchemaVersion = 9 },
			wantHint: "compatible archive contract",
		},
		{
			name:     "major version never installed",
			mutate:   func(m *targetplugin.SnapshotMetadata) { m.PluginVersion = "3.0.0" },
			wantHint: "compatible archive contract",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			broken := metadata
			test.mutate(&broken)
			_, _, err := SelectRestorePlugin(ctx, db, broken)
			var unavailable *RestorePluginUnavailableError
			if !errors.As(err, &unavailable) {
				t.Fatalf("SelectRestorePlugin error = %v, want RestorePluginUnavailableError", err)
			}
			if !strings.Contains(unavailable.Reason, test.wantHint) {
				t.Fatalf("reason = %q, want %q", unavailable.Reason, test.wantHint)
			}
			if !strings.Contains(unavailable.Error(), PluginInstallURL) {
				t.Fatalf("error = %q, want install guidance", unavailable.Error())
			}
		})
	}

	if _, err := db.SetInstalledPluginEnabled(ctx, pluginID, false); err != nil {
		t.Fatalf("SetInstalledPluginEnabled: %v", err)
	}
	var unavailable *RestorePluginUnavailableError
	if _, _, err := SelectRestorePlugin(ctx, db, metadata); !errors.As(err, &unavailable) || !strings.Contains(unavailable.Reason, "disabled") {
		t.Fatalf("disabled plugin error = %v", err)
	}
}

func compatManifest(t *testing.T, pluginID, version string, schemaVersion uint32) []byte {
	t.Helper()
	descriptor := targetplugin.Descriptor{
		ProtocolVersion: targetplugin.CurrentProtocolVersion,
		PluginID:        pluginID,
		Version:         version,
		TargetTypes:     []string{"compat"},
		TargetSchema:    targetplugin.FormSchema{Version: schemaVersion},
		BackupSchema:    targetplugin.FormSchema{Version: schemaVersion},
		RestoreSchema:   targetplugin.FormSchema{Version: schemaVersion},
	}
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
	return manifest
}
