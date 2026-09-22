//go:build linux

package restore

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/pbs-plus/pbs-plus/internal/server/application"
	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
	"github.com/pbs-plus/pbs-plus/internal/server/plugins"
	"github.com/pbs-plus/pbs-plus/internal/targetplugin"
	"github.com/pbs-plus/pbs-plus/internal/targetplugin/filesystem"
)

// Reaching the schema-migration guard proves the legacy snapshot fallback
// produced plugin metadata and dispatch entered OpenRestore; a real snapshot
// read is impossible without a PBS datastore.
func TestPluginExecuteRoutesLegacyLocalSnapshots(t *testing.T) {
	ctx := context.Background()
	db, err := coredb.Initialize(ctx, filepath.Join(t.TempDir(), "plugin-restore.db"))
	if err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	defer db.Close()

	if err := db.CreatePluginRepository(ctx, coredb.PluginRepository{
		ID: "org.pbs-plus.restore-tests", Name: "Restore Tests",
		URL: "https://plugins.example.test/index.toml", PublicKey: []byte("key"), Enabled: true,
	}); err != nil {
		t.Fatalf("CreatePluginRepository: %v", err)
	}
	for _, version := range []string{"0.9.0", filesystem.Version} {
		if err := db.RegisterPluginVersion(ctx, "org.pbs-plus.restore-tests", coredb.InstalledPluginVersion{
			PluginID: filesystem.PluginID, Version: version, Platform: "linux/amd64",
			InstallPath: filepath.Join("/plugins/filesystem", version), Manifest: filesystemManifest(t, version),
			ArtifactSHA256: strings.Repeat("ab", 32), InstalledAt: time.Unix(1_700_000_000, 0),
			HealthState: coredb.PluginHealthUnknown,
		}, true); err != nil {
			t.Fatalf("RegisterPluginVersion %s: %v", version, err)
		}
	}

	legacy := coredb.Target{
		Name: "docs", Type: coredb.TargetTypeFilesystem, Access: coredb.FilesystemAccessLocal, Path: "/srv/docs",
	}
	if err := db.CreateTarget(nil, legacy); err != nil {
		t.Fatalf("CreateTarget: %v", err)
	}
	if _, err := plugins.ImportLocalTargets(ctx, db); err != nil {
		t.Fatalf("ImportLocalTargets: %v", err)
	}
	if _, err := db.ActivatePluginVersion(ctx, filesystem.PluginID, "0.9.0"); err != nil {
		t.Fatalf("ActivatePluginVersion: %v", err)
	}

	job := &restoreJob{
		job: coredb.Restore{
			ID: "restore-1", DestTarget: legacy,
			Store: "store", Namespace: "ns", Snapshot: "host/backup/1700000000",
		},
		app:         &application.Runtime{CoreDB: db},
		executionID: "execution-1",
	}
	err = job.pluginExecute(ctx, coredb.PluginTarget{
		Name: legacy.Name, PluginID: filesystem.PluginID, PluginVersion: filesystem.Version,
		TargetType: filesystem.TargetTypeLocal, SchemaVersion: 1,
	}, "attempt-1")
	if err == nil || !strings.Contains(err.Error(), "requires schema migration before restore") {
		t.Fatalf("pluginExecute error = %v, want the schema migration guard", err)
	}
}

func filesystemManifest(t *testing.T, version string) []byte {
	t.Helper()
	descriptor := filesystem.Descriptor()
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
	return manifest
}

func TestRestoreDestinationPath(t *testing.T) {
	cases := []struct {
		name     string
		lease    string
		subpath  string
		expected string
		wantErr  bool
	}{
		{name: "empty subpath keeps the lease root", lease: "/srv/dest", subpath: "", expected: "/srv/dest"},
		{name: "blank subpath keeps the lease root", lease: "/srv/dest", subpath: "  ", expected: "/srv/dest"},
		{name: "subpath joins under the root", lease: "/srv/dest", subpath: "nested/dir", expected: "/srv/dest/nested/dir"},
		{name: "absolute subpath stays under the root", lease: "/srv/dest", subpath: "/etc", expected: "/srv/dest/etc"},
		{name: "dotdot escapes the root", lease: "/srv/dest", subpath: "../escape", wantErr: true},
		{name: "traversal that stays inside the root is allowed", lease: "/srv/dest", subpath: "a/../b", expected: "/srv/dest/b"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := restoreDestinationPath(tc.lease, tc.subpath)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("restoreDestinationPath(%q, %q) = %q, want error", tc.lease, tc.subpath, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("restoreDestinationPath(%q, %q): %v", tc.lease, tc.subpath, err)
			}
			if got != tc.expected {
				t.Fatalf("restoreDestinationPath(%q, %q) = %q, want %q", tc.lease, tc.subpath, got, tc.expected)
			}
		})
	}
}
