//go:build linux

package plugins

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pbs-plus/pbs-plus/internal/crypto"
	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
	"github.com/pbs-plus/pbs-plus/internal/targetplugin"
	"github.com/pbs-plus/pbs-plus/internal/targetplugin/filesystem"
	"github.com/pbs-plus/pbs-plus/internal/targetplugin/postgresql"
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

	pluginID, version := installFilesystemPlugin(t, ctx, db, filesystem.Version)
	createLocalTarget(t, ctx, db, "docs", "/srv/docs")

	imported, err := ImportLocalTargets(ctx, db)
	if err != nil || imported != 1 {
		t.Fatalf("ImportLocalTargets = %d, %v", imported, err)
	}
	pluginTarget, err := db.GetPluginTarget(ctx, "docs")
	if err != nil || pluginTarget.PluginID != pluginID ||
		pluginTarget.TargetType != filesystem.TargetTypeLocal || pluginTarget.PluginVersion != version {
		t.Fatalf("imported plugin target = %#v, %v", pluginTarget, err)
	}
	var config targetplugin.Values
	if err := targetplugin.UnmarshalProtocol(pluginTarget.Config, &config); err != nil {
		t.Fatalf("UnmarshalProtocol: %v", err)
	}
	if path, _ := config["path"].StringValue(); path != "/srv/docs" {
		t.Fatalf("imported config = %#v", config)
	}
	legacyAfter, err := db.GetTarget("docs")
	if err != nil || legacyAfter.Type != coredb.TargetTypeFilesystem || legacyAfter.Access != coredb.FilesystemAccessLocal || legacyAfter.Path != "/srv/docs" {
		t.Fatalf("legacy target after import = %#v, %v", legacyAfter, err)
	}

	again, err := ImportLocalTargets(ctx, db)
	if err != nil || again != 0 {
		t.Fatalf("second ImportLocalTargets = %d, %v", again, err)
	}
}

func TestImportPostgreSQLTargets(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	crypto.SetSealKeyPath(filepath.Join(directory, "seal.key"))
	t.Cleanup(func() { crypto.SetSealKeyPath("") })
	db, err := coredb.Initialize(ctx, filepath.Join(directory, "import-postgresql.db"))
	if err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	defer db.Close()

	installTestPlugin(t, ctx, db, postgresql.Descriptor(), postgresql.Version)
	target := coredb.Target{
		Name:                     "inventory",
		Type:                     coredb.TargetTypePostgreSQL,
		DatabaseHost:             "postgres.example",
		DatabasePort:             5433,
		DatabaseUsername:         "backup",
		DatabaseTLSMode:          "verify-full",
		DatabaseCACertificate:    "/etc/ssl/postgres-ca.pem",
		DatabaseDefaultClientDir: "/usr/lib/postgresql/17/bin",
	}
	if err := db.CreateTarget(nil, target); err != nil {
		t.Fatalf("CreateTarget: %v", err)
	}
	if err := db.AddDatabasePassword(nil, target.Name, "database-secret"); err != nil {
		t.Fatalf("AddDatabasePassword: %v", err)
	}

	imported, err := ImportPostgreSQLTargets(ctx, db)
	if err != nil || imported != 1 {
		t.Fatalf("ImportPostgreSQLTargets = %d, %v", imported, err)
	}
	pluginTarget, err := db.GetPluginTarget(ctx, target.Name)
	if err != nil || pluginTarget.PluginID != postgresql.PluginID ||
		pluginTarget.TargetType != postgresql.TargetType || pluginTarget.PluginVersion != postgresql.Version {
		t.Fatalf("imported plugin target = %#v, %v", pluginTarget, err)
	}
	var config targetplugin.Values
	if err := targetplugin.UnmarshalProtocol(pluginTarget.Config, &config); err != nil {
		t.Fatalf("UnmarshalProtocol: %v", err)
	}
	if host, _ := config["host"].StringValue(); host != target.DatabaseHost {
		t.Fatalf("imported config = %#v", config)
	}
	if port, _ := config["port"].IntegerValue(); port != int64(target.DatabasePort) {
		t.Fatalf("imported config = %#v", config)
	}
	secrets, err := db.ResolvePluginTargetSecrets(ctx, target.Name)
	if err != nil || string(secrets["password"]) != "database-secret" {
		t.Fatalf("imported secrets = %#v, %v", secrets, err)
	}
	legacyAfter, err := db.GetTarget(target.Name)
	if err != nil || legacyAfter.Type != coredb.TargetTypePostgreSQL || legacyAfter.DatabaseHost != target.DatabaseHost {
		t.Fatalf("legacy target after import = %#v, %v", legacyAfter, err)
	}
	metadata, ok := LegacySnapshotMetadata(ctx, db, pluginTarget)
	if !ok || metadata.PluginID != postgresql.PluginID || metadata.Archive.Type != postgresql.ArchiveType {
		t.Fatalf("legacy snapshot metadata = %#v, %v", metadata, ok)
	}

	again, err := ImportPostgreSQLTargets(ctx, db)
	if err != nil || again != 0 {
		t.Fatalf("second ImportPostgreSQLTargets = %d, %v", again, err)
	}
}
