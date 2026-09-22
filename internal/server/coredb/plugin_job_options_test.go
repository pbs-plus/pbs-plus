package coredb

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/targetplugin"
)

func TestPluginJobOptionsPersistence(t *testing.T) {
	ctx := context.Background()
	databasePath := filepath.Join(t.TempDir(), "plugin-job-options.db")
	db, err := Initialize(ctx, databasePath)
	if err != nil {
		t.Fatalf("Initialize: %v", err)
	}

	pluginID, pluginVersion, targetName := createPluginJobOptionsFixture(t, ctx, db)
	backupOptions := pluginJobOptionsValue(t, "daily")
	restoreOptions := pluginJobOptionsValue(t, "overwrite")
	updatedAt := time.Unix(1_700_000_100, 0)
	if err := db.CreateBackup(nil, Backup{
		ID:     "plugin-backup",
		Store:  "store",
		Target: Target{Name: targetName},
		PluginOptions: &PluginJobOptions{
			PluginID:      pluginID,
			PluginVersion: pluginVersion,
			SchemaVersion: 1,
			Options:       backupOptions,
			UpdatedAt:     updatedAt,
		},
	}); err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}
	if err := db.CreateRestore(nil, Restore{
		ID:         "plugin-restore",
		Store:      "store",
		Snapshot:   "host/vm/100/2026-01-01T00:00:00Z",
		SrcPath:    "/",
		DestTarget: Target{Name: targetName},
		PluginOptions: &PluginJobOptions{
			PluginID:      pluginID,
			PluginVersion: pluginVersion,
			SchemaVersion: 2,
			Options:       restoreOptions,
			UpdatedAt:     updatedAt,
		},
	}); err != nil {
		t.Fatalf("CreateRestore: %v", err)
	}

	storedBackup, err := db.GetBackupPluginOptions(ctx, "plugin-backup")
	assertPluginJobOptions(t, storedBackup, err, PluginJobOptions{
		JobID:         "plugin-backup",
		PluginID:      pluginID,
		PluginVersion: pluginVersion,
		SchemaVersion: 1,
		Options:       backupOptions,
		UpdatedAt:     updatedAt,
	})
	storedRestore, err := db.GetRestorePluginOptions(ctx, "plugin-restore")
	assertPluginJobOptions(t, storedRestore, err, PluginJobOptions{
		JobID:         "plugin-restore",
		PluginID:      pluginID,
		PluginVersion: pluginVersion,
		SchemaVersion: 2,
		Options:       restoreOptions,
		UpdatedAt:     updatedAt,
	})
	backup, err := db.GetBackup("plugin-backup")
	if err != nil || backup.PluginOptions == nil || !bytes.Equal(backup.PluginOptions.Options, backupOptions) {
		t.Fatalf("GetBackup plugin options = %#v, %v", backup.PluginOptions, err)
	}
	restore, err := db.GetRestore("plugin-restore")
	if err != nil || restore.PluginOptions == nil || !bytes.Equal(restore.PluginOptions.Options, restoreOptions) {
		t.Fatalf("GetRestore plugin options = %#v, %v", restore.PluginOptions, err)
	}

	backups, err := db.ListBackupPluginOptions(ctx, pluginID, pluginVersion)
	if err != nil || len(backups) != 1 || backups[0].JobID != "plugin-backup" {
		t.Fatalf("ListBackupPluginOptions = %#v, %v", backups, err)
	}
	restores, err := db.ListRestorePluginOptions(ctx, pluginID, pluginVersion)
	if err != nil || len(restores) != 1 || restores[0].JobID != "plugin-restore" {
		t.Fatalf("ListRestorePluginOptions = %#v, %v", restores, err)
	}

	backupOptions = pluginJobOptionsValue(t, "weekly")
	updatedAt = time.Unix(1_700_000_200, 0)
	backup.PluginOptions = &PluginJobOptions{
		PluginID:      pluginID,
		PluginVersion: pluginVersion,
		SchemaVersion: 3,
		Options:       backupOptions,
		UpdatedAt:     updatedAt,
	}
	if err := db.UpdateBackup(nil, backup); err != nil {
		t.Fatalf("UpdateBackup: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	db, err = Initialize(ctx, databasePath)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db.Close()

	storedBackup, err = db.GetBackupPluginOptions(ctx, "plugin-backup")
	assertPluginJobOptions(t, storedBackup, err, PluginJobOptions{
		JobID:         "plugin-backup",
		PluginID:      pluginID,
		PluginVersion: pluginVersion,
		SchemaVersion: 3,
		Options:       backupOptions,
		UpdatedAt:     updatedAt,
	})
	storedRestore, err = db.GetRestorePluginOptions(ctx, "plugin-restore")
	assertPluginJobOptions(t, storedRestore, err, PluginJobOptions{
		JobID:         "plugin-restore",
		PluginID:      pluginID,
		PluginVersion: pluginVersion,
		SchemaVersion: 2,
		Options:       restoreOptions,
		UpdatedAt:     time.Unix(1_700_000_100, 0),
	})

	if err := db.DeleteBackup(nil, "plugin-backup"); err != nil {
		t.Fatalf("DeleteBackup: %v", err)
	}
	if _, err := db.GetBackupPluginOptions(ctx, "plugin-backup"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("GetBackupPluginOptions after delete: %v", err)
	}
	if err := db.DeleteRestore(nil, "plugin-restore"); err != nil {
		t.Fatalf("DeleteRestore: %v", err)
	}
	if _, err := db.GetRestorePluginOptions(ctx, "plugin-restore"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("GetRestorePluginOptions after delete: %v", err)
	}
}

func TestPluginJobOptionsValidation(t *testing.T) {
	db := &Store{}
	valid := PluginJobOptions{
		JobID:         "job",
		PluginID:      "org.pbs-plus.external",
		PluginVersion: "1.0.0",
		SchemaVersion: 1,
		Options:       []byte{0xa0},
	}

	tests := []struct {
		name    string
		options PluginJobOptions
	}{
		{name: "missing identity", options: PluginJobOptions{SchemaVersion: 1, Options: []byte{0xa0}}},
		{name: "zero schema", options: PluginJobOptions{JobID: valid.JobID, PluginID: valid.PluginID, PluginVersion: valid.PluginVersion, Options: valid.Options}},
		{name: "empty options", options: PluginJobOptions{JobID: valid.JobID, PluginID: valid.PluginID, PluginVersion: valid.PluginVersion, SchemaVersion: 1}},
		{name: "oversized options", options: PluginJobOptions{JobID: valid.JobID, PluginID: valid.PluginID, PluginVersion: valid.PluginVersion, SchemaVersion: 1, Options: make([]byte, maxPluginJobOptionsBytes+1)}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := db.UpsertBackupPluginOptions(context.Background(), test.options); err == nil {
				t.Fatal("UpsertBackupPluginOptions succeeded")
			}
			if err := db.UpsertRestorePluginOptions(context.Background(), test.options); err == nil {
				t.Fatal("UpsertRestorePluginOptions succeeded")
			}
		})
	}
}

func createPluginJobOptionsFixture(t *testing.T, ctx context.Context, db *Store) (string, string, string) {
	t.Helper()
	repository := PluginRepository{
		ID:        "org.pbs-plus.job-tests",
		Name:      "PBS Plus Job Tests",
		URL:       "https://plugins.example.test/index.toml",
		PublicKey: []byte("test-key"),
		Enabled:   true,
	}
	if err := db.CreatePluginRepository(ctx, repository); err != nil {
		t.Fatalf("CreatePluginRepository: %v", err)
	}
	version := InstalledPluginVersion{
		PluginID:       "org.pbs-plus.job-external",
		Version:        "1.0.0",
		Platform:       "linux/amd64",
		InstallPath:    "/plugins/org.pbs-plus.job-external/1.0.0",
		Manifest:       []byte("manifest"),
		ArtifactSHA256: strings.Repeat("ab", 32),
		InstalledAt:    time.Unix(1_700_000_000, 0),
		HealthState:    PluginHealthUnknown,
	}
	if err := db.RegisterPluginVersion(ctx, repository.ID, version, true); err != nil {
		t.Fatalf("RegisterPluginVersion: %v", err)
	}
	targetName := "plugin-job-target"
	if err := db.CreatePluginTarget(ctx, PluginTarget{
		Name:          targetName,
		PluginID:      version.PluginID,
		PluginVersion: version.Version,
		TargetType:    "external-filesystem",
		SchemaVersion: 1,
		Config:        pluginJobOptionsValue(t, "/srv/source"),
	}, nil); err != nil {
		t.Fatalf("CreatePluginTarget: %v", err)
	}
	return version.PluginID, version.Version, targetName
}

func pluginJobOptionsValue(t *testing.T, value string) []byte {
	t.Helper()
	encoded, err := targetplugin.MarshalProtocol(targetplugin.Values{
		"value": targetplugin.NewStringScalar(value),
	})
	if err != nil {
		t.Fatalf("MarshalProtocol: %v", err)
	}
	return encoded
}

func assertPluginJobOptions(t *testing.T, got PluginJobOptions, err error, want PluginJobOptions) {
	t.Helper()
	if err != nil {
		t.Fatalf("get plugin job options: %v", err)
	}
	if got.JobID != want.JobID || got.PluginID != want.PluginID || got.PluginVersion != want.PluginVersion ||
		got.SchemaVersion != want.SchemaVersion || !bytes.Equal(got.Options, want.Options) || !got.UpdatedAt.Equal(want.UpdatedAt) {
		t.Fatalf("plugin job options = %#v, want %#v", got, want)
	}
}
