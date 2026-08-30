//go:build linux

package coredb

import (
	"context"
	"path/filepath"
	"testing"
)

func TestBackupGroupMigrationCompletionIsPerJob(t *testing.T) {
	db, err := Initialize(context.Background(), filepath.Join(t.TempDir(), "backup-group-migration.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	target := Target{
		Name:   "local",
		Type:   TargetTypeFilesystem,
		Access: FilesystemAccessLocal,
		Path:   t.TempDir(),
	}
	if err := db.CreateTarget(nil, target); err != nil {
		t.Fatal(err)
	}
	backup := Backup{ID: "job", Store: "store", Target: target}
	if err := db.CreateBackup(nil, backup); err != nil {
		t.Fatal(err)
	}

	completed, err := db.BackupGroupMigrationCompleted(backup.ID)
	if err != nil {
		t.Fatal(err)
	}
	if completed {
		t.Fatal("new backup job migration is already complete")
	}
	if err := db.CompleteBackupGroupMigration(backup.ID); err != nil {
		t.Fatal(err)
	}
	if err := db.CompleteBackupGroupMigration(backup.ID); err != nil {
		t.Fatal(err)
	}
	completed, err = db.BackupGroupMigrationCompleted(backup.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !completed {
		t.Fatal("backup job migration completion was not persisted")
	}

	if err := db.DeleteBackup(nil, backup.ID); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateBackup(nil, backup); err != nil {
		t.Fatal(err)
	}
	completed, err = db.BackupGroupMigrationCompleted(backup.ID)
	if err != nil {
		t.Fatal(err)
	}
	if completed {
		t.Fatal("recreated backup job inherited migration completion")
	}
}
