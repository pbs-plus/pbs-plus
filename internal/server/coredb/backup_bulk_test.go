package coredb

import (
	"context"
	"path/filepath"
	"testing"
)

func TestUpdateBackupsIsAtomic(t *testing.T) {
	db, err := Initialize(context.Background(), filepath.Join(t.TempDir(), "backup-bulk.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	target := Target{Name: "local", Type: TargetTypeFilesystem, Access: FilesystemAccessLocal, Path: t.TempDir()}
	if err := db.CreateTarget(nil, target); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"a", "b"} {
		if err := db.CreateBackup(nil, Backup{ID: id, Store: "store", Target: target, Schedule: "daily", Comment: "keep"}); err != nil {
			t.Fatal(err)
		}
	}

	err = db.UpdateBackups([]string{"a", "missing", "b"}, func(b *Backup) { b.Schedule = "weekly" })
	if err == nil {
		t.Fatal("expected an error for the missing job")
	}
	for _, id := range []string{"a", "b"} {
		got, err := db.GetBackup(id)
		if err != nil {
			t.Fatal(err)
		}
		if got.Schedule != "daily" {
			t.Fatalf("%s: schedule = %q after a failed bulk update", id, got.Schedule)
		}
	}

	if err := db.UpdateBackups([]string{"a", "b"}, func(b *Backup) { b.Schedule = "weekly" }); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"a", "b"} {
		got, err := db.GetBackup(id)
		if err != nil {
			t.Fatal(err)
		}
		if got.Schedule != "weekly" || got.Comment != "keep" {
			t.Fatalf("%s: schedule = %q comment = %q", id, got.Schedule, got.Comment)
		}
	}
}
