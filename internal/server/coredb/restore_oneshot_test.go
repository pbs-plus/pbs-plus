package coredb

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
)

func TestDeleteSucceededOneShotRestoreIsGuarded(t *testing.T) {
	db, err := Initialize(context.Background(), filepath.Join(t.TempDir(), "restore-oneshot.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	target := Target{Name: "local", Type: TargetTypeFilesystem, Access: FilesystemAccessLocal, Path: t.TempDir()}
	if err := db.CreateTarget(nil, target); err != nil {
		t.Fatal(err)
	}
	create := func(id string, oneShot bool, lastRun, lastSuccess string) {
		t.Helper()
		r := Restore{
			ID: id, Store: "store", Snapshot: "host/job/1", DestTarget: target, RemoveOnSuccess: oneShot,
			History: JobHistory{LastRunUpid: lastRun, LastSuccessfulUpid: lastSuccess},
		}
		if err := db.CreateRestore(nil, &r); err != nil {
			t.Fatal(err)
		}
	}
	create("kept-flag-off", false, "u1", "u1")
	create("kept-new-run", true, "u2", "u1")
	create("removed", true, "u1", "u1")

	for _, tc := range []struct {
		id, upid string
		want     bool
	}{
		{"kept-flag-off", "u1", false},
		{"kept-new-run", "u1", false},
		{"removed", "u0", false},
		{"removed", "u1", true},
		{"missing", "u1", false},
	} {
		got, err := db.DeleteSucceededOneShotRestore(tc.id, tc.upid)
		if err != nil {
			t.Fatalf("%s: %v", tc.id, err)
		}
		if got != tc.want {
			t.Errorf("%s/%s: removed = %v, want %v", tc.id, tc.upid, got, tc.want)
		}
	}
	if _, err := db.GetRestore("removed"); !errors.Is(err, ErrRestoreNotFound) {
		t.Errorf("removed restore still exists: %v", err)
	}
	for _, id := range []string{"kept-flag-off", "kept-new-run"} {
		if _, err := db.GetRestore(id); err != nil {
			t.Errorf("%s: %v", id, err)
		}
	}
}
