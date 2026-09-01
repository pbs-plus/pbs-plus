package coredb

import (
	"context"
	"path/filepath"
	"testing"
)

func TestBackupArchiveExpansionConfigRoundTrip(t *testing.T) {
	db, err := Initialize(context.Background(), filepath.Join(t.TempDir(), "backup-archive-config.db"))
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
	want := Backup{
		ID:               "archive-config",
		Store:            "store",
		Target:           target,
		ExpandArchives:   true,
		ExpandZip:        true,
		ExpandSevenZip:   false,
		ExpandMaxDepth:   -1,
		ExpandMaxEntries: -1,
	}
	if err := db.CreateBackup(nil, want); err != nil {
		t.Fatal(err)
	}

	got, err := db.GetBackup(want.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.ExpandArchives != want.ExpandArchives || got.ExpandZip != want.ExpandZip ||
		got.ExpandSevenZip != want.ExpandSevenZip || got.ExpandMaxDepth != want.ExpandMaxDepth ||
		got.ExpandMaxEntries != want.ExpandMaxEntries {
		t.Fatalf("archive config = %#v", got)
	}
}
