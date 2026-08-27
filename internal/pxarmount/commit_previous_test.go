package pxarmount

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/pbs-plus/pxar/datastore"
)

func TestNextBackupTimeSkipsExistingSnapshots(t *testing.T) {
	groupDir := t.TempDir()
	for _, ts := range []int64{100, 101} {
		name := time.Unix(ts, 0).UTC().Format("2006-01-02T15:04:05Z")
		if err := os.Mkdir(filepath.Join(groupDir, name), 0o700); err != nil {
			t.Fatal(err)
		}
	}

	got, err := nextBackupTime(groupDir, 100)
	if err != nil {
		t.Fatal(err)
	}
	if got != 102 {
		t.Fatalf("next backup time = %d, want 102", got)
	}
}

func TestPreviousBackupRefRequiresSameGroup(t *testing.T) {
	orig := snapshotRef{
		BackupType: "vm",
		BackupID:   "100",
		BackupTime: 123,
		Namespace:  "source",
	}

	if got := previousBackupRef(orig, "vm", "100", "source", datastore.BackupVM); got == nil {
		t.Fatal("matching backup group did not reuse previous snapshot")
	}

	tests := []struct {
		name       string
		backupType string
		backupID   string
		namespace  string
	}{
		{name: "type", backupType: "ct", backupID: "100", namespace: "source"},
		{name: "id", backupType: "vm", backupID: "101", namespace: "source"},
		{name: "namespace", backupType: "vm", backupID: "100", namespace: "target"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := previousBackupRef(orig, tt.backupType, tt.backupID, tt.namespace, datastore.BackupVM); got != nil {
				t.Fatalf("reused previous snapshot across %s mismatch", tt.name)
			}
		})
	}
}
