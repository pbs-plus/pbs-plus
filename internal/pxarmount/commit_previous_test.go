package pxarmount

import (
	"testing"

	"github.com/pbs-plus/pxar/datastore"
)

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
