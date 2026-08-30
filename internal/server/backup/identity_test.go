//go:build linux

package backup

import (
	"testing"

	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
)

func TestBackupIdentityIsPerJobNotPerTarget(t *testing.T) {
	target := coredb.Target{
		Name:      "IT-D002 - D",
		Type:      coredb.TargetTypeFilesystem,
		Access:    coredb.FilesystemAccessAgent,
		Path:      "/mnt/data",
		AgentHost: coredb.AgentHost{Name: "IT-D002"},
	}
	first := coredb.Backup{ID: "IT-D002-D-timesheets", Store: "test-datastore", Subpath: "Timesheets", Target: target}
	second := coredb.Backup{ID: "IT-D002-D-payroll", Store: "test-datastore", Subpath: "Payroll", Target: target}

	firstID, err := getBackupId(first)
	if err != nil {
		t.Fatal(err)
	}
	secondID, err := getBackupId(second)
	if err != nil {
		t.Fatal(err)
	}
	if firstID == secondID {
		t.Fatalf("two jobs on one target share backup ID %q; concurrent runs would collide", firstID)
	}
	if firstID != first.ID || secondID != second.ID {
		t.Fatalf("backup IDs = %q, %q, want %q, %q", firstID, secondID, first.ID, second.ID)
	}

	firstWID, err := backupWorkerID(first)
	if err != nil {
		t.Fatal(err)
	}
	secondWID, err := backupWorkerID(second)
	if err != nil {
		t.Fatal(err)
	}
	if firstWID == secondWID {
		t.Fatalf("two jobs on one target share worker ID %q; task discovery would be ambiguous", firstWID)
	}

	if _, err := getBackupId(coredb.Backup{Store: "test-datastore", Target: target}); err == nil {
		t.Fatal("getBackupId accepted a job with no ID")
	}
}
