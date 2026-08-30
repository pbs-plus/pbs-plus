//go:build linux

package backup

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
)

func TestMigrateLegacyBackupGroupPreservesHistory(t *testing.T) {
	root := t.TempDir()
	store := filepath.Join(root, "store")
	locks := filepath.Join(root, "locks")
	backup := migrationTestBackup()
	source := filepath.Join(store, "ns", "IT", "host", "IT-D002")
	target := filepath.Join(store, "ns", "IT", "host", backup.ID)
	snapshot := filepath.Join(source, "2026-08-27T16:31:30Z")
	if err := os.MkdirAll(snapshot, 0o755); err != nil {
		t.Fatal(err)
	}
	index := filepath.Join(snapshot, "IT-D002---D.mpxar.didx")
	if err := os.WriteFile(index, []byte("index"), 0o640); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(snapshot, "IT-D002---D.ppxar.didx"), []byte("payload"), 0o640); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(index)
	if err != nil {
		t.Fatal(err)
	}

	migrated, err := migrateLegacyBackupGroupAt(backup, store, locks, "IT-D002")
	if err != nil {
		t.Fatal(err)
	}
	if !migrated {
		t.Fatal("legacy group was not migrated")
	}
	if _, err := os.Stat(source); !os.IsNotExist(err) {
		t.Fatalf("legacy group still exists: %v", err)
	}
	after, err := os.Stat(filepath.Join(target, "2026-08-27T16:31:30Z", filepath.Base(index)))
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(before, after) {
		t.Fatal("snapshot index was copied instead of atomically renamed")
	}
	journal, err := os.ReadFile(filepath.Join(locks, backup.Store, "move-journal"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(journal), filepath.Join(target, "2026-08-27T16:31:30Z", filepath.Base(index))) {
		t.Fatalf("move journal does not contain migrated index: %s", journal)
	}

	migrated, err = migrateLegacyBackupGroupAt(backup, store, locks, "IT-D002")
	if err != nil || migrated {
		t.Fatalf("second migration = %v, %v; want false, nil", migrated, err)
	}
}

func TestMigrateLegacyBackupGroupRejectsMixedArchives(t *testing.T) {
	root := t.TempDir()
	store := filepath.Join(root, "store")
	backup := migrationTestBackup()
	snapshot := filepath.Join(store, "ns", "IT", "host", "IT-D002", "2026-08-27T16:31:30Z")
	if err := os.MkdirAll(snapshot, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(snapshot, "another-job.pxar.didx"), nil, 0o640); err != nil {
		t.Fatal(err)
	}

	migrated, err := migrateLegacyBackupGroupAt(backup, store, filepath.Join(root, "locks"), "IT-D002")
	if err == nil || migrated || !strings.Contains(err.Error(), "another job") {
		t.Fatalf("migration = %v, %v; want mixed-archive error", migrated, err)
	}
	if _, err := os.Stat(filepath.Join(store, "ns", "IT", "host", "IT-D002")); err != nil {
		t.Fatalf("legacy group changed after rejected migration: %v", err)
	}
}

func TestMigrateLegacyBackupGroupMergesExistingPerJobGroup(t *testing.T) {
	root := t.TempDir()
	store := filepath.Join(root, "store")
	backup := migrationTestBackup()
	source := filepath.Join(store, "ns", "IT", "host", "IT-D002")
	target := filepath.Join(store, "ns", "IT", "host", backup.ID)
	oldSnapshot := "2026-08-27T16:31:30Z"
	newSnapshot := "2026-08-30T20:54:46Z"
	for _, group := range []string{source, target} {
		if err := os.MkdirAll(group, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(group, "owner"), []byte("plus-user@pbs!server\n"), 0o640); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(source, "notes"), []byte("legacy notes"), 0o640); err != nil {
		t.Fatal(err)
	}
	for group, snapshot := range map[string]string{source: oldSnapshot, target: newSnapshot} {
		dir := filepath.Join(group, snapshot)
		if err := os.Mkdir(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "IT-D002---D.mpxar.didx"), nil, 0o640); err != nil {
			t.Fatal(err)
		}
	}

	migrated, err := migrateLegacyBackupGroupAt(backup, store, filepath.Join(root, "locks"), "IT-D002")
	if err != nil || !migrated {
		t.Fatalf("migration = %v, %v; want true, nil", migrated, err)
	}
	if _, err := os.Stat(source); !os.IsNotExist(err) {
		t.Fatalf("legacy group still exists: %v", err)
	}
	for _, snapshot := range []string{oldSnapshot, newSnapshot} {
		if _, err := os.Stat(filepath.Join(target, snapshot)); err != nil {
			t.Fatalf("merged snapshot %q: %v", snapshot, err)
		}
	}
	notes, err := os.ReadFile(filepath.Join(target, "notes"))
	if err != nil || string(notes) != "legacy notes" {
		t.Fatalf("merged notes = %q, %v", notes, err)
	}
}

func TestMigrateLegacyBackupGroupRejectsOverlappingSnapshots(t *testing.T) {
	root := t.TempDir()
	store := filepath.Join(root, "store")
	backup := migrationTestBackup()
	for _, id := range []string{"IT-D002", backup.ID} {
		group := filepath.Join(store, "ns", "IT", "host", id)
		snapshot := filepath.Join(group, "2026-08-27T16:31:30Z")
		if err := os.MkdirAll(snapshot, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(group, "owner"), []byte("owner"), 0o640); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(snapshot, "IT-D002---D.mpxar.didx"), nil, 0o640); err != nil {
			t.Fatal(err)
		}
	}

	migrated, err := migrateLegacyBackupGroupAt(backup, store, filepath.Join(root, "locks"), "IT-D002")
	if err == nil || migrated || !strings.Contains(err.Error(), "both contain snapshot") {
		t.Fatalf("migration = %v, %v; want overlap error", migrated, err)
	}
}

func TestMigrateLegacyBackupGroupStopsWhenPBSHoldsLock(t *testing.T) {
	root := t.TempDir()
	store := filepath.Join(root, "store")
	locks := filepath.Join(root, "locks")
	backup := migrationTestBackup()
	source := filepath.Join(store, "ns", "IT", "host", "IT-D002")
	if err := os.MkdirAll(source, 0o755); err != nil {
		t.Fatal(err)
	}
	lock, err := lockPBSPath(pbsLockPath(locks, backup.Store, backup.Namespace, "host", "IT-D002"))
	if err != nil {
		t.Fatal(err)
	}
	defer closePBSLocks([]*os.File{lock})
	migrated, err := migrateLegacyBackupGroupAt(backup, store, locks, "IT-D002")
	if err == nil || migrated || !strings.Contains(err.Error(), "in use") {
		t.Fatalf("migration = %v, %v; want lock conflict", migrated, err)
	}
	if _, err := os.Stat(source); err != nil {
		t.Fatalf("legacy group changed while locked: %v", err)
	}
}

func TestLegacyGroupMembersFindsSharedGroup(t *testing.T) {
	first := migrationTestBackup()
	second := first
	second.ID = "IT-D002-timesheets"
	third := first
	third.ID = "another-namespace"
	third.Namespace = "Other"

	members, err := legacyGroupMembers([]coredb.Backup{third, second, first}, first, "IT-D002")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(members, ",") != "IT-D002-D,IT-D002-timesheets" {
		t.Fatalf("shared legacy members = %v", members)
	}
}

func TestPBSLockPathMatchesProxmoxFormat(t *testing.T) {
	got := pbsLockPath("/run/proxmox-backup/locks", "test-datastore", "IT", "host", "IT-D002", "2026-08-27T16:31:30Z")
	want := `/run/proxmox-backup/locks/test-datastore/IT/host-IT\x2dD002-2026\x2d08\x2d27T16\x3a31\x3a30Z`
	if got != want {
		t.Fatalf("PBS lock path = %q, want %q", got, want)
	}
}

func TestPBSLockPathHashesLongNames(t *testing.T) {
	id := strings.Repeat("job-", 70)
	rpath := "host/" + id
	digest := sha256.Sum256([]byte(rpath))
	got := pbsLockPath("/locks", "store", "", "host", id)
	if !strings.Contains(got, "/hashed/") || !strings.HasSuffix(got, "-"+hex.EncodeToString(digest[:])) {
		t.Fatalf("hashed PBS lock path = %q", got)
	}
}

func migrationTestBackup() coredb.Backup {
	return coredb.Backup{
		ID:        "IT-D002-D",
		Store:     "test-datastore",
		Namespace: "IT",
		Target: coredb.Target{
			Name:   "IT-D002 - D",
			Type:   coredb.TargetTypeFilesystem,
			Access: coredb.FilesystemAccessAgent,
			AgentHost: coredb.AgentHost{
				Name: "IT-D002",
			},
		},
	}
}
