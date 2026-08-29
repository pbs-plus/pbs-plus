//go:build linux

package snapshotmount

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/pbs-plus/pxar/datastore"
	"golang.org/x/sys/unix"
)

func TestBackupLockPathMatchesPBS(t *testing.T) {
	original := datastoreLocksDir
	datastoreLocksDir = "/run/proxmox-backup/locks"
	defer func() { datastoreLocksDir = original }()

	tests := []struct {
		namespace string
		path      string
		want      string
	}{
		{path: "vm/100", want: "/run/proxmox-backup/locks/store/vm-100"},
		{
			namespace: "one/two",
			path:      "host/node-a/2026-08-28T17:00:00Z",
			want:      `/run/proxmox-backup/locks/store/one:two/host-node\x2da-2026\x2d08\x2d28T17\x3a00\x3a00Z`,
		},
	}
	for _, test := range tests {
		if got := backupLockPath("store", test.namespace, test.path); got != test.want {
			t.Fatalf("backupLockPath(%q, %q) = %q, want %q", test.namespace, test.path, got, test.want)
		}
	}
}

func TestChunkStoreLockReferenceCount(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, ".lock"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	releaseOne, err := acquireChunkStoreLock(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	releaseTwo, err := acquireChunkStoreLock(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	if chunkLockProbe(t, filepath.Join(root, ".lock")) {
		t.Fatal("exclusive lock succeeded while shared locks were held")
	}
	if err := releaseOne(); err != nil {
		t.Fatal(err)
	}
	if chunkLockProbe(t, filepath.Join(root, ".lock")) {
		t.Fatal("exclusive lock succeeded before the final shared release")
	}
	if err := releaseTwo(); err != nil {
		t.Fatal(err)
	}
	if !chunkLockProbe(t, filepath.Join(root, ".lock")) {
		t.Fatal("exclusive lock failed after all shared locks were released")
	}
}

func chunkLockProbe(t *testing.T, path string) bool {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestChunkStoreLockProbe$")
	cmd.Env = append(os.Environ(), "PBS_PLUS_CHUNK_LOCK_PROBE="+path)
	return cmd.Run() == nil
}

func TestChunkStoreLockProbe(t *testing.T) {
	path := os.Getenv("PBS_PLUS_CHUNK_LOCK_PROBE")
	if path == "" {
		return
	}
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		os.Exit(2)
	}
	lock := unix.Flock_t{Type: unix.F_WRLCK}
	if unix.FcntlFlock(file.Fd(), unix.F_SETLK, &lock) != nil {
		os.Exit(2)
	}
}

func TestComposePublicationCleansUnfinishedSnapshot(t *testing.T) {
	root := t.TempDir()
	locks := t.TempDir()
	active := t.TempDir()
	originalLocks, originalActive := datastoreLocksDir, activeOperationsDir
	datastoreLocksDir, activeOperationsDir = locks, active
	defer func() { datastoreLocksDir, activeOperationsDir = originalLocks, originalActive }()

	if err := os.Mkdir(filepath.Join(root, ".chunks"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, ".lock"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	sourceTime := time.Date(2026, 8, 28, 17, 0, 0, 0, time.UTC)
	sourceDir := filepath.Join(root, "host", "source", DirTime(sourceTime))
	if err := os.MkdirAll(sourceDir, 0o755); err != nil {
		t.Fatal(err)
	}

	publication, err := beginComposePublication(
		context.Background(), "store", root,
		"", "host", "source", sourceTime,
		"target", "host", "composed", "pbs-plus@pbs!local",
	)
	if err != nil {
		t.Fatal(err)
	}
	snapshotDir := publication.snapshotDir
	groupDir := publication.groupDir
	if _, err := os.Stat(filepath.Join(groupDir, "owner")); err != nil {
		t.Fatal(err)
	}
	if err := publication.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(snapshotDir); !os.IsNotExist(err) {
		t.Fatal("unfinished snapshot directory remains")
	}
	if _, err := os.Stat(groupDir); !os.IsNotExist(err) {
		t.Fatal("new empty target group remains")
	}
	data, err := os.ReadFile(filepath.Join(active, "store"))
	if err != nil {
		t.Fatal(err)
	}
	var operations []taskOperations
	if err := json.Unmarshal(data, &operations); err != nil {
		t.Fatal(err)
	}
	if len(operations) != 1 || operations[0].ActiveOperations.Write != 0 {
		t.Fatalf("unexpected active operations: %+v", operations)
	}
}

func TestVerifyComposeSource(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "source.ppxar.didx")
	index := datastore.NewDynamicIndexWriter(0)
	digest := sha256.Sum256([]byte("payload"))
	index.Add(7, digest)
	raw, err := index.Finish()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	csum := index.Csum()
	manifest := &datastore.Manifest{
		BackupType: "host",
		BackupID:   "source",
		BackupTime: 1787936400,
		Files: []datastore.BackupFileInfo{{
			Filename:  filepath.Base(path),
			CryptMode: string(datastore.CryptModeNone),
			CSum:      hex.EncodeToString(csum[:]),
			Size:      7,
		}},
	}
	manifestJSON, err := manifest.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	blob, err := datastore.EncodeBlob(nil, manifestJSON)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "index.json.blob"), blob, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := verifyComposeSource(dir, "host", "source", 1787936400, path); err != nil {
		t.Fatal(err)
	}
	raw[len(raw)-1]++
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := verifyComposeSource(dir, "host", "source", 1787936400, path); err == nil {
		t.Fatal("corrupt source index was accepted")
	}
}
