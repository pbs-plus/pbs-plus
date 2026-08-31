//go:build linux

package tasklog

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pbs-plus/pbs-plus/internal/proxmox"
)

// fabricateDeadQueuedOrphan writes a pbsplusgen-queue task entry owned by a
// dead pid, with its log on disk, into the active list (state nil) or archive.
func fabricateDeadQueuedOrphan(t *testing.T, state *TaskState) string {
	t.Helper()
	upid := fmt.Sprintf("UPID:pbsplusgen-queue:%08X:%08X:%08X:%08X:%s:%s:root@pam:",
		4190212, 0x0BCDEF12, 0x12345678, 1700000000, "backup", "job-r")
	if _, err := proxmox.ParseUPID(upid); err != nil {
		t.Fatalf("fabricated upid does not parse: %v", err)
	}
	logPath, err := UPIDLogPath(upid)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(logPath), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(logPath, []byte("2026-08-31T18:00:00Z: QUEUED: job started from web UI\n"), 0660); err != nil {
		t.Fatal(err)
	}
	target := archivePath
	if state == nil {
		target = activeTasks
	}
	if err := os.WriteFile(target, []byte(RenderStatusLine(upid, state)), 0660); err != nil {
		t.Fatal(err)
	}
	return upid
}

func listFileHas(t *testing.T, path, upid string) bool {
	t.Helper()
	list, err := readTaskFileAny(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, info := range list {
		if info.UPID == upid {
			return true
		}
	}
	return false
}

// Dead-pid queued tasks are terminal per proxmox-backup: the new task is
// fresh, references the orphan for continuity, and leaves the orphan's list
// entries and log untouched for the archive to keep.
func TestNewQueuedTaskReferencesOrphanWithoutAdopting(t *testing.T) {
	setupTaskDirs(t)
	upid := fabricateDeadQueuedOrphan(t, nil)

	q, err := NewQueuedTask("backup", "job-r", true)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(q.Close)
	if q.UPID() == upid {
		t.Fatal("dead-pid queued upid was adopted")
	}
	if !listFileHas(t, activeTasks, upid) && !listFileHas(t, archivePath, upid) {
		t.Fatal("orphan entry vanished from the task lists")
	}
	logPath, err := UPIDLogPath(q.UPID())
	if err != nil {
		t.Fatal(err)
	}
	logBytes, err := os.ReadFile(logPath)
	if err != nil || !strings.Contains(string(logBytes), "RESUMED: continuing after server restart (previous queue task: "+upid) {
		t.Fatalf("new log = %q, %v; want resume pointer to orphan", logBytes, err)
	}
	orphanLog, err := UPIDLogPath(upid)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(orphanLog); err != nil {
		t.Fatalf("orphan log disturbed: %v", err)
	}
}

// A queued entry that finished with a real outcome is history, not an orphan.
func TestNewQueuedTaskIgnoresFinishedQueuedEntry(t *testing.T) {
	setupTaskDirs(t)
	upid := fabricateDeadQueuedOrphan(t, &TaskState{Status: StatusOK, EndTime: 1700000100})

	q, err := NewQueuedTask("backup", "job-r", true)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(q.Close)
	logPath, err := UPIDLogPath(q.UPID())
	if err != nil {
		t.Fatal(err)
	}
	logBytes, err := os.ReadFile(logPath)
	if err != nil || strings.Contains(string(logBytes), upid) {
		t.Fatalf("new log = %q, %v; must not reference finished entry", logBytes, err)
	}
}
