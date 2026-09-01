//go:build linux

package tasklog

import "testing"

// A dead-pid queued orphan left by a crashed process must leave tasks/active
// on the next task-list reconciliation and land in the archive.
func TestRestartOrphanArchivedOnReconcile(t *testing.T) {
	setupTaskDirs(t)
	upid := fabricateDeadQueuedOrphan(t, nil)

	if _, err := ListTasks(true); err != nil {
		t.Fatal(err)
	}
	if listFileHas(t, activeTasks, upid) {
		t.Fatal("dead-pid queued orphan still active after reconcile")
	}
	if !listFileHas(t, archivePath, upid) {
		t.Fatal("dead-pid queued orphan not archived")
	}
}
