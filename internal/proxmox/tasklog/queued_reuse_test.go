//go:build linux

package tasklog

import (
	"errors"
	"testing"
)

// NewQueuedTask must hand back the live task for the same worker so retries
// keep one UPID and log, and mint a fresh task only after Close.
func TestNewQueuedTaskReusesLiveTask(t *testing.T) {
	setupTaskDirs(t)

	first, err := NewQueuedTask("backup", "job-1", true)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewQueuedTask("backup", "job-1", false)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("second NewQueuedTask created %p, want reuse of %p", second, first)
	}
	other, err := NewQueuedTask("backup", "job-2", true)
	if err != nil {
		t.Fatal(err)
	}
	if other == first {
		t.Fatal("different worker reused the same task")
	}
	other.Close()

	first.Close()
	fresh, err := NewQueuedTask("backup", "job-1", true)
	if err != nil {
		t.Fatal(err)
	}
	if fresh == first {
		t.Fatal("closed task reused")
	}
	if got := QueuedState(first.UPID()); got != "" {
		t.Fatalf("state after close = %q, want cleared", got)
	}
	fresh.Close()
}

func TestNewQueuedTaskFreshAfterCancelClose(t *testing.T) {
	setupTaskDirs(t)

	minted, err := NewQueuedTask("backup", "job-9", true)
	if err != nil {
		t.Fatal(err)
	}
	upid := minted.UPID()
	minted.CloseErr(errors.New("operation canceled"))

	fresh, err := NewQueuedTask("backup", "job-9", true)
	if err != nil {
		t.Fatal(err)
	}
	if fresh == minted {
		t.Fatal("canceled-closed task reused")
	}
	if fresh.UPID() == upid {
		t.Fatalf("fresh task kept canceled UPID %q", upid)
	}
	fresh.Close()
}
