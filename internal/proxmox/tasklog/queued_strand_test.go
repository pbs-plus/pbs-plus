//go:build linux

package tasklog

import (
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
)

func TestQueuedTaskCloseRetiresWorkerWhenActiveRewriteFails(t *testing.T) {
	setupTaskDirs(t)

	queued, err := NewQueuedTask("backup", "store%3Ahost-strand", false)
	if err != nil {
		t.Fatal(err)
	}
	upid := queued.UPID()

	orig := writeTaskList
	writeTaskList = func(string, string, os.FileMode) error {
		return errors.New("task list is read-only")
	}
	queued.Close()
	writeTaskList = orig

	if workerIsActiveLocal(queued.Task) {
		t.Fatal("queued worker still reports active after close; the job would show running forever")
	}
	if state := QueuedState(upid); state != "" {
		t.Fatalf("QueuedState = %q, want empty after close", state)
	}
	path, err := UPIDLogPath(upid)
	if err != nil {
		t.Fatal(err)
	}
	logged, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("stranded queued log must survive for the archive: %v", err)
	}
	if !strings.Contains(string(logged), "TASK OK") {
		t.Fatalf("queued log = %q, want a result line", logged)
	}

	if err := Reconcile(""); err != nil {
		t.Fatal(err)
	}
	active, err := readTaskFile(activeTasks)
	if err != nil {
		t.Fatal(err)
	}
	for _, info := range active {
		if info.UPID == upid {
			t.Fatal("stranded queued task is still listed active after reconcile")
		}
	}
	archived, err := readTaskFileAny(archivePath)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, info := range archived {
		if info.UPID == upid {
			found = true
			if info.State == nil || info.State.Status != StatusOK {
				t.Fatalf("archived state = %+v, want ok", info.State)
			}
		}
	}
	if !found {
		t.Fatal("stranded queued task was never archived")
	}
}

func TestLockTaskListServesEveryConcurrentCaller(t *testing.T) {
	setupTaskDirs(t)

	const callers = 48
	var start sync.WaitGroup
	start.Add(1)
	var wg sync.WaitGroup
	errs := make([]error, callers)
	for i := range callers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			start.Wait()
			if i%3 == 0 {
				_, errs[i] = ListTasks(true)
				return
			}
			errs[i] = Reconcile("")
		}(i)
	}
	start.Done()
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("caller %d failed to take the task list lock: %v", i, err)
		}
	}
}
