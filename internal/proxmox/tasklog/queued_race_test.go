//go:build linux

package tasklog

import (
	"sync"
	"testing"
)

// Concurrent mint attempts for one worker (submit-time PrepareQueue racing
// the dispatched workflow) must resolve to a single task; losing mints are
// torn down instead of staying in tasks/active forever.
func TestNewQueuedTaskConcurrentMintSingleTask(t *testing.T) {
	setupTaskDirs(t)

	const n = 8
	tasks := make([]*QueuedTask, n)
	var start sync.WaitGroup
	start.Add(1)
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			start.Wait()
			q, err := NewQueuedTask("backup", "store%3Ahost-race", false)
			if err != nil {
				t.Errorf("NewQueuedTask: %v", err)
				return
			}
			tasks[i] = q
		}(i)
	}
	start.Done()
	wg.Wait()

	winner := tasks[0]
	for _, q := range tasks[1:] {
		if q != winner {
			t.Fatalf("concurrent mints produced distinct tasks; orphaned entries remain in tasks/active")
		}
	}
	winner.Close()

	list, err := readTaskFile(activeTasks)
	if err != nil {
		t.Fatal(err)
	}
	for _, info := range list {
		if IsQueuedUPID(info.UPID) {
			t.Fatalf("queued entry %s still active after winner closed", info.UPID)
		}
	}
}
