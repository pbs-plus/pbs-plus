//go:build linux

package tasklog

import (
	"fmt"
	"os"

	"log/slog"
)

type QueuedTask struct {
	*WorkerTask
}

func SourceString(web bool) string {
	if web {
		return "web UI"
	}
	return "schedule"
}

// NewQueuedTask creates a transient active PBS task while work is waiting to start.
func NewQueuedTask(workerType, wid string, web bool) (*QueuedTask, error) {
	worker, err := NewWorkerTask("pbsplusgen-queue", workerType, wid)
	if err != nil {
		return nil, err
	}
	worker.LogString(fmt.Sprintf("TASK QUEUED: job started from %s", SourceString(web)))
	return &QueuedTask{WorkerTask: worker}, nil
}

// Close removes a queued task without archiving it as job history.
func (t *QueuedTask) Close() {
	if t == nil {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed.Load() {
		return
	}

	lock, err := lockTaskList(true)
	if err != nil {
		slog.Error("tasklog: lock queued task removal", "error", err, "upid", t.UPID())
		return
	}
	defer lock.Close()

	active, err := readTaskFile(activeTasks)
	if err != nil {
		slog.Error("tasklog: read active tasks for queued task removal", "error", err, "upid", t.UPID())
		return
	}
	kept := active[:0]
	for _, info := range active {
		if info.UPID != t.UPID() {
			kept = append(kept, info)
		}
	}
	if err := replaceFile(activeTasks, renderTaskList(kept), 0660); err != nil {
		slog.Error("tasklog: remove queued task from active tasks", "error", err, "upid", t.UPID())
		return
	}

	unregisterWorker(t.Task.TaskId)
	t.close()

	path, err := UPIDLogPath(t.UPID())
	if err != nil {
		slog.Error("tasklog: resolve queued task log", "error", err, "upid", t.UPID())
		return
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		slog.Error("tasklog: remove queued task log", "error", err, "upid", t.UPID())
	}
}
