//go:build linux

package tasklog

import (
	"fmt"
	"os"
	"strings"
	"sync"

	"log/slog"
)

type QueuedTask struct {
	*WorkerTask
}

var queuedStates sync.Map

// QueuedState returns the job status text for a live queued task UPID.
func QueuedState(upid string) string {
	if v, ok := queuedStates.Load(upid); ok {
		return v.(string)
	}
	return ""
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
	queuedStates.Store(worker.UPID(), fmt.Sprintf("QUEUED: job started from %s", SourceString(web)))
	return &QueuedTask{WorkerTask: worker}, nil
}

// LogString records task output and marks an untouched queued task as running.
func (t *QueuedTask) LogString(data string) {
	if t == nil {
		return
	}
	t.WorkerTask.LogString(data)
	if state, ok := queuedStates.Load(t.UPID()); ok && strings.HasPrefix(state.(string), "QUEUED:") {
		queuedStates.CompareAndSwap(t.UPID(), state, "RUNNING: task output")
	}
}

// SetState updates the live state shown for a transient task and records it in the task log.
func (t *QueuedTask) SetState(state string) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed.Load() {
		return
	}
	t.writeLogLine("%s", state)
	if err := t.file.Sync(); err != nil {
		slog.Error(err.Error())
	}
	queuedStates.Store(t.UPID(), state)
}

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
	queuedStates.Delete(t.UPID())

	path, err := UPIDLogPath(t.UPID())
	if err != nil {
		slog.Error("tasklog: resolve queued task log", "error", err, "upid", t.UPID())
		return
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		slog.Error("tasklog: remove queued task log", "error", err, "upid", t.UPID())
	}
}
