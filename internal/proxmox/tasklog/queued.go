//go:build linux

package tasklog

import (
	"fmt"
	"os"
	"strings"
	"sync"

	"log/slog"

	"github.com/pbs-plus/pbs-plus/internal/proxmox"
)

type QueuedTask struct {
	*WorkerTask
	key string
}

var queuedStates sync.Map
var queuedTasks sync.Map

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

func NewQueuedTask(workerType, wid string, web bool) (*QueuedTask, error) {
	key := workerType + "\t" + wid
	for {
		if v, ok := queuedTasks.Load(key); ok {
			if q := v.(*QueuedTask); !q.closed.Load() {
				return q, nil
			}
			queuedTasks.CompareAndDelete(key, v)
		}
		worker, err := NewWorkerTask("pbsplusgen-queue", workerType, wid)
		if err != nil {
			return nil, err
		}
		t := &QueuedTask{WorkerTask: worker, key: key}
		worker.LogString(fmt.Sprintf("QUEUED: job started from %s", SourceString(web)))
		if upid, ok := previousQueuedOrphan(workerType, wid); ok {
			worker.LogString(fmt.Sprintf("RESUMED: continuing after server restart (previous queue task: %s)", upid))
		}
		queuedStates.Store(worker.UPID(), fmt.Sprintf("QUEUED: job started from %s", SourceString(web)))
		if actual, loaded := queuedTasks.LoadOrStore(key, t); loaded {
			q := actual.(*QueuedTask)
			t.Close()
			if q.closed.Load() {
				queuedTasks.CompareAndDelete(key, actual)
				continue
			}
			return q, nil
		}
		return t, nil
	}
}

// previousQueuedOrphan returns the newest dead queued placeholder for the
// worker, whether still listed active after a crash or already archived as
// unknown. Dead-pid UPIDs are terminal in proxmox-backup's eyes, so the
// orphan is never resurrected: it stays archived with its log, and the
// caller's fresh task just references it for continuity.
func previousQueuedOrphan(workerType, wid string) (string, bool) {
	lists := [][]TaskListInfo{}
	if active, err := readTaskFile(activeTasks); err == nil {
		lists = append(lists, active)
	}
	if archived, err := readTaskFileAny(archivePath); err == nil {
		lists = append(lists, archived)
	}
	var found proxmox.Task
	for _, list := range lists {
		for _, info := range list {
			if !IsQueuedUPID(info.UPID) || info.Task.WorkerType != workerType || info.Task.WID != wid {
				continue
			}
			if info.State != nil && info.State.Status != StatusUnknown {
				continue
			}
			if active, err := workerIsActive(info.Task); err != nil || active {
				continue
			}
			if found.UPID == "" || info.Task.StartTime > found.StartTime {
				found = info.Task
			}
		}
	}
	return found.UPID, found.UPID != ""
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
	queuedTasks.CompareAndDelete(t.key, t)
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
