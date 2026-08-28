//go:build linux

package tasklog

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/proxmox"
	"log/slog"
)

type WorkerTask struct {
	Task   proxmox.Task
	mu     sync.Mutex
	closed atomic.Bool
	file   *os.File
	abort  atomic.Bool
}

// NewWorkerTask is PBS's WorkerTask::new: allocate a UPID, create its log
// file, register the worker in the process registry, then publish it to
// tasks/active via Reconcile.
func NewWorkerTask(node, workerType, wid string) (*WorkerTask, error) {
	task := NewTask(node, workerType, wid)

	file, _, err := CreateTaskLogFile(task.UPID)
	if err != nil {
		return nil, fmt.Errorf("tasklog: create log file: %w", err)
	}

	wt := &WorkerTask{
		Task: task,
		file: file,
	}
	registerWorker(wt)
	startControlSocket()

	if err := Reconcile(task.UPID); err != nil {
		unregisterWorker(task.TaskId)
		wt.close()
		return nil, fmt.Errorf("tasklog: register active task: %w", err)
	}

	return wt, nil
}

func (w *WorkerTask) UPID() string {
	return w.Task.UPID
}

func (w *WorkerTask) Log(format string, args ...any) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed.Load() {
		return
	}
	w.writeLogLine(format, args...)
}

func (w *WorkerTask) LogString(data string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed.Load() {
		return
	}
	w.writeLogLine("%s", data)
	if err := w.file.Sync(); err != nil {
		slog.Error(err.Error())
	}
}

// CloseWithStatus is PBS's log_result: append the result line to the log
// unfiltered, unregister the worker, then Reconcile so the finished entry
// lands in the archive and leaves tasks/active.
func (w *WorkerTask) CloseWithStatus(state TaskState) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed.Load() {
		return
	}

	w.writeLogLine("%s", state.ResultText())
	if err := w.file.Sync(); err != nil {
		slog.Error(err.Error())
	}

	unregisterWorker(w.Task.TaskId)
	if err := Reconcile(""); err != nil {
		slog.Error("tasklog: reconcile after close", "error", err, "upid", w.Task.UPID)
	}

	w.close()
}

func (w *WorkerTask) CloseOK() {
	w.CloseWithStatus(TaskState{Status: StatusOK, EndTime: time.Now().Unix()})
}

func (w *WorkerTask) CloseErr(err error) {
	w.CloseWithStatus(TaskState{Status: StatusError, EndTime: time.Now().Unix(), Message: err.Error()})
}

func (w *WorkerTask) CloseWarn(count uint64) {
	w.CloseWithStatus(TaskState{Status: StatusWarning, EndTime: time.Now().Unix(), WarnCount: count})
}

func (w *WorkerTask) RequestAbort() {
	w.abort.Store(true)
}

func (w *WorkerTask) AbortRequested() bool {
	return w.abort.Load()
}

func CreateState(result error, warnCount uint64) TaskState {
	endtime := time.Now().Unix()
	if result != nil {
		return TaskState{Status: StatusError, EndTime: endtime, Message: result.Error()}
	}
	if warnCount > 0 {
		return TaskState{Status: StatusWarning, EndTime: endtime, WarnCount: warnCount}
	}
	return TaskState{Status: StatusOK, EndTime: endtime}
}

// ReopenWorkerTask reattaches to an existing task log by UPID so a
// restarted process can continue appending to and closing a task it
// did not create in memory. The worker is re-registered and re-published
// as active, mirroring how the owning process would see it.
func ReopenWorkerTask(upid string) (*WorkerTask, error) {
	parsed, err := proxmox.ParseUPID(upid)
	if err != nil {
		return nil, fmt.Errorf("tasklog: parse upid: %w", err)
	}
	if existing, ok := lookupWorker(parsed.TaskId); ok && existing.UPID() == upid && !existing.closed.Load() {
		return existing, nil
	}

	path, err := UPIDLogPath(upid)
	if err != nil {
		return nil, err
	}

	if err := os.MkdirAll(dirName(path), 0755); err != nil {
		return nil, fmt.Errorf("tasklog: ensure log dir: %w", err)
	}

	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("tasklog: open log file: %w", err)
	}

	wt := &WorkerTask{Task: parsed, file: file}
	registerWorker(wt)
	startControlSocket()

	if err := Reconcile(upid); err != nil {
		unregisterWorker(parsed.TaskId)
		wt.close()
		return nil, fmt.Errorf("tasklog: register active task: %w", err)
	}

	return wt, nil
}

func dirName(path string) string {
	for i := len(path) - 1; i >= 0; i-- {
		if path[i] == '/' {
			return path[:i]
		}
	}
	return "."
}

func (w *WorkerTask) writeLogLine(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	if _, err := w.file.WriteString(proxmox.FormatLogLine(time.Now(), msg) + "\n"); err != nil {
		slog.Error(err.Error())
	}
}

func (w *WorkerTask) close() {
	if w.file != nil {
		if err := w.file.Close(); err != nil {
			slog.Error(err.Error())
		}
		w.file = nil
	}
	w.closed.Store(true)
}
