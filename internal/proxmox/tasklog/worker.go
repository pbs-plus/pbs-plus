//go:build linux

package tasklog

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"log/slog"
	"github.com/pbs-plus/pbs-plus/internal/proxmox"
)

type WorkerTask struct {
	Task   proxmox.Task
	mu     sync.Mutex
	closed atomic.Bool
	file   *os.File
	abort  atomic.Bool
}

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

	if err := AddActive(task.UPID); err != nil {
		slog.Error("tasklog: add active", "error", err, "upid", task.UPID)
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

func (w *WorkerTask) CloseWithStatus(state TaskState) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed.Load() {
		return
	}

	w.writeLogLine(state.ResultText())
	if err := WriteArchive(w.Task.UPID, state); err != nil {
		slog.Error("tasklog: archive error", "error", err, "upid", w.Task.UPID)
	}

	if err := RemoveActive(w.Task.UPID); err != nil {
		slog.Error(err.Error())
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

func (w *WorkerTask) writeLogLine(format string, args ...any) {
	timestamp := time.Now().Format(time.RFC3339)
	line := fmt.Sprintf("%s: "+format+"\n", append([]any{timestamp}, args...)...)
	if _, err := w.file.WriteString(line); err != nil {
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
