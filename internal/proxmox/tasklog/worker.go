//go:build linux

package tasklog

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/proxmox"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

type WorkerTask struct {
	Task       proxmox.Task
	mu         sync.Mutex
	closed     atomic.Bool
	file       *os.File
	abort      atomic.Bool
	afterClose func()
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
		syslog.L.Error(err).WithField("upid", task.UPID).WithMessage("tasklog: add active").Write()
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
		syslog.L.Error(err).Write()
	}
}

func (w *WorkerTask) CloseWithStatus(state TaskState, afterClose func()) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed.Load() {
		return
	}

	w.writeLogLine(state.ResultText())
	if err := WriteArchive(w.Task.UPID, state); err != nil {
		syslog.L.Error(err).WithField("upid", w.Task.UPID).Write()
	}

	if afterClose != nil {
		afterClose()
	}

	w.close()
}

func (w *WorkerTask) CloseOK(afterClose func()) {
	w.CloseWithStatus(TaskState{Status: StatusOK, EndTime: time.Now().Unix()}, afterClose)
}

func (w *WorkerTask) CloseErr(err error, afterClose func()) {
	w.CloseWithStatus(TaskState{Status: StatusError, EndTime: time.Now().Unix(), Message: err.Error()}, afterClose)
}

func (w *WorkerTask) CloseWarn(count uint64, afterClose func()) {
	w.CloseWithStatus(TaskState{Status: StatusWarning, EndTime: time.Now().Unix(), WarnCount: count}, afterClose)
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
		syslog.L.Error(err).Write()
	}
}

func (w *WorkerTask) close() {
	if w.file != nil {
		if err := w.file.Close(); err != nil {
			syslog.L.Error(err).Write()
		}
		w.file = nil
	}
	w.closed.Store(true)
}

func WriteQueuedLog(node, workerType, wid string, description string) (proxmox.Task, string, error) {
	task := NewTask(node, workerType, wid)
	file, path, err := CreateTaskLogFile(task.UPID)
	if err != nil {
		return proxmox.Task{}, "", err
	}
	wt := &WorkerTask{Task: task, file: file}
	wt.LogString("TASK QUEUED: " + description)
	if err := file.Close(); err != nil {
		syslog.L.Error(err).Write()
	}
	wt.closed.Store(true)
	task.Status = "running"
	return task, path, nil
}
