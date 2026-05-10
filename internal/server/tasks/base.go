//go:build linux

package tasks

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/conf"
	"github.com/pbs-plus/pbs-plus/internal/server/proxmox"
)

// BaseTask contains shared fields and operations for all task types.
// Embed this in task-specific structs to avoid duplicating common file-handling logic.
type BaseTask struct {
	proxmox.Task
	mu     sync.Mutex
	closed atomic.Bool
	file   *os.File
}

// NewBaseTask creates a BaseTask with the given task and file.
func NewBaseTask(task proxmox.Task, file *os.File) BaseTask {
	return BaseTask{Task: task, file: file}
}

// Close closes the task log file and marks the task as closed.
// Must be called while holding the mutex.
func (t *BaseTask) Close() {
	if t.file != nil {
		t.file.Close()
		t.file = nil
	}
	t.closed.Store(true)
}

// WriteLogLine writes a timestamped line to the task file.
// Must be called while holding the mutex and with an open t.file.
func (t *BaseTask) WriteLogLine(format string, args ...any) bool {
	timestamp := time.Now().Format(time.RFC3339)
	line := fmt.Sprintf("%s: "+format+"\n", append([]any{timestamp}, args...)...)
	if _, err := t.file.WriteString(line); err != nil {
		return false
	}
	return true
}

// CloseWithStatus writes a final TASK status line, archives it, and closes the log file.
// The onClose callback runs while the mutex is held but before the file is closed.
// The afterUnlock callback runs after the mutex is released.
func (t *BaseTask) CloseWithStatus(status string, onClose func(), afterUnlock func()) {
	t.mu.Lock()

	if !t.WriteLogLine("TASK %s", status) {
		if onClose != nil {
			onClose()
		}
		t.Close()
		t.mu.Unlock()
		return
	}
	WriteArchive(t.UPID, t.StartTime, status)

	if onClose != nil {
		onClose()
	}
	t.Close()
	t.mu.Unlock()

	if afterUnlock != nil {
		afterUnlock()
	}
}

// WriteArchive writes an entry to the global task archive.
// The format is "UPID StartTime Status\n"
func WriteArchive(upid string, startTime int64, status string) bool {
	archive, err := os.OpenFile(filepath.Join(conf.TaskLogsBasePath, "archive"), os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return false
	}
	defer archive.Close()

	startTimeHex := fmt.Sprintf("%08X", uint32(startTime))
	archiveLine := fmt.Sprintf("%s %s %s\n", upid, startTimeHex, status)
	if _, err := archive.WriteString(archiveLine); err != nil {
		return false
	}
	return true
}

// CreateTaskLogFile creates a task log file and returns the open file handle and path.
// It handles directory creation, ownership, and file creation atomically.
func CreateTaskLogFile(upid string) (*os.File, string, error) {
	path, err := proxmox.GetLogPath(upid)
	if err != nil {
		return nil, "", err
	}

	dir := filepath.Dir(path)
	_ = os.MkdirAll(dir, 0755)
	_ = os.Chown(dir, 34, 34)

	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return nil, "", err
	}

	if err := file.Chown(34, 34); err != nil {
		file.Close()
		return nil, "", err
	}

	return file, path, nil
}

// Now returns the current time.
func Now() time.Time { return time.Now() }

// WriteString writes a timestamped log line with mutex protection and file sync.
func (t *BaseTask) WriteString(data string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed.Load() {
		return
	}
	t.WriteLogLine("%s", data)
	t.file.Sync()
}
