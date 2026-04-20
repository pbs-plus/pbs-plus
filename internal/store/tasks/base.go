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
	"github.com/pbs-plus/pbs-plus/internal/store/proxmox"
)

// baseTask contains shared fields and operations for all task types.
// Embed this in task-specific structs (like RestoreTask, QueuedTask, etc.)
// to avoid duplicating common file-handling logic.
type baseTask struct {
	proxmox.Task
	sync.Mutex
	closed atomic.Bool
	file   *os.File
}

// close closes the task log file and marks the task as closed.
// Must be called while holding the mutex.
func (t *baseTask) close() {
	if t.file != nil {
		t.file.Close()
		t.file = nil
	}
	t.closed.Store(true)
}

// writeLogLine writes a timestamped line to the task file.
// Must be called while holding the mutex and with an open t.file.
func (t *baseTask) writeLogLine(format string, args ...any) bool {
	timestamp := time.Now().Format(time.RFC3339)
	line := fmt.Sprintf("%s: "+format+"\n", append([]any{timestamp}, args...)...)
	if _, err := t.file.WriteString(line); err != nil {
		return false
	}
	return true
}

// closeWithStatus closes the task with a status line and archives it.
// The beforeClose callback is called before closing, allowing the task to write
// additional data while the file is still open.
func (t *baseTask) closeWithStatus(status string, beforeClose func(), afterUnlock func()) {
	t.Lock()
	defer func() {
		if beforeClose != nil {
			beforeClose()
		}
		t.close()
		t.Unlock()
		if afterUnlock != nil {
			afterUnlock()
		}
	}()

	if !t.writeLogLine("TASK %s", status) {
		return
	}
	writeArchive(t.UPID, t.StartTime, status)
}

// writeArchive writes an entry to the global task archive.
// The format is "UPID StartTime Status\n"
func writeArchive(upid string, startTime int64, status string) bool {
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

// createTaskLogFile creates a task log file and returns the open file handle and path.
// It handles directory creation, ownership, and file creation atomically.
func createTaskLogFile(upid string) (*os.File, string, error) {
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

func now() time.Time { return time.Now() }
