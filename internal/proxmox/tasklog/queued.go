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

func FormatWorkerID(store, prefix, identifier string) string {
	return proxmox.EncodeToHexEscapes(store) +
		proxmox.EncodeToHexEscapes(":") +
		prefix +
		proxmox.EncodeToHexEscapes(identifier)
}

func SourceString(web bool) string {
	if web {
		return "web UI"
	}
	return "schedule"
}

type QueuedTask struct {
	Task   proxmox.Task
	mu     sync.Mutex
	closed atomic.Bool
	path   string
}

func (t *QueuedTask) Lock() { t.mu.Lock() }

func (t *QueuedTask) Unlock() { t.mu.Unlock() }

func (t *QueuedTask) UpdateDescription(desc string) error {
	if t.closed.Load() {
		return nil
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	file, err := os.OpenFile(t.path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0660)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := file.Close(); cerr != nil {
			slog.Error(cerr.Error())
		}
	}()

	timestamp := time.Now().Format(time.RFC3339)
	if _, err := fmt.Fprintf(file, "%s: TASK QUEUED: %s\n", timestamp, desc); err != nil {
		return fmt.Errorf("failed to write status line: %w", err)
	}
	return nil
}

func (t *QueuedTask) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := os.Remove(t.path); err != nil && !os.IsNotExist(err) {
		slog.Error(err.Error())
	}
	t.closed.Store(true)
}

func WriteQueuedLog(node, workerType, wid string, web bool) (*QueuedTask, error) {
	task := NewTask(node, workerType, wid)
	task.Status = "running"

	file, path, err := CreateTaskLogFile(task.UPID)
	if err != nil {
		return nil, err
	}

	desc := fmt.Sprintf("job started from %s", SourceString(web))
	wt := &WorkerTask{Task: task, file: file}
	wt.LogString("TASK QUEUED: " + desc)
	if err := file.Close(); err != nil {
		slog.Error(err.Error())
	}
	wt.closed.Store(true)

	return &QueuedTask{Task: task, path: path}, nil
}
