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
			syslog.L.Error(cerr).Write()
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
		syslog.L.Error(err).Write()
	}
	t.closed.Store(true)
}

func WriteQueuedLog(node, workerType, wid string, description string) (*QueuedTask, error) {
	task := NewTask(node, workerType, wid)
	task.Status = "running"

	file, path, err := CreateTaskLogFile(task.UPID)
	if err != nil {
		return nil, err
	}

	wt := &WorkerTask{Task: task, file: file}
	wt.LogString("TASK QUEUED: " + description)
	if err := file.Close(); err != nil {
		syslog.L.Error(err).Write()
	}
	wt.closed.Store(true)

	return &QueuedTask{Task: task, path: path}, nil
}
