//go:build linux

package tasks

import (
	"fmt"
	"math/rand/v2"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/store/database"
	"github.com/pbs-plus/pbs-plus/internal/store/proxmox"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

// QueuedTask represents a task in queued state before execution starts.
type QueuedTask struct {
	proxmox.Task
	mu       sync.Mutex
	closed   atomic.Bool
	path     string
	job      any // either database.Backup or database.Restore
	isBackup bool
}

// Lock locks the task mutex.
func (t *QueuedTask) Lock() { t.mu.Lock() }

// Unlock unlocks the task mutex.
func (t *QueuedTask) Unlock() { t.mu.Unlock() }

// GenerateBackupQueuedTask creates a queued task for backup jobs.
func GenerateBackupQueuedTask(job database.Backup, web bool) (QueuedTask, error) {
	return generateQueuedTask(job, job.Target.GetHostname(), "backup", web, true)
}

// GenerateRestoreQueuedTask creates a queued task for restore jobs.
func GenerateRestoreQueuedTask(job database.Restore, web bool) (QueuedTask, error) {
	return generateQueuedTask(job, job.DestTarget.GetHostname(), "reader", web, false)
}

// generateQueuedTask creates a queued task with common setup logic.
func generateQueuedTask(job any, target, wtype string, web, isBackup bool) (QueuedTask, error) {
	var store string
	if isBackup {
		store = job.(database.Backup).Store
	} else {
		store = job.(database.Restore).Store
	}

	wid := fmt.Sprintf("%s%shost-%s", proxmox.EncodeToHexEscapes(store), proxmox.EncodeToHexEscapes(":"), proxmox.EncodeToHexEscapes(target))
	startTime := now()
	startTimeHex := fmt.Sprintf("%08X", uint32(startTime.Unix()))

	task := proxmox.Task{
		Node:       "pbsplusgen-queue",
		PID:        os.Getpid(),
		PStart:     proxmox.GetPStart(),
		StartTime:  startTime.Unix(),
		WorkerType: wtype,
		WID:        wid,
		User:       proxmox.AUTH_ID,
	}

	pid := fmt.Sprintf("%08X", task.PID)
	pstart := fmt.Sprintf("%08X", task.PStart)
	taskID := fmt.Sprintf("%08X", rand.Uint32())
	task.UPID = fmt.Sprintf("UPID:%s:%s:%s:%s:%s:%s:%s:%s:", task.Node, pid, pstart, taskID, startTimeHex, wtype, wid, proxmox.AUTH_ID)

	file, path, err := createTaskLogFile(task.UPID)
	if err != nil {
		return QueuedTask{}, err
	}

	source := "web UI"
	if !web {
		source = "schedule"
	}
	timestamp := now().Format(time.RFC3339)
	fmt.Fprintf(file, "%s: TASK QUEUED: job started from %s\n", timestamp, source)
	file.Close()

	task.Status = "running"
	return QueuedTask{Task: task, path: path, job: job, isBackup: isBackup}, nil
}

// UpdateDescription updates the queued task status description.
func (t *QueuedTask) UpdateDescription(desc string) error {
	if t.closed.Load() {
		return nil
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	// Re-create with Truncate to overwrite
	file, err := os.OpenFile(t.path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	timestamp := now().Format(time.RFC3339)
	if _, err := fmt.Fprintf(file, "%s: TASK QUEUED: %s\n", timestamp, desc); err != nil {
		return fmt.Errorf("failed to write status line: %w", err)
	}

	if t.isBackup {
		syslog.L.Info().WithJob(t.job.(database.Backup).ID).WithMessage(desc).Write()
	} else {
		syslog.L.Info().WithJob(t.job.(database.Restore).ID).WithMessage(desc).Write()
	}
	return nil
}

// Close removes the queued task file.
func (t *QueuedTask) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()
	_ = os.Remove(t.path)
	t.closed.Store(true)
}
