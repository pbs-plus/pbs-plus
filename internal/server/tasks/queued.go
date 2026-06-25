//go:build linux

package tasks

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/proxmox"
	"github.com/pbs-plus/pbs-plus/internal/proxmox/tasklog"
	"github.com/pbs-plus/pbs-plus/internal/server/database"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

type QueuedTask struct {
	proxmox.Task
	mu       sync.Mutex
	closed   atomic.Bool
	path     string
	job      any
	isBackup bool
}

func (t *QueuedTask) Lock() { t.mu.Lock() }

func (t *QueuedTask) Unlock() { t.mu.Unlock() }

func GenerateBackupQueuedTask(job database.Backup, web bool) (QueuedTask, error) {
	return generateQueuedTask(job, job.Target.GetHostname(), "backup", web, true)
}

func GenerateRestoreQueuedTask(job database.Restore, web bool) (QueuedTask, error) {
	return generateQueuedTask(job, job.DestTarget.GetHostname(), "reader", web, false)
}

func GenerateVerificationQueuedTask(job database.VerificationJob, web bool) (QueuedTask, error) {
	return generateQueuedTask(job, job.ID, "verification", web, false)
}

func generateQueuedTask(job any, target, wtype string, web, isBackup bool) (QueuedTask, error) {
	var store string
	switch j := job.(type) {
	case database.Backup:
		store = j.Store
	case database.Restore:
		store = j.Store
	case database.VerificationJob:
		store = j.Store
	default:
		store = "unknown"
	}

	wid := fmt.Sprintf("%s%shost-%s", proxmox.EncodeToHexEscapes(store), proxmox.EncodeToHexEscapes(":"), proxmox.EncodeToHexEscapes(target))

	source := "web UI"
	if !web {
		source = "schedule"
	}
	desc := fmt.Sprintf("job started from %s", source)

	task, path, err := tasklog.WriteQueuedLog("pbsplusgen-queue", wtype, wid, desc)
	if err != nil {
		return QueuedTask{}, err
	}

	return QueuedTask{Task: task, path: path, job: job, isBackup: isBackup}, nil
}

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

	if t.isBackup {
		syslog.L.Info().WithJob(t.job.(database.Backup).ID).WithMessage(desc).Write()
	} else {
		syslog.L.Info().WithJob(t.job.(database.Restore).ID).WithMessage(desc).Write()
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

func GenerateMtfQueuedTask(jobID, datastore string, web bool) (QueuedTask, error) {
	wid := proxmox.EncodeToHexEscapes(datastore) +
		proxmox.EncodeToHexEscapes(":") +
		"mtf-" + proxmox.EncodeToHexEscapes(jobID)

	source := "web UI"
	if !web {
		source = "schedule"
	}
	desc := fmt.Sprintf("MTF job started from %s", source)

	task, path, err := tasklog.WriteQueuedLog("pbsplusgen-queue", "mtf2pxar", wid, desc)
	if err != nil {
		return QueuedTask{}, err
	}

	return QueuedTask{Task: task, path: path}, nil
}
