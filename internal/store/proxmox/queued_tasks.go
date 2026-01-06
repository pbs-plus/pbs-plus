//go:build linux

package proxmox

import (
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/store/types"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

type QueuedTask struct {
	Task
	sync.Mutex
	closed  atomic.Bool
	path    string
	backup  types.Backup
	restore types.Restore
}

func GenerateBackupQueuedTask(job types.Backup, web bool) (QueuedTask, error) {
	targetName := strings.TrimSpace(strings.Split(job.Target, " - ")[0])
	wid := fmt.Sprintf("%s%shost-%s", encodeToHexEscapes(job.Store), encodeToHexEscapes(":"), encodeToHexEscapes(targetName))
	startTime := fmt.Sprintf("%08X", uint32(time.Now().Unix()))

	wtype := "backup"
	node := "pbsplusgen-queue"

	task := Task{
		Node:       node,
		PID:        os.Getpid(),
		PStart:     getPStart(),
		StartTime:  time.Now().Unix(),
		WorkerType: wtype,
		WID:        wid,
		User:       AUTH_ID,
	}

	pid := fmt.Sprintf("%08X", task.PID)
	pstart := fmt.Sprintf("%08X", task.PStart)
	taskID := fmt.Sprintf("%08X", rand.Uint32())

	upid := fmt.Sprintf("UPID:%s:%s:%s:%s:%s:%s:%s:%s:", node, pid, pstart, taskID, startTime, wtype, wid, AUTH_ID)

	task.UPID = upid

	path, err := GetLogPath(upid)
	if err != nil {
		return QueuedTask{}, err
	}

	_ = os.MkdirAll(filepath.Dir(path), 0755)
	_ = os.Chown(filepath.Dir(path), 34, 34)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return QueuedTask{}, err
	}

	err = file.Chown(34, 34)
	if err != nil {
		return QueuedTask{}, err
	}
	defer file.Close()

	timestamp := time.Now().Format(time.RFC3339)
	statusLine := fmt.Sprintf("%s: TASK QUEUED: ", timestamp)
	if web {
		statusLine += "job started from web UI\n"
	} else {
		statusLine += "job started from schedule\n"
	}

	if _, err := file.WriteString(statusLine); err != nil {
		return QueuedTask{}, fmt.Errorf("failed to write status line: %w", err)
	}

	task.Status = "running"

	return QueuedTask{Task: task, backup: job, path: path}, nil
}

func GenerateRestoreQueuedTask(job types.Restore, web bool) (QueuedTask, error) {
	targetName := strings.TrimSpace(strings.Split(job.DestTarget, " - ")[0])
	wid := fmt.Sprintf("%s%shost-%s", encodeToHexEscapes(job.Store), encodeToHexEscapes(":"), encodeToHexEscapes(targetName))
	startTime := fmt.Sprintf("%08X", uint32(time.Now().Unix()))

	wtype := "read"
	node := "pbsplusgen-queue"

	task := Task{
		Node:       node,
		PID:        os.Getpid(),
		PStart:     getPStart(),
		StartTime:  time.Now().Unix(),
		WorkerType: wtype,
		WID:        wid,
		User:       AUTH_ID,
	}

	pid := fmt.Sprintf("%08X", task.PID)
	pstart := fmt.Sprintf("%08X", task.PStart)
	taskID := fmt.Sprintf("%08X", rand.Uint32())

	upid := fmt.Sprintf("UPID:%s:%s:%s:%s:%s:%s:%s:%s:", node, pid, pstart, taskID, startTime, wtype, wid, AUTH_ID)

	task.UPID = upid

	path, err := GetLogPath(upid)
	if err != nil {
		return QueuedTask{}, err
	}

	_ = os.MkdirAll(filepath.Dir(path), 0755)
	_ = os.Chown(filepath.Dir(path), 34, 34)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return QueuedTask{}, err
	}

	err = file.Chown(34, 34)
	if err != nil {
		return QueuedTask{}, err
	}
	defer file.Close()

	timestamp := time.Now().Format(time.RFC3339)
	statusLine := fmt.Sprintf("%s: TASK QUEUED: ", timestamp)
	if web {
		statusLine += "job started from web UI\n"
	} else {
		statusLine += "job started from schedule\n"
	}

	if _, err := file.WriteString(statusLine); err != nil {
		return QueuedTask{}, fmt.Errorf("failed to write status line: %w", err)
	}

	task.Status = "running"

	return QueuedTask{Task: task, restore: job, path: path}, nil
}

func (task *QueuedTask) UpdateDescription(desc string) error {
	if task.closed.Load() {
		return nil
	}

	task.Lock()
	defer task.Unlock()

	file, err := os.OpenFile(task.path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	timestamp := time.Now().Format(time.RFC3339)
	statusLine := fmt.Sprintf("%s: TASK QUEUED: ", timestamp)
	statusLine += desc + "\n"

	if _, err := file.WriteString(statusLine); err != nil {
		return fmt.Errorf("failed to write status line: %w", err)
	}

	syslog.L.Info().WithJob(task.backup.ID).WithMessage(desc).Write()

	return nil
}

func (task *QueuedTask) Close() {
	task.Lock()
	defer task.Unlock()

	_ = os.Remove(task.path)
	task.closed.Store(true)
}
