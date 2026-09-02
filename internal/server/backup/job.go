//go:build linux

package backup

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/proxmox"
	"github.com/pbs-plus/pbs-plus/internal/proxmox/tasklog"
	"github.com/pbs-plus/pbs-plus/internal/server/application"
	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
	"github.com/pbs-plus/pbs-plus/internal/server/database"
	"github.com/pbs-plus/pbs-plus/internal/server/dovecot"
	"github.com/pbs-plus/pbs-plus/internal/server/jobs"
	"github.com/pbs-plus/pbs-plus/internal/server/rpc/mountrpc"
)

type backupStartLock struct {
	mu   sync.Mutex
	refs int
}

var backupStartLocks = struct {
	sync.Mutex
	locks map[string]*backupStartLock
}{locks: make(map[string]*backupStartLock)}

func lockBackupStart(workerID string) func() {
	backupStartLocks.Lock()
	lock := backupStartLocks.locks[workerID]
	if lock == nil {
		lock = &backupStartLock{}
		backupStartLocks.locks[workerID] = lock
	}
	lock.refs++
	backupStartLocks.Unlock()

	lock.mu.Lock()
	return func() {
		lock.mu.Unlock()
		backupStartLocks.Lock()
		lock.refs--
		if lock.refs == 0 {
			delete(backupStartLocks.locks, workerID)
		}
		backupStartLocks.Unlock()
	}
}

type backupJob struct {
	mu     sync.RWMutex
	cancel context.CancelFunc

	Task      proxmox.Task
	currOwner string
	waitGroup *sync.WaitGroup
	err       error

	logger *log.Logger

	job coredb.Backup

	app             *application.Runtime
	skipCheck       bool
	extraExclusions []string
	databaseAware   bool

	cleanupOnce sync.Once
	started     atomic.Bool

	agentMount    *mountrpc.AgentMount
	s3Mount       *mountrpc.S3Mount
	stagedDump    *database.StagedDump
	stagedDovecot *dovecot.StagedBackup
	srcPath       string
	cmd           *exec.Cmd
	upid          string
	workerID      string
	workflowStart int64
	executionID   string
	scriptTask    *tasklog.QueuedTask
}

func (b *backupJob) waitForCompletion(ctx context.Context, cmd *exec.Cmd, upid string) error {
	if cmd == nil {
		return b.waitTaskByUPID(ctx, upid)
	}

	done := make(chan error, 1)
	waitDone := make(chan struct{})

	go func() {
		done <- cmd.Wait()
		close(waitDone)
	}()

	taskStopped := make(chan string, 1)
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
			case <-waitDone:
			case <-ticker.C:
				task, err := tasklog.GetTaskByUPID(upid)
				if err != nil || task.Status == "running" {
					continue
				}
				taskStopped <- task.ExitStatus
			}
			return
		}
	}()

	select {
	case err := <-done:
		if err != nil {
			b.mu.Lock()
			b.err = err
			b.mu.Unlock()
			return err
		}
		return nil
	case <-ctx.Done():
		if cmd.Process != nil {
			if err := cmd.Process.Kill(); err != nil {
				b.logger.Error(err, "failed to kill backup process after cancellation")
			}
		}
		<-done
		b.mu.Lock()
		b.err = jobs.ErrCanceled
		b.mu.Unlock()
		return jobs.ErrCanceled
	case exitStatus := <-taskStopped:
		if cmd.Process != nil {
			if err := cmd.Process.Kill(); err != nil {
				b.logger.Error(err, "failed to kill backup process after task stopped", "upid", upid)
			}
		}
		<-done
		if taskExitSucceeded(exitStatus) {
			return nil
		}
		b.mu.Lock()
		b.err = jobs.ErrCanceled
		b.mu.Unlock()
		return jobs.ErrCanceled
	}
}

func taskExitSucceeded(exitStatus string) bool {
	return strings.HasPrefix(exitStatus, "OK") || strings.HasPrefix(exitStatus, "WARNINGS:")
}

func (b *backupJob) waitTaskByUPID(ctx context.Context, upid string) error {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return jobs.ErrCanceled
		case <-ticker.C:
			task, err := tasklog.GetTaskByUPID(upid)
			if err == nil && task.Status != "running" {
				return nil
			}
		}
	}
}

func (b *backupJob) startBackup(ctx context.Context, srcPath string, target coredb.Target, info jobs.ActivityInfo) (*exec.Cmd, proxmox.Task, string, error) {
	select {
	case <-ctx.Done():
		return nil, proxmox.Task{}, "", jobs.ErrCanceled
	default:
	}

	b.mu.RLock()
	job := b.job
	extraExclusions := b.extraExclusions
	b.mu.RUnlock()

	workerID, err := backupWorkerID(job)
	if err != nil {
		return nil, proxmox.Task{}, "", fmt.Errorf("determining backup worker identity: %w", err)
	}
	unlockStart := lockBackupStart(workerID)
	defer unlockStart()

	var checkpoint startCheckpoint
	before := make(map[string]struct{})
	if len(info.ResumeCheckpoint) > 0 {
		if err := json.Unmarshal(info.ResumeCheckpoint, &checkpoint); err != nil {
			return nil, proxmox.Task{}, "", jobs.NonRetryable(fmt.Errorf("decoding backup start checkpoint: %w", err))
		}
		if checkpoint.WorkerID != workerID {
			return nil, proxmox.Task{}, "", jobs.NonRetryable(fmt.Errorf("backup worker identity changed from %q to %q", checkpoint.WorkerID, workerID))
		}
		for _, upid := range checkpoint.Before {
			before[upid] = struct{}{}
		}
		task, found, err := tasklog.FindNewWorkerTask("backup", workerID, before)
		if err != nil {
			return nil, proxmox.Task{}, "", jobs.NonRetryable(fmt.Errorf("recovering backup task: %w", err))
		}
		if found {
			return nil, task, checkpoint.Owner, nil
		}
	}

	cmd, err := prepareBackupCommand(ctx, job, b.app, srcPath, target.IsAgent(), extraExclusions, b.logger)
	if err != nil {
		return nil, proxmox.Task{}, "", fmt.Errorf("%w: %w", ErrPrepareBackupCommand, err)
	}

	currOwner, err := GetCurrentOwner(job, b.app)
	if err != nil && !os.IsNotExist(err) {
		b.logger.Error(err, "failed to get current datastore owner")
	}
	if checkpoint.WorkerID != "" {
		currOwner = checkpoint.Owner
	}
	if err := FixDatastore(job, b.app); err != nil {
		b.logger.Error(err, "failed to fix datastore")
	}

	if checkpoint.WorkerID == "" {
		before, err = tasklog.SnapshotWorkerUPIDs("backup", workerID)
		if err != nil {
			return nil, proxmox.Task{}, "", fmt.Errorf("snapshotting backup tasks: %w", err)
		}
		checkpoint = startCheckpoint{WorkerID: workerID, Owner: currOwner, Before: make([]string, 0, len(before))}
		for upid := range before {
			checkpoint.Before = append(checkpoint.Before, upid)
		}
		value, err := json.Marshal(checkpoint)
		if err != nil {
			return nil, proxmox.Task{}, "", fmt.Errorf("encoding backup start checkpoint: %w", err)
		}
		if err := info.Checkpoint(ctx, value); err != nil {
			return nil, proxmox.Task{}, "", fmt.Errorf("checkpointing backup start intent: %w", err)
		}
	}

	b.mu.RLock()
	logger := b.logger
	b.mu.RUnlock()

	stdoutWriter := io.MultiWriter(logger.JobStdoutWriter(), os.Stdout)
	cmd.Stdout = stdoutWriter
	cmd.Stderr = stdoutWriter
	b.logger.Info("starting backup job", "args", cmd.Args)

	if err := cmd.Start(); err != nil {
		if currOwner != "" {
			if err := SetDatastoreOwner(job, b.app, currOwner); err != nil {
				b.logger.Error(err, "failed to restore datastore owner after start failure")
			}
		}
		return nil, proxmox.Task{}, "", fmt.Errorf("%w (cmd: %s): %w", ErrProxmoxBackupClientStart, cmd.String(), err)
	}

	abortStart := func() {
		if cmd.Process != nil {
			if err := cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
				b.logger.Error(err, "failed to kill unassociated backup process")
			}
			_ = cmd.Wait()
		}
		if currOwner != "" {
			if err := SetDatastoreOwner(job, b.app, currOwner); err != nil {
				b.logger.Error(err, "failed to restore datastore owner")
			}
		}
	}

	if cmd.Process != nil {
		b.mu.Lock()
		b.job.CurrentPID = cmd.Process.Pid
		b.mu.Unlock()
	}

	b.mu.RLock()
	loggerPath := logger.JobLogPath()
	b.mu.RUnlock()

	go monitorPBSClientLogs(ctx, loggerPath, cmd, b.logger)
	taskChan, errChan := b.startTaskMonitoring(ctx, workerID, before)

	select {
	case task := <-taskChan:
		return cmd, task, currOwner, nil
	case err := <-errChan:
		abortStart()
		return nil, proxmox.Task{}, "", fmt.Errorf("%w: %w", ErrTaskDetectionFailed, err)
	case <-ctx.Done():
		abortStart()
		return nil, proxmox.Task{}, "", jobs.ErrCanceled
	}
}

func (b *backupJob) startTaskMonitoring(ctx context.Context, workerID string, before map[string]struct{}) (<-chan proxmox.Task, <-chan error) {
	taskChan := make(chan proxmox.Task, 1)
	errChan := make(chan error, 1)

	go func() {
		defer b.logger.Info("monitor goroutine closing")

		timedCtx, timedCancel := context.WithTimeout(ctx, 20*time.Second)
		defer timedCancel()

		task, err := GetBackupTask(timedCtx, workerID, before)
		if err != nil {
			errChan <- err
			return
		}
		taskChan <- task
	}()

	return taskChan, errChan
}
