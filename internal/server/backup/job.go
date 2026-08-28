//go:build linux

package backup

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/proxmox"
	"github.com/pbs-plus/pbs-plus/internal/proxmox/tasklog"
	"github.com/pbs-plus/pbs-plus/internal/server/application"
	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
	"github.com/pbs-plus/pbs-plus/internal/server/jobs"
	"github.com/pbs-plus/pbs-plus/internal/server/rpc/mountrpc"
)

type backupJob struct {
	mu     sync.RWMutex
	cancel context.CancelFunc

	Task      proxmox.Task
	currOwner string
	queueTask *tasklog.QueuedTask
	waitGroup *sync.WaitGroup
	err       error

	logger *log.Logger

	job coredb.Backup

	app             *application.Runtime
	skipCheck       bool
	web             bool
	extraExclusions []string

	cleanupOnce sync.Once
	started     atomic.Bool

	agentMount *mountrpc.AgentMount
	s3Mount    *mountrpc.S3Mount
	srcPath    string
	cmd        *exec.Cmd
	upid       string
}

func (b *backupJob) enqueue(ctx context.Context) error {
	wid := tasklog.FormatWorkerID(b.job.Store, "host-", b.job.Target.GetHostname())
	queueTask, err := tasklog.WriteQueuedLog("pbsplusgen-queue", "backup", wid, b.web)
	if err != nil {
		b.logger.Error(err, "failed to create queue task, not fatal")
	} else {
		if err := updateBackupStatus(false, 0, b.job, queueTask.Task, b.app); err != nil {
			b.logger.Error(err, "failed to set queue task, not fatal")
		}
	}

	b.mu.Lock()
	b.queueTask = queueTask
	b.mu.Unlock()
	return nil
}

func (b *backupJob) waitForCompletion(ctx context.Context, cmd *exec.Cmd, upid string) error {
	if cmd == nil {
		return b.waitTaskByUPID(ctx, upid)
	}

	done := make(chan error, 1)

	go func() {
		done <- cmd.Wait()
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
	}
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

func (b *backupJob) startBackup(ctx context.Context, srcPath string, target coredb.Target) (*exec.Cmd, proxmox.Task, string, error) {
	select {
	case <-ctx.Done():
		return nil, proxmox.Task{}, "", jobs.ErrCanceled
	default:
	}

	b.mu.RLock()
	qt := b.queueTask
	b.mu.RUnlock()
	if qt != nil {
		if err := qt.UpdateDescription("waiting for proxmox-backup-client to start"); err != nil {
			b.logger.Error(err, "failed to update queue task description")
		}
	}

	startupMu := b.app.Engine.StartupMu()
	startupMu.Lock()

	b.mu.RLock()
	job := b.job
	extraExclusions := b.extraExclusions
	b.mu.RUnlock()

	cmd, err := prepareBackupCommand(ctx, job, b.app, srcPath, target.IsAgent(), extraExclusions, b.logger)
	if err != nil {
		startupMu.Unlock()
		return nil, proxmox.Task{}, "", fmt.Errorf("%w: %w", ErrPrepareBackupCommand, err)
	}

	taskChan, readyChan, errChan := b.startTaskMonitoring(ctx, target)

	select {
	case <-readyChan:
	case err := <-errChan:
		startupMu.Unlock()
		return nil, proxmox.Task{}, "", fmt.Errorf("%w: %w", ErrTaskMonitoringInitializationFailed, err)
	case <-ctx.Done():
		startupMu.Unlock()
		if errors.Is(ctx.Err(), context.Canceled) {
			return nil, proxmox.Task{}, "", jobs.ErrCanceled
		}
		return nil, proxmox.Task{}, "", fmt.Errorf("%w: %w", ErrTaskMonitoringTimedOut, ctx.Err())
	}

	currOwner, err := GetCurrentOwner(job, b.app)
	if err != nil {
		b.logger.Error(err, "failed to get current datastore owner")
	}
	if err := FixDatastore(job, b.app); err != nil {
		b.logger.Error(err, "failed to fix datastore")
	}

	b.mu.RLock()
	logger := b.logger
	b.mu.RUnlock()

	stdoutWriter := io.MultiWriter(logger.JobStdoutWriter(), os.Stdout)
	cmd.Stdout = stdoutWriter
	cmd.Stderr = stdoutWriter
	b.logger.Info("starting backup job", "args", cmd.Args)

	if err := cmd.Start(); err != nil {
		startupMu.Unlock()
		if currOwner != "" {
			if err := SetDatastoreOwner(job, b.app, currOwner); err != nil {
				b.logger.Error(err, "failed to restore datastore owner after start failure")
			}
		}
		return nil, proxmox.Task{}, "", fmt.Errorf("%w (cmd: %s): %w", ErrProxmoxBackupClientStart, cmd.String(), err)
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

	var task proxmox.Task
	select {
	case task = <-taskChan:
		startupMu.Unlock()
	case err := <-errChan:
		startupMu.Unlock()
		return nil, proxmox.Task{}, "", fmt.Errorf("%w: %w", ErrTaskDetectionFailed, err)
	case <-ctx.Done():
		startupMu.Unlock()
		if err := cmd.Process.Kill(); err != nil {
			b.logger.Error(err, "failed to kill process after context cancellation")
		}
		if currOwner != "" {
			if err := SetDatastoreOwner(job, b.app, currOwner); err != nil {
				b.logger.Error(err, "failed to restore datastore owner")
			}
		}
		return nil, proxmox.Task{}, "", jobs.ErrCanceled
	}

	return cmd, task, currOwner, nil
}

func (b *backupJob) startTaskMonitoring(ctx context.Context, target coredb.Target) (<-chan proxmox.Task, <-chan struct{}, <-chan error) {
	readyChan := make(chan struct{})
	taskChan := make(chan proxmox.Task, 1)
	errChan := make(chan error, 1)

	b.mu.RLock()
	job := b.job
	b.mu.RUnlock()

	go func() {
		defer b.logger.Info("monitor goroutine closing")

		timedCtx, timedCancel := context.WithTimeout(ctx, 20*time.Second)
		defer timedCancel()

		task, err := GetBackupTask(timedCtx, readyChan, job, target)
		if err != nil {
			errChan <- err
			return
		}
		taskChan <- task
	}()

	return taskChan, readyChan, errChan
}
