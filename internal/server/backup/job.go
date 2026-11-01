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

const (
	agentOutageGrace = 15 * time.Minute

	maxBackupRuntime = 72 * time.Hour
)

var stallCheckInterval = time.Minute

// noProgressGrace is generous because a large source legitimately spends a long time in metadata comparison, emitting no chunks and no task log lines.
var noProgressGrace = 2 * time.Hour

var exitDrainTimeout = 30 * time.Second

// awaitExit bounds the post-kill wait: a client wedged in D state ignores SIGKILL, and the unmount that frees it only runs after this returns.
func (b *backupJob) awaitExit(done <-chan error, upid string) {
	timer := time.NewTimer(exitDrainTimeout)
	defer timer.Stop()

	select {
	case err := <-done:
		if err != nil {
			b.logger.Debug("backup process exited", "error", err.Error(), "upid", upid)
		}
	case <-timer.C:
		b.logger.Warn("backup process did not exit after kill, proceeding to unmount",
			"timeout", exitDrainTimeout.String(), "upid", upid)
	}
}

// watchStall reports when a run can no longer make progress, either because
// its agent transport has been down past the grace period or because it has
// outlived the runtime ceiling.
type mountProgressReporter interface {
	Progress() (connected bool, activity int64)
}

func (b *backupJob) watchStall(ctx context.Context, upid string, finished <-chan struct{}, mount mountProgressReporter) <-chan error {
	stalled := make(chan error, 1)
	idleGrace := noProgressGrace
	checkInterval := stallCheckInterval

	go func() {
		ticker := time.NewTicker(checkInterval)
		defer ticker.Stop()

		start := time.Now()
		var downSince time.Time
		lastActivity := int64(-1)
		activitySince := time.Now()

		for {
			select {
			case <-ctx.Done():
				return
			case <-finished:
				return
			case <-ticker.C:
			}

			if time.Since(start) > maxBackupRuntime {
				stalled <- fmt.Errorf("%w: exceeded %s runtime ceiling", ErrBackupStalled, maxBackupRuntime)
				return
			}

			if mount == nil {
				continue
			}
			connected, activity := mount.Progress()
			if connected {
				if !downSince.IsZero() {
					b.logger.Info("agent connection restored", "upid", upid)
					downSince = time.Time{}
				}
				if activity != lastActivity {
					lastActivity = activity
					activitySince = time.Now()
					continue
				}
				if time.Since(activitySince) > idleGrace {
					stalled <- fmt.Errorf("%w: agent served no filesystem operations for %s", ErrBackupStalled, idleGrace)
					return
				}
				continue
			}
			if downSince.IsZero() {
				downSince = time.Now()
				b.logger.Warn("agent connection lost, cancelling run if it does not return",
					"waitingFor", agentOutageGrace.String(), "upid", upid)
				continue
			}
			if time.Since(downSince) > agentOutageGrace {
				stalled <- fmt.Errorf("%w: agent transport down for %s", ErrBackupStalled, agentOutageGrace)
				return
			}
		}
	}()

	return stalled
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

	b.mu.RLock()
	agentMount := b.agentMount
	b.mu.RUnlock()
	var mount mountProgressReporter
	if agentMount != nil {
		mount = agentMount
	}

	stalled := b.watchStall(ctx, upid, waitDone, mount)

	select {
	case err := <-done:
		if err != nil {
			b.mu.Lock()
			b.err = err
			b.mu.Unlock()
			return err
		}
		return nil
	case err := <-stalled:
		b.logger.Error(err, "killing stalled backup process", "upid", upid)
		if cmd.Process != nil {
			if killErr := cmd.Process.Kill(); killErr != nil {
				b.logger.Error(killErr, "failed to kill stalled backup process", "upid", upid)
			}
		}
		b.awaitExit(done, upid)
		b.mu.Lock()
		b.err = err
		b.mu.Unlock()
		return err
	case <-ctx.Done():
		if cmd.Process != nil {
			if err := cmd.Process.Kill(); err != nil {
				b.logger.Error(err, "failed to kill backup process after cancellation")
			}
		}
		b.awaitExit(done, upid)
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
		b.awaitExit(done, upid)
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
	b.logger.Info("starting proxmox-backup-client", "command", cmd.String())

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
