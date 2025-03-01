//go:build linux

package backup

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/alexflint/go-filemutex"
	"github.com/sonroyaalmerol/pbs-plus/internal/backend/mount"
	"github.com/sonroyaalmerol/pbs-plus/internal/store"
	"github.com/sonroyaalmerol/pbs-plus/internal/store/proxmox"
	"github.com/sonroyaalmerol/pbs-plus/internal/store/types"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils"
)

var ErrOneInstance = errors.New("a job is still running; only one instance allowed")

type BackupOperation struct {
	Task      *proxmox.Task
	waitGroup *sync.WaitGroup
	err       error
}

func (b *BackupOperation) Wait() error {
	if b.waitGroup != nil {
		b.waitGroup.Wait()
	}
	return b.err
}

func runBackupAttempt(
	ctx context.Context,
	job *types.Job,
	storeInstance *store.Store,
	skipCheck bool,
) (*BackupOperation, error) {
	jobInstanceMutex, err := filemutex.New(fmt.Sprintf("/tmp/pbs-plus-mutex-job-%s", job.ID))
	if err != nil {
		return nil, fmt.Errorf("runBackupAttempt: failed to create job mutex: %w", err)
	}
	if err := jobInstanceMutex.TryLock(); err != nil {
		return nil, ErrOneInstance
	}

	var agentMount *mount.AgentMount
	var stdout, stderr io.ReadCloser

	errCleanUp := func() {
		utils.ClearIOStats(job.CurrentPID)
		job.CurrentPID = 0

		_ = jobInstanceMutex.Close()
		if agentMount != nil {
			agentMount.Unmount()
			agentMount.CloseMount()
		}
		if stdout != nil {
			_ = stdout.Close()
		}
		if stderr != nil {
			_ = stderr.Close()
		}
	}

	backupMutex, err := filemutex.New("/tmp/pbs-plus-mutex-lock")
	if err != nil {
		errCleanUp()
		return nil, fmt.Errorf("runBackupAttempt: failed to create backup mutex: %w", err)
	}
	defer backupMutex.Close()

	if err := backupMutex.Lock(); err != nil {
		errCleanUp()
		return nil, fmt.Errorf("runBackupAttempt: failed to lock backup mutex: %w", err)
	}

	if proxmox.Session.APIToken == nil {
		errCleanUp()
		return nil, errors.New("runBackupAttempt: API token is required")
	}

	target, err := storeInstance.Database.GetTarget(job.Target)
	if err != nil {
		errCleanUp()
		return nil, fmt.Errorf("runBackupAttempt: failed to get target: %w", err)
	}
	if target == nil {
		errCleanUp()
		return nil, fmt.Errorf("runBackupAttempt: target '%s' does not exist", job.Target)
	}

	if !skipCheck {
		targetSplit := strings.Split(target.Name, " - ")
		arpcSess := storeInstance.GetARPC(targetSplit[0])
		if arpcSess != nil {
			timeout, timeoutCancel := context.WithTimeout(ctx, 3*time.Second)
			defer timeoutCancel()
			pingResp, err := arpcSess.CallContext(timeout, "ping", nil)
			if err != nil || pingResp.Status != 200 {
				errCleanUp()
				return nil, fmt.Errorf("runBackupAttempt: target '%s' is unreachable", job.Target)
			}
		}
	}

	srcPath := target.Path
	isAgent := strings.HasPrefix(target.Path, "agent://")
	if isAgent {
		agentMount, err = mount.Mount(storeInstance, job, target)
		if err != nil {
			errCleanUp()
			return nil, fmt.Errorf("runBackupAttempt: mount initialization error: %w", err)
		}
		srcPath = agentMount.Path
	}
	srcPath = filepath.Join(srcPath, job.Subpath)

	cmd, err := prepareBackupCommand(ctx, job, storeInstance, srcPath, isAgent)
	if err != nil {
		errCleanUp()
		return nil, err
	}
	stdout, stderr, err = setupCommandPipes(cmd)
	if err != nil {
		errCleanUp()
		return nil, err
	}

	// Create channels for task handling
	readyChan := make(chan struct{})
	taskResultChan := make(chan *proxmox.Task, 1)
	taskErrorChan := make(chan error, 1)

	// Setup monitoring context
	monitorCtx, monitorCancel := context.WithTimeout(ctx, 20*time.Second)
	defer monitorCancel()

	// Launch the task monitoring goroutine
	go func() {
		task, err := proxmox.Session.GetJobTask(monitorCtx, readyChan, job, target)
		if err != nil {
			select {
			case taskErrorChan <- err:
			case <-monitorCtx.Done():
			}
			return
		}
		select {
		case taskResultChan <- task:
		case <-monitorCtx.Done():
		}
	}()

	// Wait for monitor initialization
	select {
	case <-readyChan:
		// Watcher is ready
	case err := <-taskErrorChan:
		monitorCancel()
		errCleanUp()
		return nil, fmt.Errorf("runBackupAttempt: task monitoring initialization failed: %w", err)
	case <-monitorCtx.Done():
		errCleanUp()
		return nil, fmt.Errorf("runBackupAttempt: task monitoring initialization timed out: %w", monitorCtx.Err())
	}

	currOwner, _ := GetCurrentOwner(job, storeInstance)
	_ = FixDatastore(job, storeInstance)

	// Start the command now that watcher is ready
	if err := cmd.Start(); err != nil {
		monitorCancel()
		if currOwner != "" {
			_ = SetDatastoreOwner(job, storeInstance, currOwner)
		}
		errCleanUp()
		return nil, fmt.Errorf("runBackupAttempt: proxmox-backup-client start error (%s): %w", cmd.String(), err)
	}

	if cmd.Process != nil {
		job.CurrentPID = cmd.Process.Pid
	}

	// Setup log collection
	var logLines atomic.Value
	logDone := make(chan struct{})
	go func() {
		lines, _ := collectLogs(job.ID, cmd, stdout, stderr)
		// Atomically store the slice of log lines.
		logLines.Store(lines)
		close(logDone)
	}()

	// Wait for task to be detected
	var task *proxmox.Task
	select {
	case task = <-taskResultChan:
		if task == nil {
			monitorCancel()
			if cmd.Process != nil {
				_ = cmd.Process.Kill()
			}
			errCleanUp()
			if currOwner != "" {
				_ = SetDatastoreOwner(job, storeInstance, currOwner)
			}
			return nil, errors.New("runBackupAttempt: received nil task")
		}
	case err := <-taskErrorChan:
		monitorCancel()
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		errCleanUp()
		if currOwner != "" {
			_ = SetDatastoreOwner(job, storeInstance, currOwner)
		}
		return nil, fmt.Errorf("runBackupAttempt: task detection failed: %w", err)
	case <-monitorCtx.Done():
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		errCleanUp()
		if currOwner != "" {
			_ = SetDatastoreOwner(job, storeInstance, currOwner)
		}
		return nil, fmt.Errorf("runBackupAttempt: task detection timed out: %w", monitorCtx.Err())
	}

	// Task is guaranteed to be non-nil at this point
	if err := updateJobStatus(job, task, storeInstance); err != nil {
		errCleanUp()
		if currOwner != "" {
			_ = SetDatastoreOwner(job, storeInstance, currOwner)
		}
		return nil, fmt.Errorf("runBackupAttempt: failed to update job status: %w", err)
	}

	// Create operation with proper waitgroup
	wg := &sync.WaitGroup{}
	wg.Add(1)
	operation := &BackupOperation{
		Task:      task,
		waitGroup: wg,
	}

	go func() {
		defer wg.Done()
		defer jobInstanceMutex.Close()

		if err := cmd.Wait(); err != nil {
			operation.err = err
		}

		utils.ClearIOStats(job.CurrentPID)
		job.CurrentPID = 0

		<-logDone

		if val := logLines.Load(); val != nil {
			if lines, ok := val.([]string); ok {
				if err := writeLogsToFile(task.UPID, lines); err != nil {
					log.Printf("Failed to write logs: %v", err)
				}
			} else {
				log.Print("Failed to write logs: log lines stored differently")
			}
		} else {
			log.Print("Failed to write logs: log lines are missing")
		}

		if err := updateJobStatus(job, task, storeInstance); err != nil {
			log.Printf("runBackupAttempt: failed to update job status (post cmd.Wait): %v", err)
		}

		if currOwner != "" {
			_ = SetDatastoreOwner(job, storeInstance, currOwner)
		}

		if agentMount != nil {
			agentMount.Unmount()
			agentMount.CloseMount()
		}
		stdout.Close()
		stderr.Close()
	}()

	return operation, nil
}

type AutoBackupOperation struct {
	Task          *proxmox.Task
	err           error
	done          chan struct{}
	job           *types.Job
	storeInstance *store.Store

	ctx context.Context

	BackupOp *BackupOperation
	started  chan struct{}
}

func (a *AutoBackupOperation) Wait() error {
	select {
	case <-a.done:
		select {
		case <-a.started:
		default:
			if a.err != nil {
				a.updatePlusError()
			}
		}
		return a.err
	case <-a.ctx.Done():
		return errors.New("RunBackup: context cancelled")
	}
}

func (a *AutoBackupOperation) WaitForStart() error {
	select {
	case <-a.done:
		if a.err != nil {
			a.updatePlusError()
		}
		return a.err
	case <-a.started:
	case <-a.ctx.Done():
		return errors.New("context cancelled")
	}
	return nil
}

func (a *AutoBackupOperation) updatePlusError() {
	if a.err == nil {
		return
	}

	if !strings.Contains(a.err.Error(), "job is still running") {
		a.job.LastRunPlusError = a.err.Error()
		a.job.LastRunPlusTime = int(time.Now().Unix())
		if uErr := a.storeInstance.Database.UpdateJob(*a.job); uErr != nil {
			syslog.L.Errorf("LastRunPlusError update: %v", uErr)
		}
	}
}

func RunBackup(ctx context.Context, job *types.Job, storeInstance *store.Store, skipCheck bool) *AutoBackupOperation {
	autoOp := &AutoBackupOperation{
		done:          make(chan struct{}),
		started:       make(chan struct{}),
		job:           job,
		storeInstance: storeInstance,
		ctx:           ctx,
	}

	go func() {
		var lastErr error
		for attempt := 0; attempt <= job.Retry; attempt++ {
			select {
			case <-autoOp.ctx.Done():
				autoOp.err = errors.New("context cancelled")
				close(autoOp.done)
				return
			default:
				op, err := runBackupAttempt(ctx, job, storeInstance, skipCheck)
				if err != nil {
					lastErr = err
					if err == ErrOneInstance {
						autoOp.err = err
						close(autoOp.done)
						return
					}
					log.Printf("Backup attempt %d setup failed: %v", attempt, err)
					time.Sleep(10 * time.Second)
					continue
				}

				autoOp.Task = op.Task
				close(autoOp.started)

				if err := op.Wait(); err != nil {
					lastErr = err
					log.Printf("Backup attempt %d execution failed: %v", attempt, err)
					if err.Error() == "signal: killed" {
						autoOp.err = err
						close(autoOp.done)
						return
					}
					time.Sleep(10 * time.Second)
					autoOp.started = make(chan struct{})
					continue
				}

				autoOp.err = nil
				autoOp.BackupOp = op
				close(autoOp.done)
				return
			}
		}

		autoOp.err = fmt.Errorf("backup failed after %d attempts, last error: %w", job.Retry+1, lastErr)
		close(autoOp.done)
	}()
	return autoOp
}
