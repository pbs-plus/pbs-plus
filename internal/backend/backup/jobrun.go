//go:build linux

package backup

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
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

	ErrUnreachable := fmt.Errorf("runBackupAttempt: target '%s' is unreachable", job.Target)

	// Create temporary files for stdout and stderr
	clientLogFile, err := os.CreateTemp("", fmt.Sprintf("backup-%s-stdout-*", job.ID))
	if err != nil {
		return nil, fmt.Errorf("failed to create stdout temp file: %w", err)
	}
	clientLogPath := clientLogFile.Name()

	errorMonitorDone := make(chan struct{})

	var agentMount *mount.AgentMount

	errCleanUp := func() {
		utils.ClearIOStats(job.CurrentPID)
		job.CurrentPID = 0

		_ = jobInstanceMutex.Close()
		if agentMount != nil {
			agentMount.Unmount()
			agentMount.CloseMount()
		}
		if clientLogFile != nil {
			clientLogFile.Close()
			os.Remove(clientLogPath)
		}
		close(errorMonitorDone)
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
		pinged := false
		targetSplit := strings.Split(target.Name, " - ")
		arpcSess := storeInstance.GetARPC(targetSplit[0])
		if arpcSess != nil {
			pingResp, err := arpcSess.CallWithTimeout(3*time.Second, "ping", nil)
			if err == nil && pingResp.Status == 200 {
				pinged = true
			}
		}

		if !pinged {
			errCleanUp()
			return nil, ErrUnreachable
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

	// Create multi-writers that write to both file and standard output/error
	stdoutWriter := io.MultiWriter(clientLogFile, os.Stdout)

	cmd.Stdout = stdoutWriter
	cmd.Stderr = stdoutWriter

	// Start the command
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

	// Start a low-overhead monitor for critical errors
	go monitorPBSClientLogs(clientLogPath, cmd, errorMonitorDone)

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

		// Signal monitor to stop and wait for it
		close(errorMonitorDone)

		// Close the files
		clientLogFile.Close()

		// Read log files after process completes
		err := processPBSProxyLogs(task.UPID, clientLogPath)
		if err != nil {
			syslog.L.Errorf("Failed to process logs: %v", err)
		}

		// Clean up temp files
		os.Remove(clientLogPath)

		if err := updateJobStatus(job, task, storeInstance); err != nil {
			syslog.L.Errorf("runBackupAttempt: failed to update job status (post cmd.Wait): %v", err)
		}

		if currOwner != "" {
			_ = SetDatastoreOwner(job, storeInstance, currOwner)
		}

		if agentMount != nil {
			agentMount.Unmount()
			agentMount.CloseMount()
		}
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
	ErrUnreachable := fmt.Errorf("runBackupAttempt: target '%s' is unreachable", job.Target)

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
					if err == ErrOneInstance || err == ErrUnreachable {
						autoOp.err = err
						close(autoOp.done)
						return
					}
					syslog.L.Errorf("Backup attempt %d setup failed: %v", attempt, err)
					time.Sleep(10 * time.Second)
					continue
				}

				autoOp.Task = op.Task
				close(autoOp.started)

				if err := op.Wait(); err != nil {
					lastErr = err
					syslog.L.Errorf("Backup attempt %d execution failed: %v", attempt, err)
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
