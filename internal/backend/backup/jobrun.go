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
	"time"

	"github.com/alexflint/go-filemutex"
	"github.com/sonroyaalmerol/pbs-plus/internal/backend/mount"
	"github.com/sonroyaalmerol/pbs-plus/internal/store"
	"github.com/sonroyaalmerol/pbs-plus/internal/store/proxmox"
	"github.com/sonroyaalmerol/pbs-plus/internal/store/types"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
)

// BackupOperation represents one backup attempt.
// Its Wait() method waits for the asynchronous logging/cleanup to complete
// and returns an error if the backup command (or its post-processing)
// eventually fails.
//
// The additional 'started' channel is closed as soon as the goroutine
// that waits on cmd.Wait() has been launched.
type BackupOperation struct {
	Task      *proxmox.Task
	waitGroup *sync.WaitGroup
	err       error
}

// Wait blocks until the asynchronous operations (e.g., cmd.Wait logging)
// are complete, and then returns the error encountered (if any).
func (b *BackupOperation) Wait() error {
	if b.waitGroup != nil {
		b.waitGroup.Wait()
	}
	return b.err
}

// runBackupAttempt performs one complete backup attempt. It returns an error
// immediately if any failure occurs during setup (locks, mounts, etc.). Otherwise,
// it starts the backup command and launches background goroutines for monitoring,
// logging, and waiting on the command. Any error occurring later is stored in the
// returned BackupOperation, to be retrievable via Wait().
func runBackupAttempt(
	ctx context.Context,
	job *types.Job,
	storeInstance *store.Store,
	skipCheck bool,
) (*BackupOperation, error) {
	// Create a mutex to ensure that only one instance of this job is running.
	jobInstanceMutex, err :=
		filemutex.New(fmt.Sprintf("/tmp/pbs-plus-mutex-job-%s", job.ID))
	if err != nil {
		return nil, fmt.Errorf("runBackupAttempt: failed to create job mutex: %w", err)
	}
	if err := jobInstanceMutex.TryLock(); err != nil {
		return nil, errors.New("a job is still running; only one instance allowed")
	}

	var agentMount *mount.AgentMount
	var stdout, stderr io.ReadCloser

	// Local clean-up helper.
	errCleanUp := func() {
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

	// Acquire a global backup lock.
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
	defer backupMutex.Unlock()

	// Ensure required API token is set.
	if proxmox.Session.APIToken == nil {
		errCleanUp()
		return nil, errors.New("runBackupAttempt: API token is required")
	}

	// Validate and setup the target.
	target, err := storeInstance.Database.GetTarget(job.Target)
	if err != nil {
		errCleanUp()
		return nil, fmt.Errorf("runBackupAttempt: failed to get target: %w", err)
	}
	if target == nil {
		errCleanUp()
		return nil, fmt.Errorf("runBackupAttempt: target '%s' does not exist", job.Target)
	}

	if !skipCheck && !storeInstance.WSHub.AgentPing(target) {
		errCleanUp()
		return nil, fmt.Errorf("runBackupAttempt: target '%s' is unreachable", job.Target)
	}

	srcPath := target.Path
	isAgent := strings.HasPrefix(target.Path, "agent://")
	if isAgent {
		agentMount, err = mount.Mount(storeInstance, target)
		if err != nil {
			errCleanUp()
			return nil, fmt.Errorf("runBackupAttempt: mount initialization error: %w", err)
		}
		srcPath = agentMount.Path
	}
	srcPath = filepath.Join(srcPath, job.Subpath)

	// Prepare the backup command.
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

	// Start monitoring for the backup task.
	monitorCtx, monitorCancel :=
		context.WithTimeout(ctx, 20*time.Second)
	defer monitorCancel()

	var task *proxmox.Task
	var monitorErr error
	readyChan := make(chan struct{})
	go func() {
		defer monitorCancel()
		task, monitorErr = proxmox.Session.GetJobTask(monitorCtx, readyChan, job, target)
	}()

	// Wait for either the task to be created or a timeout.
	select {
	case <-readyChan:
	case <-monitorCtx.Done():
		errCleanUp()
		return nil, fmt.Errorf("runBackupAttempt: task monitoring crashed: %w",
			monitorErr)
	}

	currOwner, _ := GetCurrentOwner(job, storeInstance)
	_ = FixDatastore(job, storeInstance)

	// Launch a goroutine to collect command logs concurrently.
	var logLines []string
	var logGlobalMu sync.Mutex
	logDone := make(chan struct{})
	go func() {
		logLines, _ = collectLogs(job.ID, cmd, stdout, stderr)
		close(logDone)
	}()

	// Start the backup process.
	if err := cmd.Start(); err != nil {
		monitorCancel()
		if currOwner != "" {
			_ = SetDatastoreOwner(job, storeInstance, currOwner)
		}
		errCleanUp()
		return nil, fmt.Errorf("runBackupAttempt: proxmox-backup-client start error (%s): %w",
			cmd.String(), err)
	}

	// Ensure that a task was created.
	select {
	case <-monitorCtx.Done():
		if task == nil {
			errCleanUp()
			if currOwner != "" {
				_ = SetDatastoreOwner(job, storeInstance, currOwner)
			}
			_ = cmd.Process.Kill()
			return nil, fmt.Errorf("runBackupAttempt: no task created")
		}
	}

	if err := updateJobStatus(job, task, storeInstance); err != nil {
		errCleanUp()
		if currOwner != "" {
			_ = SetDatastoreOwner(job, storeInstance, currOwner)
		}
		return nil, fmt.Errorf("runBackupAttempt: failed to update job status: %w", err)
	}

	// Prepare an operation that will capture any error from cmd.Wait.
	wg := &sync.WaitGroup{}
	wg.Add(1)
	operation := &BackupOperation{
		Task:      task,
		waitGroup: wg,
	}

	// Spawn a goroutine that waits on the backup command and completes logging/cleanup.
	go func() {
		defer stdout.Close()
		defer stderr.Close()
		defer wg.Done()
		defer jobInstanceMutex.Close()

		// Wait for the backup command to finish.
		if err := cmd.Wait(); err != nil {
			operation.err = err
		}

		<-logDone

		logGlobalMu.Lock()
		defer logGlobalMu.Unlock()

		if err := writeLogsToFile(task.UPID, logLines); err != nil {
			log.Printf("Failed to write logs: %v", err)
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
	}()

	// Return the backup operation.
	return operation, nil
}

/*
   AUTO–RETRY BACKGROUND OPERATION

   This wraps a backup attempt that auto–retries until a backup
   succeeds or the maximum number of attempts is reached.
   Even if a caller never waits on it, the auto-retry loop continues.
*/

// AutoBackupOperation wraps a backup attempt that auto-retries.
type AutoBackupOperation struct {
	Task          *proxmox.Task
	err           error
	done          chan struct{}
	job           *types.Job
	storeInstance *store.Store

	ctx context.Context

	// Allow access to the underlying BackupOperation if needed.
	BackupOp *BackupOperation
	started  chan struct{}
}

// Wait blocks until the entire auto-retry process has finished,
// and returns the final outcome (nil if a backup succeeded).
func (a *AutoBackupOperation) Wait() error {
	hasStarted := false
	for {
		select {
		case <-a.done:
			if a.err != nil && !hasStarted {
				a.updatePlusError()
			}
			return a.err
		case <-a.started:
			hasStarted = true
		case <-a.ctx.Done():
			return errors.New("RunBackup: context cancelled")
		}
	}
}

// WaitForStart blocks until the backup command's wait goroutine has been started.
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
	if !strings.Contains(a.err.Error(), "A job is still running.") {
		a.job.LastRunPlusError = a.err.Error()
		a.job.LastRunPlusTime = int(time.Now().Unix())
		if uErr := a.storeInstance.Database.UpdateJob(*a.job); uErr != nil {
			syslog.L.Errorf("LastRunPlusError update: %v", uErr)
		}
	}
}

// RunBackup launches the backup process in the background and automatically
// retries up to job.Retry+1 attempts. It returns immediately; even if no caller
// waits on it, the auto-retry loop will keep it alive.
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
		// Retry up to job.Retry+1 times.
		for attempt := 0; attempt <= job.Retry; attempt++ {
			select {
			case <-autoOp.ctx.Done():
				autoOp.err = errors.New("context cancelled")
				return
			default:
				op, err := runBackupAttempt(ctx, job, storeInstance, skipCheck)
				if err != nil {
					lastErr = err
					log.Printf("Backup attempt %d setup failed: %v", attempt, err)
					time.Sleep(1 * time.Second)
					continue
				}

				// Signal that the cmd.Wait goroutine has started.
				close(autoOp.started)

				// Wait for the asynchronous backup process to finish.
				if err := op.Wait(); err != nil {
					lastErr = err
					log.Printf("Backup attempt %d execution failed: %v", attempt, err)
					time.Sleep(1 * time.Second)
					continue
				}
				// A backup attempt succeeded.
				autoOp.Task = op.Task
				autoOp.err = nil
				autoOp.BackupOp = op
				close(autoOp.done)
				return
			}
		}

		// If all attempts are exhausted, report the final error.
		autoOp.err = fmt.Errorf("backup failed after %d attempts, last error: %w",
			job.Retry+1, lastErr)
		close(autoOp.done)
	}()
	return autoOp
}
