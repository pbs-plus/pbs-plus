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
		return nil, errors.New("a job is still running; only one instance allowed")
	}

	var agentMount *mount.AgentMount
	var stdout, stderr io.ReadCloser

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

	monitorCtx, monitorCancel := context.WithTimeout(ctx, 20*time.Second)
	defer monitorCancel()

	var task *proxmox.Task
	var monitorErr error
	readyChan := make(chan struct{})
	go func() {
		defer monitorCancel()
		task, monitorErr = proxmox.Session.GetJobTask(monitorCtx, readyChan, job, target)
	}()

	select {
	case <-readyChan:
	case <-monitorCtx.Done():
		errCleanUp()
		return nil, fmt.Errorf("runBackupAttempt: task monitoring crashed: %w", monitorErr)
	}

	currOwner, _ := GetCurrentOwner(job, storeInstance)
	_ = FixDatastore(job, storeInstance)

	if err := cmd.Start(); err != nil {
		monitorCancel()
		if currOwner != "" {
			_ = SetDatastoreOwner(job, storeInstance, currOwner)
		}
		errCleanUp()
		return nil, fmt.Errorf("runBackupAttempt: proxmox-backup-client start error (%s): %w", cmd.String(), err)
	}

	var logLines []string
	var logGlobalMu sync.Mutex
	logDone := make(chan struct{})
	go func() {
		lines, _ := collectLogs(job.ID, cmd, stdout, stderr)
		logGlobalMu.Lock()
		logLines = lines
		logGlobalMu.Unlock()
		close(logDone)
	}()

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
					log.Printf("Backup attempt %d setup failed: %v", attempt, err)
					time.Sleep(10 * time.Second)
					continue
				}

				autoOp.Task = op.Task
				close(autoOp.started)

				if err := op.Wait(); err != nil {
					lastErr = err
					log.Printf("Backup attempt %d execution failed: %v", attempt, err)
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
