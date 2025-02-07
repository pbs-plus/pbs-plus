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

func RunBackup(job *types.Job, storeInstance *store.Store, skipCheck bool) (*BackupOperation, error) {
	jobInstanceMutex, err := filemutex.New(fmt.Sprintf("/tmp/pbs-plus-mutex-job-%s", job.ID))
	if err != nil {
		return nil, fmt.Errorf("RunBackup: failed to create mutex lock: %w", err)
	}

	if err := jobInstanceMutex.TryLock(); err != nil {
		return nil, errors.New("A job is still running. Only one instance of a job at a time is allowed.")
	}

	var agentMount *mount.AgentMount
	var stdout, stderr io.ReadCloser

	errCleanUp := func() {
		if jobInstanceMutex != nil {
			_ = jobInstanceMutex.Close()
		}
		if agentMount != nil {
			agentMount.Unmount()
			agentMount.CloseMount()
		}
		if stdout != nil {
			stdout.Close()
		}
		if stderr != nil {
			stderr.Close()
		}
	}

	backupMutex, err := filemutex.New("/tmp/pbs-plus-mutex-lock")
	if err != nil {
		errCleanUp()
		return nil, fmt.Errorf("RunBackup: failed to create mutex lock: %w", err)
	}
	defer backupMutex.Close()

	if err := backupMutex.Lock(); err != nil {
		errCleanUp()
		return nil, fmt.Errorf("RunBackup: failed to acquire lock: %w", err)
	}
	defer backupMutex.Unlock()

	if proxmox.Session.APIToken == nil {
		errCleanUp()
		return nil, fmt.Errorf("RunBackup: api token is required")
	}

	// Validate and setup target
	target, err := storeInstance.Database.GetTarget(job.Target)
	if err != nil {
		errCleanUp()
		return nil, fmt.Errorf("RunBackup: failed to get target: %w", err)
	}
	if target == nil {
		errCleanUp()
		return nil, fmt.Errorf("RunBackup: Target '%s' does not exist", job.Target)
	}

	if !skipCheck && !storeInstance.WSHub.AgentPing(target) {
		errCleanUp()
		return nil, fmt.Errorf("RunBackup: Target '%s' is unreachable or does not exist", job.Target)
	}

	srcPath := target.Path
	isAgent := strings.HasPrefix(target.Path, "agent://")

	if isAgent {
		if agentMount, err = mount.Mount(storeInstance, target); err != nil {
			errCleanUp()
			return nil, fmt.Errorf("RunBackup: mount initialization error: %w", err)
		}
		srcPath = agentMount.Path
	}

	srcPath = filepath.Join(srcPath, job.Subpath)

	// Prepare backup command
	cmd, err := prepareBackupCommand(job, storeInstance, srcPath, isAgent)
	if err != nil {
		errCleanUp()
		return nil, err
	}

	// Setup command pipes
	stdout, stderr, err = setupCommandPipes(cmd)
	if err != nil {
		errCleanUp()
		return nil, err
	}

	// Start monitoring in background first
	monitorCtx, monitorCancel := context.WithTimeout(context.Background(), 20*time.Second)

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

		return nil, fmt.Errorf("RunBackup: task monitoring crashed -> %w", monitorErr)
	}

	currOwner, _ := GetCurrentOwner(job, storeInstance)
	_ = FixDatastore(job, storeInstance)

	// Start collecting logs and wait for backup completion
	var logLines []string
	var logGlobalMu sync.Mutex

	go func() {
		logGlobalMu.Lock()
		defer logGlobalMu.Unlock()

		logLines, _ = collectLogs(job.ID, cmd, stdout, stderr)
	}()

	// Now start the backup process
	if err := cmd.Start(); err != nil {
		monitorCancel() // Cancel monitoring since backup failed to start

		if currOwner != "" {
			_ = SetDatastoreOwner(job, storeInstance, currOwner)
		}

		errCleanUp()

		return nil, fmt.Errorf("RunBackup: proxmox-backup-client start error (%s): %w", cmd.String(), err)
	}

	// Wait for either monitoring to complete or timeout
	select {
	case <-monitorCtx.Done():
		if task == nil {
			errCleanUp()

			if currOwner != "" {
				_ = SetDatastoreOwner(job, storeInstance, currOwner)
			}

			_ = cmd.Process.Kill()
			return nil, fmt.Errorf("RunBackup: no task created")
		}
	}

	if err := updateJobStatus(job, task, storeInstance); err != nil {
		errCleanUp()
		if currOwner != "" {
			_ = SetDatastoreOwner(job, storeInstance, currOwner)
		}

		return &BackupOperation{
			Task: task,
		}, fmt.Errorf("RunBackup: failed to update job status: %w", err)
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)

	operation := &BackupOperation{
		Task:      task,
		waitGroup: wg,
	}

	go func() {
		defer stdout.Close()
		defer stderr.Close()
		defer wg.Done()
		defer jobInstanceMutex.Close()

		if err := cmd.Wait(); err != nil {
			operation.err = err
		}

		logGlobalMu.Lock()
		defer logGlobalMu.Unlock()

		if err := writeLogsToFile(task.UPID, logLines); err != nil {
			log.Printf("Failed to write logs: %v", err)
		}

		if err := updateJobStatus(job, task, storeInstance); err != nil {
			log.Printf("RunBackup: failed to update job status: %v", err)
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
