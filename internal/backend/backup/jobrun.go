//go:build linux

package backup

import (
	"context"
	"fmt"
	"log"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/alexflint/go-filemutex"
	"github.com/sonroyaalmerol/pbs-plus/internal/backend/mount"
	"github.com/sonroyaalmerol/pbs-plus/internal/store"
)

func RunBackup(job *store.Job, storeInstance *store.Store) (*store.Task, error) {
	backupMutex, err := filemutex.New("/tmp/pbs-plus-mutex-lock")
	if err != nil {
		return nil, fmt.Errorf("RunBackup: failed to create mutex lock: %w", err)
	}
	defer backupMutex.Close() // Ensure mutex is always closed

	if err := backupMutex.Lock(); err != nil {
		return nil, fmt.Errorf("RunBackup: failed to acquire lock: %w", err)
	}
	defer backupMutex.Unlock()

	if storeInstance.APIToken == nil {
		return nil, fmt.Errorf("RunBackup: api token is required")
	}

	// Validate and setup target
	target, err := storeInstance.GetTarget(job.Target)
	if err != nil {
		return nil, fmt.Errorf("RunBackup: failed to get target: %w", err)
	}
	if target == nil {
		return nil, fmt.Errorf("RunBackup: Target '%s' does not exist", job.Target)
	}
	if !target.ConnectionStatus {
		return nil, fmt.Errorf("RunBackup: Target '%s' is unreachable or does not exist", job.Target)
	}

	srcPath := target.Path
	isAgent := strings.HasPrefix(target.Path, "agent://")

	var agentMount *mount.AgentMount
	if isAgent {
		if agentMount, err = mount.Mount(storeInstance, target); err != nil {
			return nil, fmt.Errorf("RunBackup: mount initialization error: %w", err)
		}
		srcPath = agentMount.Path
	}

	srcPath = filepath.Join(srcPath, job.Subpath)

	// Prepare backup command
	cmd, err := prepareBackupCommand(job, storeInstance, srcPath, isAgent)
	if err != nil {
		return nil, err
	}

	// Setup command pipes
	stdout, stderr, err := setupCommandPipes(cmd)
	if err != nil {
		return nil, err
	}
	defer stdout.Close()
	defer stderr.Close()

	// Start monitoring in background first
	monitorCtx, monitorCancel := context.WithTimeout(context.Background(), 20*time.Second)

	var task *store.Task
	var monitorErr error

	readyChan := make(chan struct{})
	go func() {
		defer monitorCancel()
		task, monitorErr = storeInstance.GetJobTask(monitorCtx, readyChan, job)
	}()

	select {
	case <-readyChan:
	case <-monitorCtx.Done():
		return nil, fmt.Errorf("RunBackup: task monitoring crashed -> %w", monitorErr)
	}

	// Start collecting logs and wait for backup completion
	var logLines []string
	var logMu sync.Mutex
	var logGlobalMu sync.Mutex

	go func() {
		logGlobalMu.Lock()
		defer logGlobalMu.Unlock()

		collectLogs(stdout, stderr, logLines, &logMu)
	}()

	// Now start the backup process
	if err := cmd.Start(); err != nil {
		monitorCancel() // Cancel monitoring since backup failed to start
		return nil, fmt.Errorf("RunBackup: proxmox-backup-client start error (%s): %w", cmd.String(), err)
	}

	// Wait for either monitoring to complete or timeout
	select {
	case <-monitorCtx.Done():
		if task == nil {
			_ = cmd.Process.Kill()
			return nil, fmt.Errorf("RunBackup: no task created")
		}
	}

	if err := updateJobStatus(job, task, storeInstance); err != nil {
		return task, fmt.Errorf("RunBackup: failed to update job status: %w", err)
	}

	go func() {
		_ = cmd.Wait()

		logGlobalMu.Lock()
		defer logGlobalMu.Unlock()

		if err := writeLogsToFile(task.UPID, logLines); err != nil {
			log.Printf("Failed to write logs: %v", err)
		}

		if err := updateJobStatus(job, task, storeInstance); err != nil {
			log.Printf("RunBackup: failed to update job status: %v", err)
		}

		if agentMount != nil {
			agentMount.Unmount()
			agentMount.CloseSFTP()
		}
	}()

	return task, nil
}
