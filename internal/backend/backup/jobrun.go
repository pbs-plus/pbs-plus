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
	"github.com/sonroyaalmerol/pbs-plus/internal/store/proxmox"
	"github.com/sonroyaalmerol/pbs-plus/internal/store/types"
)

func RunBackup(job *types.Job, storeInstance *store.Store) (*proxmox.Task, error) {
	backupMutex, err := filemutex.New("/tmp/pbs-plus-mutex-lock")
	if err != nil {
		return nil, fmt.Errorf("failed to create mutex: %w", err)
	}
	defer backupMutex.Close()

	if err := backupMutex.Lock(); err != nil {
		return nil, fmt.Errorf("failed to acquire lock: %w", err)
	}
	defer backupMutex.Unlock()

	if proxmox.Session.APIToken == nil {
		return nil, fmt.Errorf("api token required")
	}

	target, err := storeInstance.Database.GetTarget(job.Target)
	if err != nil {
		return nil, fmt.Errorf("failed to get target: %w", err)
	}
	if target == nil {
		return nil, fmt.Errorf("target '%s' not found", job.Target)
	}
	if !storeInstance.WSHub.AgentPing(target) {
		return nil, fmt.Errorf("target '%s' unreachable", job.Target)
	}

	srcPath := target.Path
	isAgent := strings.HasPrefix(target.Path, "agent://")

	var agentMount *mount.AgentMount
	if isAgent {
		if agentMount, err = mount.Mount(storeInstance, target); err != nil {
			return nil, fmt.Errorf("mount error: %w", err)
		}
		defer func() {
			agentMount.Unmount()
			agentMount.CloseMount()
		}()
		srcPath = agentMount.Path
	}

	srcPath = filepath.Join(srcPath, job.Subpath)

	cmd, err := prepareBackupCommand(job, storeInstance, srcPath, isAgent)
	if err != nil {
		return nil, err
	}

	stdout, stderr, err := setupCommandPipes(cmd)
	if err != nil {
		return nil, err
	}
	defer stdout.Close()
	defer stderr.Close()

	logCollector := NewLogCollector(1000)
	go logCollector.collectLogs(stdout, stderr)

	var task *proxmox.Task
	monitorCtx, monitorCancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer monitorCancel()

	var wg sync.WaitGroup
	wg.Add(1)
	errorChan := make(chan error, 1)

	go func() {
		defer wg.Done()
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-monitorCtx.Done():
				errorChan <- fmt.Errorf("monitoring timed out")
				return
			case <-ticker.C:
				if t, err := proxmox.Session.GetJobTask(monitorCtx, nil, job, target); err != nil {
					continue
				} else if t != nil {
					task = t
					return
				}
			}
		}
	}()

	wg.Wait()

	select {
	case err := <-errorChan:
		return nil, err
	default:
		if task == nil {
			return nil, fmt.Errorf("no task created")
		}
	}

	currOwner, _ := GetCurrentOwner(job, storeInstance)
	_ = FixDatastore(job, storeInstance)

	if err := cmd.Start(); err != nil {
		if currOwner != "" {
			_ = SetDatastoreOwner(job, storeInstance, currOwner)
		}
		return nil, fmt.Errorf("backup start failed: %w", err)
	}

	completionCtx, cancel := context.WithTimeout(context.Background(), 2*time.Hour)
	defer cancel()

	go func() {
		select {
		case <-completionCtx.Done():
			cmd.Process.Kill()
		case <-logCollector.done:
		}
	}()

	cmdErr := make(chan error, 1)
	go func() {
		cmdErr <- cmd.Wait()
	}()

	select {
	case err = <-cmdErr:
		if err != nil {
			if currOwner != "" {
				_ = SetDatastoreOwner(job, storeInstance, currOwner)
			}
			return task, fmt.Errorf("backup failed: %w", err)
		}
	case <-completionCtx.Done():
		cmd.Process.Kill()
		if currOwner != "" {
			_ = SetDatastoreOwner(job, storeInstance, currOwner)
		}
		return task, fmt.Errorf("backup timed out")
	}

	if err := writeLogsToFile(task.UPID, logCollector.lines); err != nil {
		log.Printf("Failed to write logs: %v", err)
	}

	if err := updateJobStatus(job, task, storeInstance); err != nil {
		log.Printf("Failed to update job status: %v", err)
	}

	if currOwner != "" {
		_ = SetDatastoreOwner(job, storeInstance, currOwner)
	}

	return task, nil
}
