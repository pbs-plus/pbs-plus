//go:build linux

package backup

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/proxmox"
	"github.com/pbs-plus/pbs-plus/internal/proxmox/tasklog"
	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
)

func backupWorkerID(job coredb.Backup) (string, error) {
	backupID, err := getBackupId(job)
	if err != nil {
		return "", err
	}
	return tasklog.FormatWorkerID(job.Store, "host-", backupID), nil
}

func GetBackupTask(ctx context.Context, workerID string, before map[string]struct{}) (proxmox.Task, error) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		task, found, err := tasklog.FindNewWorkerTask("backup", workerID, before)
		if err != nil {
			return proxmox.Task{}, err
		}
		if found {
			return task, nil
		}

		select {
		case <-ctx.Done():
			return proxmox.Task{}, fmt.Errorf("timed out")
		case <-ticker.C:
		}
	}
}

func GenerateBackupTaskErrorFile(job coredb.Backup, pbsError error, additionalData []string, jobLogPath string) (proxmox.Task, error) {
	wid, err := backupWorkerID(job)
	if err != nil {
		return proxmox.Task{}, err
	}

	wt, err := tasklog.NewWorkerTask("pbsplusgen-error", "backup", wid)
	if err != nil {
		return proxmox.Task{}, err
	}

	for _, data := range additionalData {
		wt.LogString(data)
	}

	wt.LogString(pbsError.Error())
	appendBackupJobLog(wt, jobLogPath)

	wt.CloseWithStatus(tasklog.CreateState(pbsError, 0))

	return wt.Task, nil
}

func appendBackupJobLog(task *tasklog.WorkerTask, path string) {
	if path == "" {
		return
	}
	file, err := os.Open(path)
	if err != nil {
		task.LogString("failed to open backup job log: " + err.Error())
		return
	}
	defer file.Close()
	task.LogString("--- proxmox-backup-client log starts here ---")
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		task.LogString(scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		task.LogString("failed to read backup job log: " + err.Error())
	}
}

func GenerateBackupTaskOKFile(job coredb.Backup, additionalData []string) (proxmox.Task, error) {
	wid, err := backupWorkerID(job)
	if err != nil {
		return proxmox.Task{}, err
	}

	wt, err := tasklog.NewWorkerTask("pbsplusgen-ok", "backup", wid)
	if err != nil {
		return proxmox.Task{}, err
	}

	for _, data := range additionalData {
		wt.LogString(data)
	}

	wt.CloseOK()

	return wt.Task, nil
}
