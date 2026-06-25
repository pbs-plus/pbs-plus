//go:build linux

package backup

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/proxmox"
	"github.com/pbs-plus/pbs-plus/internal/proxmox/tasklog"
	"github.com/pbs-plus/pbs-plus/internal/server/database"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"os"
)

func GetBackupTask(
	ctx context.Context,
	readyChan chan struct{},
	job database.Backup,
	target database.Target,
) (proxmox.Task, error) {
	hostname, err := os.Hostname()
	if err != nil {
		if hostnameBytes, err := os.ReadFile("/etc/hostname"); err == nil {
			hostname = strings.TrimSpace(string(hostnameBytes))
		} else {
			hostname = "localhost"
		}
	}

	startTimeThreshold := time.Now().Unix()
	backupID := hostname
	if target.IsAgent() {
		backupID = target.GetHostname()
	}
	backupID = proxmox.NormalizeHostname(backupID)

	searchString := fmt.Sprintf(":backup:%s%shost-%s", proxmox.EncodeToHexEscapes(job.Store), proxmox.EncodeToHexEscapes(":"), proxmox.EncodeToHexEscapes(backupID))

	close(readyChan)

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return proxmox.Task{}, fmt.Errorf("timed out")
		case <-ticker.C:
			if task, found := tasklog.FindRunningTask("backup", searchString, startTimeThreshold); found {
				return task, nil
			}
		}
	}
}

func GenerateBackupTaskErrorFile(job database.Backup, pbsError error, additionalData []string) (proxmox.Task, error) {
	targetName := job.Target.GetHostname()
	wid := fmt.Sprintf("%s%shost-%s", proxmox.EncodeToHexEscapes(job.Store), proxmox.EncodeToHexEscapes(":"), proxmox.EncodeToHexEscapes(targetName))

	wt, err := tasklog.NewWorkerTask("pbsplusgen-error", "backup", wid)
	if err != nil {
		return proxmox.Task{}, err
	}

	for _, data := range additionalData {
		wt.LogString(data)
	}

	wt.LogString(pbsError.Error())

	wt.CloseWithStatus(tasklog.CreateState(pbsError, 0), func() {
		if err := tasklog.RemoveActive(wt.UPID()); err != nil {
			syslog.L.Error(err).Write()
		}
	})

	return wt.Task, nil
}

func GenerateBackupTaskOKFile(job database.Backup, additionalData []string) (proxmox.Task, error) {
	targetName := job.Target.GetHostname()
	wid := fmt.Sprintf("%s%shost-%s", proxmox.EncodeToHexEscapes(job.Store), proxmox.EncodeToHexEscapes(":"), proxmox.EncodeToHexEscapes(targetName))

	wt, err := tasklog.NewWorkerTask("pbsplusgen-ok", "backup", wid)
	if err != nil {
		return proxmox.Task{}, err
	}

	for _, data := range additionalData {
		wt.LogString(data)
	}

	wt.CloseWithStatus(tasklog.TaskState{Status: tasklog.StatusOK, EndTime: time.Now().Unix()}, func() {
		if err := tasklog.RemoveActive(wt.UPID()); err != nil {
			syslog.L.Error(err).Write()
		}
	})

	return wt.Task, nil
}
