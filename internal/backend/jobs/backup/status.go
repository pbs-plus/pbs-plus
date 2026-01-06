//go:build linux

package backup

import (
	"fmt"
	"sync"

	"github.com/pbs-plus/pbs-plus/internal/store"
	"github.com/pbs-plus/pbs-plus/internal/store/proxmox"
	"github.com/pbs-plus/pbs-plus/internal/store/types"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

var backupMutexes sync.Map

func updateBackupStatus(succeeded bool, warningsNum int, backup types.Backup, task proxmox.Task, storeInstance *store.Store) error {
	value, _ := backupMutexes.LoadOrStore(backup.ID, &sync.Mutex{})
	mu := value.(*sync.Mutex)

	mu.Lock()
	defer mu.Unlock()

	taskFound, err := proxmox.GetTaskByUPID(task.UPID)
	if err != nil {
		syslog.L.Error(err).WithMessage("unable to get task by upid").Write()
		return err
	}

	latestBackup, err := storeInstance.Database.GetBackup(backup.ID)
	if err != nil {
		syslog.L.Error(err).WithMessage("unable to get backup").Write()
		return err
	}

	latestBackup.CurrentPID = backup.CurrentPID
	latestBackup.LastRunUpid = taskFound.UPID
	latestBackup.LastRunState = taskFound.Status
	latestBackup.LastRunEndtime = taskFound.EndTime

	if warningsNum > 0 && succeeded {
		latestBackup.LastRunState = fmt.Sprintf("WARNINGS: %d", warningsNum)
	}

	if succeeded {
		latestBackup.LastSuccessfulUpid = taskFound.UPID
		latestBackup.LastSuccessfulEndtime = task.EndTime
	}

	if err := storeInstance.Database.UpdateBackup(nil, latestBackup); err != nil {
		return err
	}

	return nil
}
