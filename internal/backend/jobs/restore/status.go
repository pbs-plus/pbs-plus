//go:build linux

package restore

import (
	"fmt"
	"sync"

	"github.com/pbs-plus/pbs-plus/internal/store"
	"github.com/pbs-plus/pbs-plus/internal/store/database"
	"github.com/pbs-plus/pbs-plus/internal/store/proxmox"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

var restoreMutexes sync.Map

func updateRestoreStatus(succeeded bool, warningsNum int, restore database.Restore, task proxmox.Task, storeInstance *store.Store) error {
	value, _ := restoreMutexes.LoadOrStore(restore.ID, &sync.Mutex{})
	mu := value.(*sync.Mutex)

	mu.Lock()
	defer mu.Unlock()

	taskFound, err := proxmox.GetTaskByUPID(task.UPID)
	if err != nil {
		syslog.L.Error(err).WithMessage("unable to get task by upid").Write()
		return err
	}

	latestRestore, err := storeInstance.Database.GetRestore(restore.ID)
	if err != nil {
		syslog.L.Error(err).WithMessage("unable to get restore").Write()
		return err
	}

	latestRestore.CurrentPID = restore.CurrentPID
	latestRestore.History.LastRunUpid = taskFound.UPID
	latestRestore.History.LastRunState = taskFound.Status
	latestRestore.History.LastRunEndtime = taskFound.EndTime

	if warningsNum > 0 && succeeded {
		latestRestore.History.LastRunState = fmt.Sprintf("WARNINGS: %d", warningsNum)
	}

	if succeeded {
		latestRestore.History.LastSuccessfulUpid = taskFound.UPID
		latestRestore.History.LastSuccessfulEndtime = task.EndTime
	}

	if err := storeInstance.Database.UpdateRestore(nil, latestRestore); err != nil {
		return err
	}

	return nil
}
