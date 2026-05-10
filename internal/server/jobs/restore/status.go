//go:build linux

package restore

import (
	"github.com/pbs-plus/pbs-plus/internal/server/jobs"
	"github.com/pbs-plus/pbs-plus/internal/server/store"
	"github.com/pbs-plus/pbs-plus/internal/server/database"
	"github.com/pbs-plus/pbs-plus/internal/server/proxmox"
)

func updateRestoreStatus(succeeded bool, warningsNum int, restore database.Restore, task proxmox.Task, storeInstance *store.Store) error {
	return jobs.UpdateJobHistory(
		restore.ID,
		restore.CurrentPID,
		succeeded,
		warningsNum,
		task,
		func() (database.JobHistory, int, error) {
			r, err := storeInstance.Database.GetRestore(restore.ID)
			return r.History, r.CurrentPID, err
		},
		func(history database.JobHistory, currentPID int) error {
			r, err := storeInstance.Database.GetRestore(restore.ID)
			if err != nil {
				return err
			}
			r.CurrentPID = currentPID
			r.History = history
			return storeInstance.Database.UpdateRestore(nil, r)
		},
	)
}
