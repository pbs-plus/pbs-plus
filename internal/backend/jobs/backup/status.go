//go:build linux

package backup

import (
	"github.com/pbs-plus/pbs-plus/internal/backend/jobs"
	"github.com/pbs-plus/pbs-plus/internal/store"
	"github.com/pbs-plus/pbs-plus/internal/store/database"
	"github.com/pbs-plus/pbs-plus/internal/store/proxmox"
)

func updateBackupStatus(succeeded bool, warningsNum int, backup database.Backup, task proxmox.Task, storeInstance *store.Store) error {
	return jobs.UpdateJobHistory(
		backup.ID,
		backup.CurrentPID,
		succeeded,
		warningsNum,
		task,
		func() (database.JobHistory, int, error) {
			b, err := storeInstance.Database.GetBackup(backup.ID)
			return b.History, b.CurrentPID, err
		},
		func(history database.JobHistory, currentPID int) error {
			b, err := storeInstance.Database.GetBackup(backup.ID)
			if err != nil {
				return err
			}
			b.CurrentPID = currentPID
			b.History = history
			return storeInstance.Database.UpdateBackup(nil, b)
		},
	)
}
