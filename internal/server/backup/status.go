//go:build linux

package backup

import (
	"github.com/pbs-plus/pbs-plus/internal/proxmox"
	"github.com/pbs-plus/pbs-plus/internal/server/application"
	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
	"github.com/pbs-plus/pbs-plus/internal/server/jobs"
)

func updateBackupStatus(succeeded bool, warningsNum int, backup coredb.Backup, task proxmox.Task, app *application.Runtime) error {
	return jobs.UpdateJobHistory(
		backup.ID,
		backup.CurrentPID,
		succeeded,
		warningsNum,
		task,
		func() (coredb.JobHistory, int, error) {
			b, err := app.CoreDB.GetBackup(backup.ID)
			return b.History, b.CurrentPID, err
		},
		func(history coredb.JobHistory, currentPID int) error {
			b, err := app.CoreDB.GetBackup(backup.ID)
			if err != nil {
				return err
			}
			b.CurrentPID = currentPID
			b.History = history
			return app.CoreDB.UpdateBackup(nil, b)
		},
	)
}
