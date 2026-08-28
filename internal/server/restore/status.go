//go:build linux

package restore

import (
	"github.com/pbs-plus/pbs-plus/internal/proxmox"
	"github.com/pbs-plus/pbs-plus/internal/server/application"
	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
	"github.com/pbs-plus/pbs-plus/internal/server/jobs"
)

func updateRestoreStatus(succeeded bool, warningsNum int, restore coredb.Restore, task proxmox.Task, app *application.Runtime) error {
	return jobs.UpdateJobHistory(
		restore.ID,
		restore.CurrentPID,
		succeeded,
		warningsNum,
		task,
		func() (coredb.JobHistory, int, error) {
			r, err := app.CoreDB.GetRestore(restore.ID)
			return r.History, r.CurrentPID, err
		},
		func(history coredb.JobHistory, currentPID int) error {
			r, err := app.CoreDB.GetRestore(restore.ID)
			if err != nil {
				return err
			}
			r.CurrentPID = currentPID
			r.History = history
			return app.CoreDB.UpdateRestore(nil, r)
		},
	)
}
