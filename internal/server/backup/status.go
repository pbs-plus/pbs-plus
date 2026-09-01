//go:build linux

package backup

import (
	"github.com/pbs-plus/pbs-plus/internal/proxmox"
	"github.com/pbs-plus/pbs-plus/internal/server/application"
	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
	"github.com/pbs-plus/pbs-plus/internal/server/jobs"
)

func updateBackupStatus(succeeded bool, warningsNum int, backup coredb.Backup, task proxmox.Task, executionID string, workflowStart, workflowEnd int64, app *application.Runtime) error {
	return jobs.UpdateJobHistory(
		backup.ID,
		executionID,
		backup.CurrentPID,
		succeeded,
		warningsNum,
		task,
		func() (coredb.JobHistory, int, error) {
			b, err := app.CoreDB.GetBackup(backup.ID)
			return b.History, b.CurrentPID, err
		},
		func(history coredb.JobHistory, currentPID int) error {
			applyWorkflowBounds(&history, workflowStart, workflowEnd, succeeded)
			return app.CoreDB.UpdateBackupHistory(backup.ID, history, currentPID)
		},
	)
}

func applyWorkflowBounds(history *coredb.JobHistory, start, end int64, succeeded bool) {
	if start <= 0 {
		return
	}
	history.LastRunStarttime = start
	history.LastRunEndtime = end
	history.Duration = max(end-start, 0)
	if succeeded && end > 0 {
		history.LastSuccessfulEndtime = end
	}
}
