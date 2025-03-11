//go:build linux

package backup

import (
	"github.com/sonroyaalmerol/pbs-plus/internal/store"
	"github.com/sonroyaalmerol/pbs-plus/internal/store/proxmox"
	"github.com/sonroyaalmerol/pbs-plus/internal/store/types"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
)

func updateJobStatus(succeeded bool, job *types.Job, task *proxmox.Task, storeInstance *store.Store) error {
	// Update task status
	taskFound, err := proxmox.Session.GetTaskByUPID(task.UPID)
	if err != nil {
		syslog.L.Error(err).WithMessage("unable to get task by upid").Write()
		return err
	}

	// Update job status
	latestJob, err := storeInstance.Database.GetJob(job.ID)
	if err != nil {
		syslog.L.Error(err).WithMessage("unable to get job").Write()
		return err
	}

	latestJob.CurrentPID = job.CurrentPID
	latestJob.LastRunUpid = taskFound.UPID
	latestJob.LastRunState = &taskFound.Status
	latestJob.LastRunEndtime = &taskFound.EndTime
	latestJob.LastRunPlusTime = 0
	latestJob.LastRunPlusError = ""

	if succeeded {
		latestJob.LastSuccessfulUpid = taskFound.UPID
		latestJob.LastSuccessfulEndtime = &task.EndTime
	}

	if err := storeInstance.Database.UpdateJob(*latestJob); err != nil {
		syslog.L.Error(err).WithMessage("unable to update job").Write()
		return err
	}

	return nil
}
