//go:build linux

package backup

import (
	"github.com/sonroyaalmerol/pbs-plus/internal/store"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
)

func updateJobStatus(job *store.Job, task *store.Task, storeInstance *store.Store) error {
	// Update task status
	taskFound, err := storeInstance.GetTaskByUPID(task.UPID)
	if err != nil {
		syslog.L.Errorf("Unable to get task by UPID: %v", err)
		return err
	}

	// Update job status
	latestJob, err := storeInstance.GetJob(job.ID)
	if err != nil {
		syslog.L.Errorf("Unable to get job: %v", err)
		return err
	}

	latestJob.LastRunUpid = &taskFound.UPID
	latestJob.LastRunState = &taskFound.Status
	latestJob.LastRunEndtime = &taskFound.EndTime

	if err := storeInstance.UpdateJob(*latestJob); err != nil {
		syslog.L.Errorf("Unable to update job: %v", err)
		return err
	}

	return nil
}

