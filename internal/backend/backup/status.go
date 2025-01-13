//go:build linux

package backup

import (
	"fmt"

	"github.com/sonroyaalmerol/pbs-plus/internal/store"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
)

func updateJobStatus(job *store.Job, task *store.Task, storeInstance *store.Store) error {
	syslogger, err := syslog.InitializeLogger()
	if err != nil {
		return fmt.Errorf("failed to initialize logger: %w", err)
	}

	// Update task status
	taskFound, err := storeInstance.GetTaskByUPID(task.UPID)
	if err != nil {
		syslogger.Errorf("Unable to get task by UPID: %v", err)
		return err
	}

	// Update job status
	latestJob, err := storeInstance.GetJob(job.ID)
	if err != nil {
		syslogger.Errorf("Unable to get job: %v", err)
		return err
	}

	latestJob.LastRunUpid = &taskFound.UPID
	latestJob.LastRunState = &taskFound.Status
	latestJob.LastRunEndtime = &taskFound.EndTime

	if err := storeInstance.UpdateJob(*latestJob); err != nil {
		syslogger.Errorf("Unable to update job: %v", err)
		return err
	}

	return nil
}
