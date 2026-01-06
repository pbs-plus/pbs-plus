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

var jobMutexes sync.Map

func updateJobStatus(succeeded bool, warningsNum int, job types.Job, task proxmox.Task, storeInstance *store.Store) error {
	value, _ := jobMutexes.LoadOrStore(job.ID, &sync.Mutex{})
	mu := value.(*sync.Mutex)

	mu.Lock()
	defer mu.Unlock()

	taskFound, err := proxmox.GetTaskByUPID(task.UPID)
	if err != nil {
		syslog.L.Error(err).WithMessage("unable to get task by upid").Write()
		return err
	}

	latestJob, err := storeInstance.Database.GetJob(job.ID)
	if err != nil {
		syslog.L.Error(err).WithMessage("unable to get job").Write()
		return err
	}

	latestJob.CurrentPID = job.CurrentPID
	latestJob.LastRunUpid = taskFound.UPID
	latestJob.LastRunState = taskFound.Status
	latestJob.LastRunEndtime = taskFound.EndTime

	if warningsNum > 0 && succeeded {
		latestJob.LastRunState = fmt.Sprintf("WARNINGS: %d", warningsNum)
	}

	if succeeded {
		latestJob.LastSuccessfulUpid = taskFound.UPID
		latestJob.LastSuccessfulEndtime = task.EndTime
	}

	if err := storeInstance.Database.UpdateJob(nil, latestJob); err != nil {
		return err
	}

	return nil
}
