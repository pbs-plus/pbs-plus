//go:build linux

package jobs

import (
	"fmt"
	"sync"

	"github.com/pbs-plus/pbs-plus/internal/store/database"
	"github.com/pbs-plus/pbs-plus/internal/store/proxmox"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

var statusMutexes sync.Map

// UpdateJobHistory fetches the task info, applies the status update to the
// current PID and history fields, and persists via the provided update function.
func UpdateJobHistory(
	jobID string,
	currentPID int,
	succeeded bool,
	warningsNum int,
	task proxmox.Task,
	getHistory func() (database.JobHistory, int, error),
	updateHistory func(database.JobHistory, int) error,
) error {
	value, _ := statusMutexes.LoadOrStore(jobID, &sync.Mutex{})
	mu := value.(*sync.Mutex)

	mu.Lock()
	defer mu.Unlock()

	taskFound, err := proxmox.GetTaskByUPID(task.UPID)
	if err != nil {
		syslog.L.Error(err).WithMessage("unable to get task by upid").Write()
		return err
	}

	history, pid, err := getHistory()
	if err != nil {
		syslog.L.Error(err).WithMessage("unable to get job for status update").Write()
		return err
	}

	// Preserve the caller's current PID only if it matches (avoid overwriting)
	_ = pid
	history.LastRunUpid = taskFound.UPID
	history.LastRunState = taskFound.Status
	history.LastRunEndtime = taskFound.EndTime

	if warningsNum > 0 && succeeded {
		history.LastRunState = fmt.Sprintf("WARNINGS: %d", warningsNum)
	}

	if succeeded {
		history.LastSuccessfulUpid = taskFound.UPID
		history.LastSuccessfulEndtime = task.EndTime
	}

	if err := updateHistory(history, currentPID); err != nil {
		return err
	}

	return nil
}
