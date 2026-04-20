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
// It also updates the typed LastRunStatus and manages the RetryCount.
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
	history.LastRunState = taskFound.ExitStatus
	history.LastRunEndtime = taskFound.EndTime

	// Determine the typed status and update retry count
	if warningsNum > 0 && succeeded {
		// Success with warnings
		history.LastRunState = fmt.Sprintf("WARNINGS: %d", warningsNum)
		history.LastRunStatus = database.JobStatusWarnings
		history.RetryCount = 0 // Reset retry count on success
	} else if succeeded {
		// Clean success
		history.LastRunStatus = database.JobStatusSuccess
		history.RetryCount = 0 // Reset retry count on success
		history.LastSuccessfulUpid = taskFound.UPID
		history.LastSuccessfulEndtime = task.EndTime
	} else if taskFound.ExitStatus == "operation canceled" {
		// Manual cancellation - not a failure, don't increment retry count
		history.LastRunStatus = database.JobStatusCanceled
	} else {
		// Actual failure - increment retry count
		history.LastRunStatus = database.JobStatusFailed
		history.RetryCount++
	}

	if err := updateHistory(history, currentPID); err != nil {
		return err
	}

	return nil
}
