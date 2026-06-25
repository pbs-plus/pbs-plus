//go:build linux

package jobs

import (
	"fmt"
	"sync"

	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/proxmox"
	"github.com/pbs-plus/pbs-plus/internal/proxmox/tasklog"
	"github.com/pbs-plus/pbs-plus/internal/server/database"
)

var statusMutexes sync.Map

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

	taskFound, err := tasklog.GetTaskByUPID(task.UPID)
	if err != nil {
		log.Error(err, "unable to get task by upid")
		return err
	}

	history, pid, err := getHistory()
	if err != nil {
		log.Error(err, "unable to get job for status update")
		return err
	}

	// Preserve the caller's current PID only if it matches (avoid overwriting)
	_ = pid
	history.LastRunUpid = taskFound.UPID
	history.LastRunState = taskFound.ExitStatus
	history.LastRunEndtime = taskFound.EndTime

	// Determine the typed status and update retry count
	if warningsNum > 0 && succeeded {
		history.LastRunState = fmt.Sprintf("WARNINGS: %d", warningsNum)
		history.LastRunStatus = database.JobStatusWarnings
		history.RetryCount = 0
		history.LastSuccessfulUpid = taskFound.UPID
		history.LastSuccessfulEndtime = task.EndTime
	} else if succeeded {
		history.LastRunStatus = database.JobStatusSuccess
		history.RetryCount = 0
		history.LastSuccessfulUpid = taskFound.UPID
		history.LastSuccessfulEndtime = task.EndTime
	} else if taskFound.ExitStatus == "operation canceled" {
		// Manual cancellation - not a failure, don't increment retry count
		history.LastRunStatus = database.JobStatusCanceled
	} else {
		history.LastRunStatus = database.JobStatusFailed
		history.RetryCount++
	}

	if err := updateHistory(history, currentPID); err != nil {
		return err
	}

	return nil
}
