//go:build linux

package jobs

import (
	"errors"
	"fmt"
	"sync"

	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/proxmox"
	"github.com/pbs-plus/pbs-plus/internal/proxmox/tasklog"
	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
)

var statusMutexes sync.Map

// finalizedRuns maps jobID to the execution that last terminal-wrote its history, so late queued placeholders for that execution get dropped.
var finalizedRuns sync.Map

// ErrRunFinalized reports a queued placeholder write dropped because its
// execution already recorded a terminal history entry.
var ErrRunFinalized = errors.New("job run already finalized")

// RunFinalized reports whether executionID already wrote jobID's terminal
// history entry, making a placeholder write for it a stale race.
func RunFinalized(jobID, executionID string) bool {
	if jobID == "" || executionID == "" {
		return false
	}
	v, ok := finalizedRuns.Load(jobID)
	return ok && v.(string) == executionID
}

// MarkRunFinalized records executionID as jobID's terminal history writer; terminal writers that bypass UpdateJobHistory must call it.
func MarkRunFinalized(jobID, executionID string) {
	if jobID == "" || executionID == "" {
		return
	}
	value, _ := statusMutexes.LoadOrStore(jobID, &sync.Mutex{})
	mu := value.(*sync.Mutex)
	mu.Lock()
	defer mu.Unlock()
	finalizedRuns.Store(jobID, executionID)
}

func markRunFinalizedLocked(jobID, executionID string) {
	if jobID != "" && executionID != "" {
		finalizedRuns.Store(jobID, executionID)
	}
}

func UpdateJobHistory(
	jobID string,
	executionID string,
	currentPID int,
	succeeded bool,
	warningsNum int,
	task proxmox.Task,
	getHistory func() (coredb.JobHistory, int, error),
	updateHistory func(coredb.JobHistory, int) error,
) error {
	value, _ := statusMutexes.LoadOrStore(jobID, &sync.Mutex{})
	mu := value.(*sync.Mutex)

	mu.Lock()
	defer mu.Unlock()

	if executionID != "" && tasklog.IsQueuedUPID(task.UPID) && RunFinalized(jobID, executionID) {
		return ErrRunFinalized
	}

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
	if taskFound.Status == "running" {
		history.LastRunStatus = coredb.JobStatusUnknown
		return updateHistory(history, currentPID)
	}

	// Determine the typed status and update retry count
	if warningsNum > 0 && succeeded {
		history.LastRunState = fmt.Sprintf("WARNINGS: %d", warningsNum)
		history.LastRunStatus = coredb.JobStatusWarnings
		history.RetryCount = 0
		history.LastSuccessfulUpid = taskFound.UPID
		history.LastSuccessfulEndtime = task.EndTime
	} else if succeeded {
		history.LastRunStatus = coredb.JobStatusSuccess
		history.RetryCount = 0
		history.LastSuccessfulUpid = taskFound.UPID
		history.LastSuccessfulEndtime = task.EndTime
	} else if taskFound.ExitStatus == "operation canceled" {
		// Manual cancellation - not a failure, don't increment retry count
		history.LastRunStatus = coredb.JobStatusCanceled
	} else {
		history.LastRunStatus = coredb.JobStatusFailed
		history.RetryCount++
	}

	if err := updateHistory(history, currentPID); err != nil {
		return err
	}

	markRunFinalizedLocked(jobID, executionID)
	return nil
}
