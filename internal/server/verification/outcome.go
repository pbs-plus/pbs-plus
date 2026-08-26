//go:build linux

package verification

import (
	"fmt"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/proxmox"
	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
	"github.com/pbs-plus/pbs-plus/internal/server/jobs"
	"github.com/pbs-plus/pbs-plus/internal/server/notification"
)

func (v *verificationJob) finalizeFailure(err error) {
	v.logger.Error(err, "verification failed")

	v.mu.RLock()
	t := v.task
	rID := v.resultID
	v.mu.RUnlock()

	if rID > 0 {
		if markErr := v.app.CoreDB.MarkVerificationResultStatus(rID, "failed", time.Now().Unix()); markErr != nil {
			v.logger.Error(markErr, "failed to mark result as failed")
		}
	}

	if t != nil {
		t.WriteString(fmt.Sprintf("verification job error: %v", err))
		t.WriteString(fmt.Sprintf("End Time: %s", time.Now().Format("Mon Jan 2 15:04:05 2006")))
		t.CloseErr(err)
	}

	if err := v.updateJobHistory(false, 0); err != nil {
		v.logger.Error(err, "failed to update job history on error")
	}

	if v.app.BatchTracker != nil {
		v.app.BatchTracker.RecordJobResult(
			v.job.NotificationMode,
			notification.JobTypeVerification,
			v.job.ID,
			v.job.Store,
			fmt.Errorf("verification failed: %w", err),
			map[string]string{
				"namespace": v.job.Namespace,
				"succeeded": "false",
			},
		)
	}
}

func (v *verificationJob) finalizeSuccess() {
	v.logger.Info("verification completed", "total_files", v.totalFiles, "failed_files", v.failedFiles, "skipped_files", v.skippedFiles)

	v.mu.RLock()
	t := v.task
	failed := v.failedFiles
	skipped := v.skippedFiles
	total := v.totalFiles
	v.mu.RUnlock()

	if t != nil {
		verified := total - failed - skipped
		t.WriteString("Verification job summary:")
		t.WriteString(fmt.Sprintf("  total files sampled: %d", total))
		t.WriteString(fmt.Sprintf("  verified: %d", verified))
		t.WriteString(fmt.Sprintf("  failed: %d", failed))
		t.WriteString(fmt.Sprintf("  skipped: %d", skipped))
		t.WriteString(fmt.Sprintf("End Time: %s", time.Now().Format("Mon Jan 2 15:04:05 2006")))

		if failed > 0 {
			t.CloseWarn(failed)
			if err := v.updateJobHistory(true, failed); err != nil {
				v.logger.Error(err, "failed to update job history")
			}
		} else if skipped > 0 {
			t.CloseWarn(skipped)
			if err := v.updateJobHistory(true, skipped); err != nil {
				v.logger.Error(err, "failed to update job history")
			}
		} else {
			t.CloseOK()
		}
	}

	if err := v.updateJobHistory(true, 0); err != nil {
		v.logger.Error(err, "failed to update job history on success")
	}

	var notifyErr error
	if failed > 0 {
		notifyErr = fmt.Errorf("verification found %d failed files", failed)
	}
	if v.app.BatchTracker != nil {
		verified := total - failed - skipped
		v.app.BatchTracker.RecordJobResult(
			v.job.NotificationMode,
			notification.JobTypeVerification,
			v.job.ID,
			v.job.Store,
			notifyErr,
			map[string]string{
				"namespace": v.job.Namespace,
				"total":     fmt.Sprintf("%d", total),
				"verified":  fmt.Sprintf("%d", verified),
				"failed":    fmt.Sprintf("%d", failed),
				"skipped":   fmt.Sprintf("%d", skipped),
				"succeeded": fmt.Sprintf("%v", failed == 0),
			},
		)
	}
}

func (v *verificationJob) cleanup() {
	if v.cancel != nil {
		v.cancel()
	}
}

func (v *verificationJob) updateJobStatus(succeeded bool, task proxmox.Task) error {
	job, err := v.app.CoreDB.GetVerificationJob(v.job.ID)
	if err != nil {
		return err
	}
	job.History.LastRunUpid = task.UPID
	job.History.LastRunStarttime = task.StartTime
	job.History.LastRunEndtime = task.EndTime
	job.History.LastRunState = task.Status
	if succeeded {
		job.History.LastSuccessfulUpid = task.UPID
	}
	return v.app.CoreDB.UpdateVerificationJob(nil, job)
}

// using the standard PBS task system (mirrors backup/restore pattern).

// using the standard PBS task system (mirrors backup/restore pattern).
func (v *verificationJob) updateJobHistory(succeeded bool, warningsNum int) error {
	v.mu.RLock()
	vTask := v.task
	v.mu.RUnlock()

	if vTask == nil {
		return nil
	}

	return jobs.UpdateJobHistory(
		v.job.ID,
		0,
		succeeded,
		warningsNum,
		vTask.Task,
		func() (coredb.JobHistory, int, error) {
			j, err := v.app.CoreDB.GetVerificationJob(v.job.ID)
			return j.History, 0, err
		},
		func(history coredb.JobHistory, _ int) error {
			j, err := v.app.CoreDB.GetVerificationJob(v.job.ID)
			if err != nil {
				return err
			}
			j.History = history
			return v.app.CoreDB.UpdateVerificationJob(nil, j)
		},
	)
}
