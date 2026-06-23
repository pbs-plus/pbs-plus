//go:build linux

package verification

import (
	"fmt"

	"github.com/pbs-plus/pbs-plus/internal/server/database"
	"github.com/pbs-plus/pbs-plus/internal/server/proxmox"
	"github.com/pbs-plus/pbs-plus/internal/server/tasks"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

// VerificationTask manages verification job logging and lifecycle.
type VerificationTask struct {
	tasks.BaseTask
	job database.VerificationJob
}

// NewVerificationTask creates a new verification task with log file setup.
func NewVerificationTask(job database.VerificationJob) (*VerificationTask, error) {
	wid := proxmox.EncodeToHexEscapes(job.ID)
	task := tasks.NewTask("pbsplus", "verification", wid)

	file, _, err := tasks.CreateTaskLogFile(task.UPID)
	if err != nil {
		return nil, err
	}

	vTask := &VerificationTask{
		BaseTask: tasks.NewBaseTask(task, file),
		job:      job,
	}
	if err := tasks.AddActive(task.UPID); err != nil {
		syslog.L.Error(err).WithField("upid", task.UPID).WithMessage("failed to register active verification task").Write()
	}
	return vTask, nil
}

// WriteString delegates to BaseTask.WriteString.
func (t *VerificationTask) WriteString(data string) {
	t.BaseTask.WriteString(data)
}

// CloseOK closes the task with "OK" status.
func (t *VerificationTask) CloseOK() {
	t.CloseWithStatus("OK", nil, func() {
		_ = tasks.RemoveActive(t.UPID)
	})
}

// CloseErr closes the task with "TASK ERROR: <msg>" status.
func (t *VerificationTask) CloseErr(taskErr error) {
	status := "ERROR: " + taskErr.Error()
	t.CloseWithStatus(status, nil, func() {
		_ = tasks.RemoveActive(t.UPID)
	})
}

// CloseWarn closes the task with warnings count.
// Matches the PBS pattern: final line is "TASK WARNINGS: N".
func (t *VerificationTask) CloseWarn(warnings int) {
	status := fmt.Sprintf("WARNINGS: %d", warnings)
	t.CloseWithStatus(status, nil, func() {
		_ = tasks.RemoveActive(t.UPID)
	})
}
