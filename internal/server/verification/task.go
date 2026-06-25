//go:build linux

package verification

import (
	"github.com/pbs-plus/pbs-plus/internal/proxmox"
	"github.com/pbs-plus/pbs-plus/internal/proxmox/tasklog"
	"github.com/pbs-plus/pbs-plus/internal/server/database"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

type VerificationTask struct {
	*tasklog.WorkerTask
	job database.VerificationJob
}

func NewVerificationTask(job database.VerificationJob) (*VerificationTask, error) {
	wid := proxmox.EncodeToHexEscapes(job.ID)
	wt, err := tasklog.NewWorkerTask("pbsplus", "verification", wid)
	if err != nil {
		return nil, err
	}

	return &VerificationTask{
		WorkerTask: wt,
		job:        job,
	}, nil
}

func (t *VerificationTask) WriteString(data string) {
	t.LogString(data)
}

func (t *VerificationTask) CloseOK() {
	t.CloseWithStatus(tasklog.TaskState{Status: tasklog.StatusOK, EndTime: t.Task.StartTime}, func() {
		if err := tasklog.RemoveActive(t.UPID()); err != nil {
			syslog.L.Error(err).Write()
		}
	})
}

func (t *VerificationTask) CloseErr(taskErr error) {
	t.CloseWithStatus(tasklog.TaskState{Status: tasklog.StatusError, EndTime: t.Task.StartTime, Message: taskErr.Error()}, func() {
		if err := tasklog.RemoveActive(t.UPID()); err != nil {
			syslog.L.Error(err).Write()
		}
	})
}

func (t *VerificationTask) CloseWarn(warnings int) {
	t.CloseWithStatus(tasklog.TaskState{Status: tasklog.StatusWarning, EndTime: t.Task.StartTime, WarnCount: uint64(warnings)}, func() {
		if err := tasklog.RemoveActive(t.UPID()); err != nil {
			syslog.L.Error(err).Write()
		}
	})
}
