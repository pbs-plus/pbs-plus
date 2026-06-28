//go:build linux

package verification

import (
	"github.com/pbs-plus/pbs-plus/internal/proxmox"
	"github.com/pbs-plus/pbs-plus/internal/proxmox/tasklog"
	"github.com/pbs-plus/pbs-plus/internal/server/database"
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
	t.WorkerTask.CloseOK()
}

func (t *VerificationTask) CloseErr(taskErr error) {
	t.WorkerTask.CloseErr(taskErr)
}

func (t *VerificationTask) CloseWarn(warnings int) {
	t.WorkerTask.CloseWarn(uint64(warnings))
}
