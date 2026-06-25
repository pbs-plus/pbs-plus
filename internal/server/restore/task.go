//go:build linux

package restore

import (
	"fmt"

	"github.com/pbs-plus/pbs-plus/internal/proxmox"
	"github.com/pbs-plus/pbs-plus/internal/proxmox/tasklog"
	"github.com/pbs-plus/pbs-plus/internal/server/database"
)

type RestoreTask struct {
	*tasklog.WorkerTask
	restore database.Restore
}

func GetRestoreTask(job database.Restore) (*RestoreTask, error) {
	targetName := job.DestTarget.GetHostname()
	wid := fmt.Sprintf("%s%shost-%s", proxmox.EncodeToHexEscapes(job.Store), proxmox.EncodeToHexEscapes(":"), proxmox.EncodeToHexEscapes(targetName))
	wt, err := tasklog.NewWorkerTask("pbsplus", "reader", wid)
	if err != nil {
		return nil, err
	}

	return &RestoreTask{
		WorkerTask: wt,
		restore:    job,
	}, nil
}

func (t *RestoreTask) WriteString(data string) {
	t.LogString(data)
}

func (t *RestoreTask) CloseOK() {
	t.WorkerTask.CloseOK()
}

func (t *RestoreTask) CloseErr(taskErr error) {
	t.WorkerTask.CloseErr(taskErr)
}

func (t *RestoreTask) CloseWarn(warning int) {
	t.WorkerTask.CloseWarn(uint64(warning))
}

func GenerateRestoreTaskOKFile(job database.Restore, additionalData []string) (proxmox.Task, error) {
	targetName := job.DestTarget.GetHostname()
	wid := fmt.Sprintf("%s%shost-%s", proxmox.EncodeToHexEscapes(job.Store), proxmox.EncodeToHexEscapes(":"), proxmox.EncodeToHexEscapes(targetName))

	wt, err := tasklog.NewWorkerTask("pbsplusgen-ok", "reader", wid)
	if err != nil {
		return proxmox.Task{}, err
	}

	for _, data := range additionalData {
		wt.LogString(data)
	}

	wt.CloseOK()

	return wt.Task, nil
}
