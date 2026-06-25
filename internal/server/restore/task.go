//go:build linux

package restore

import (
	"fmt"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/proxmox"
	"github.com/pbs-plus/pbs-plus/internal/proxmox/tasklog"
	"github.com/pbs-plus/pbs-plus/internal/server/database"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
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
	t.CloseWithStatus(tasklog.TaskState{Status: tasklog.StatusOK, EndTime: time.Now().Unix()}, func() {
		if err := tasklog.RemoveActive(t.UPID()); err != nil {
			syslog.L.Error(err).Write()
		}
	})
}

func (t *RestoreTask) CloseErr(taskErr error) {
	t.CloseWithStatus(tasklog.TaskState{Status: tasklog.StatusError, EndTime: time.Now().Unix(), Message: taskErr.Error()}, func() {
		if err := tasklog.RemoveActive(t.UPID()); err != nil {
			syslog.L.Error(err).Write()
		}
	})
}

func (t *RestoreTask) CloseWarn(warning int) {
	t.CloseWithStatus(tasklog.TaskState{Status: tasklog.StatusWarning, EndTime: time.Now().Unix(), WarnCount: uint64(warning)}, func() {
		if err := tasklog.RemoveActive(t.UPID()); err != nil {
			syslog.L.Error(err).Write()
		}
	})
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

	wt.CloseWithStatus(tasklog.TaskState{Status: tasklog.StatusOK, EndTime: time.Now().Unix()}, func() {
		if err := tasklog.RemoveActive(wt.UPID()); err != nil {
			syslog.L.Error(err).Write()
		}
	})

	return wt.Task, nil
}
