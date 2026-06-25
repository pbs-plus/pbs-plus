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
	task := tasklog.NewTask("pbsplusgen-ok", "reader", wid)

	file, _, err := tasklog.CreateTaskLogFile(task.UPID)
	if err != nil {
		return proxmox.Task{}, err
	}
	defer func() {
		if cerr := file.Close(); cerr != nil {
			syslog.L.Error(cerr).Write()
		}
	}()

	timestamp := time.Now().Format(time.RFC3339)
	for _, data := range additionalData {
		if _, err := fmt.Fprintf(file, "%s: %s\n", timestamp, data); err != nil {
			syslog.L.Error(err).Write()
		}
	}
	if _, err := fmt.Fprintf(file, "%s: TASK OK\n", timestamp); err != nil {
		syslog.L.Error(err).Write()
	}

	if err := tasklog.WriteArchive(task.UPID, tasklog.TaskState{Status: tasklog.StatusOK, EndTime: time.Now().Unix()}); err != nil {
		syslog.L.Error(err).Write()
	}

	task.Status = "stopped"
	task.ExitStatus = "OK"
	task.EndTime = time.Now().Unix()
	return task, nil
}
