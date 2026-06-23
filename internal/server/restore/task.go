//go:build linux

package restore

import (
	"fmt"

	"github.com/pbs-plus/pbs-plus/internal/proxmox"
	"github.com/pbs-plus/pbs-plus/internal/server/database"
	"github.com/pbs-plus/pbs-plus/internal/server/tasks"
)

type RestoreTask struct {
	tasks.BaseTask
	restore database.Restore
}

func GetRestoreTask(job database.Restore) (*RestoreTask, error) {
	targetName := job.DestTarget.GetHostname()
	wid := fmt.Sprintf("%s%shost-%s", proxmox.EncodeToHexEscapes(job.Store), proxmox.EncodeToHexEscapes(":"), proxmox.EncodeToHexEscapes(targetName))
	task := tasks.NewTask("pbsplus", "reader", wid)

	file, _, err := tasks.CreateTaskLogFile(task.UPID)
	if err != nil {
		return nil, err
	}

	rTask := &RestoreTask{
		BaseTask: tasks.NewBaseTask(task, file),
		restore:  job,
	}
	_ = tasks.AddActive(task.UPID)
	return rTask, nil
}

func (t *RestoreTask) WriteString(data string) {
	t.BaseTask.WriteString(data)
}

func (t *RestoreTask) CloseOK() {
	t.CloseWithStatus("OK", nil, func() {
		_ = tasks.RemoveActive(t.UPID)
	})
}

func (t *RestoreTask) CloseErr(taskErr error) {
	errMsg := taskErr.Error()
	t.CloseWithStatus(errMsg, nil, func() {
		_ = tasks.RemoveActive(t.UPID)
	})
}

func (t *RestoreTask) CloseWarn(warning int) {
	t.CloseWithStatus("OK", nil, func() {
		_ = tasks.RemoveActive(t.UPID)
	})
}

func GenerateRestoreTaskOKFile(job database.Restore, additionalData []string) (proxmox.Task, error) {
	targetName := job.DestTarget.GetHostname()
	wid := fmt.Sprintf("%s%shost-%s", proxmox.EncodeToHexEscapes(job.Store), proxmox.EncodeToHexEscapes(":"), proxmox.EncodeToHexEscapes(targetName))
	task := tasks.NewTask("pbsplusgen-ok", "reader", wid)

	file, _, err := tasks.CreateTaskLogFile(task.UPID)
	if err != nil {
		return proxmox.Task{}, err
	}
	defer file.Close()

	base := tasks.NewBaseTask(task, file)
	for _, data := range additionalData {
		base.WriteLogLine("%s", data)
	}
	base.WriteLogLine("TASK OK")

	tasks.WriteArchive(task.UPID, task.StartTime, "OK")
	task.Status = "stopped"
	task.ExitStatus = "OK"
	task.EndTime = tasks.Now().Unix()
	return task, nil
}
