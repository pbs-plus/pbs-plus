//go:build linux

package log

import (
	"github.com/pbs-plus/pbs-plus/internal/proxmox"
	"github.com/pbs-plus/pbs-plus/internal/proxmox/tasklog"
)

type TaskLogger struct {
	task  *tasklog.WorkerTask
	jobID string
}

func ForTask(wt *tasklog.WorkerTask, jobID string) *TaskLogger {
	return &TaskLogger{task: wt, jobID: jobID}
}

func (l *TaskLogger) Info(msg string, args ...any) {
	L.newEntry("info", nil, msg, args...).WithJob(l.jobID).write()
	if l.task != nil {
		l.task.LogString(msg)
	}
}

func (l *TaskLogger) Error(err error, msg string, args ...any) {
	L.newEntry("error", err, msg, args...).WithJob(l.jobID).write()
	if l.task != nil {
		l.task.LogString(msg)
		if err != nil {
			l.task.LogString(err.Error())
		}
	}
}

func (l *TaskLogger) Warn(msg string, args ...any) {
	L.newEntry("warn", nil, msg, args...).WithJob(l.jobID).write()
	if l.task != nil {
		l.task.LogString(msg)
	}
}

func (l *TaskLogger) Log(format string, args ...any) {
	if l.task != nil {
		l.task.Log(format, args...)
	}
}

func (l *TaskLogger) LogString(data string) {
	if l.task != nil {
		l.task.LogString(data)
	}
}

func (l *TaskLogger) CloseOK() {
	if l.task != nil {
		l.task.CloseOK()
	}
}

func (l *TaskLogger) CloseErr(err error) {
	if l.task != nil {
		l.task.CloseErr(err)
	}
}

func (l *TaskLogger) CloseWarn(count uint64) {
	if l.task != nil {
		l.task.CloseWarn(count)
	}
}

func (l *TaskLogger) CloseWithStatus(state tasklog.TaskState) {
	if l.task != nil {
		l.task.CloseWithStatus(state)
	}
}

func (l *TaskLogger) UPID() string {
	if l.task != nil {
		return l.task.UPID()
	}
	return ""
}

func (l *TaskLogger) Task() proxmox.Task {
	if l.task != nil {
		return l.task.Task
	}
	return proxmox.Task{}
}

func (l *TaskLogger) RequestAbort() {
	if l.task != nil {
		l.task.RequestAbort()
	}
}

func (l *TaskLogger) AbortRequested() bool {
	if l.task != nil {
		return l.task.AbortRequested()
	}
	return false
}
