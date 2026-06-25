//go:build linux

package tasklog

import (
	"github.com/pbs-plus/pbs-plus/internal/proxmox"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

type TaskLogger struct {
	task   *WorkerTask
	queued *QueuedTask
	jobID  string
}

func ForTask(wt *WorkerTask, jobID string) *TaskLogger {
	return &TaskLogger{task: wt, jobID: jobID}
}

func ForQueued(qt *QueuedTask, jobID string) *TaskLogger {
	return &TaskLogger{queued: qt, jobID: jobID}
}

func (l *TaskLogger) Info(msg string) {
	if l.jobID != "" {
		syslog.L.Info().WithJob(l.jobID).WithMessage(msg).Write()
	} else {
		syslog.L.Info().WithMessage(msg).Write()
	}
	if l.task != nil {
		l.task.LogString(msg)
	}
}

func (l *TaskLogger) Error(err error, msg string) {
	if l.jobID != "" {
		syslog.L.Error(err).WithJob(l.jobID).WithMessage(msg).Write()
	} else {
		syslog.L.Error(err).WithMessage(msg).Write()
	}
	if l.task != nil {
		l.task.LogString(msg)
		if err != nil {
			l.task.LogString(err.Error())
		}
	}
}

func (l *TaskLogger) Warn(msg string) {
	if l.jobID != "" {
		syslog.L.Warn().WithJob(l.jobID).WithMessage(msg).Write()
	} else {
		syslog.L.Warn().WithMessage(msg).Write()
	}
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

func (l *TaskLogger) UpdateDescription(desc string) error {
	if l.queued != nil {
		if err := l.queued.UpdateDescription(desc); err != nil {
			return err
		}
	}
	if l.jobID != "" {
		syslog.L.Info().WithJob(l.jobID).WithMessage(desc).Write()
	} else {
		syslog.L.Info().WithMessage(desc).Write()
	}
	return nil
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

func (l *TaskLogger) CloseWithStatus(state TaskState) {
	if l.task != nil {
		l.task.CloseWithStatus(state)
	}
}

func (l *TaskLogger) CloseQueued() {
	if l.queued != nil {
		l.queued.Close()
	}
}

func (l *TaskLogger) UPID() string {
	if l.task != nil {
		return l.task.UPID()
	}
	if l.queued != nil {
		return l.queued.Task.UPID
	}
	return ""
}

func (l *TaskLogger) Task() proxmox.Task {
	if l.task != nil {
		return l.task.Task
	}
	if l.queued != nil {
		return l.queued.Task
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
