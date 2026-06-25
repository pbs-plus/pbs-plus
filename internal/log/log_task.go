//go:build linux

package log

import (
	"github.com/pbs-plus/pbs-plus/internal/proxmox"
	"github.com/pbs-plus/pbs-plus/internal/proxmox/tasklog"
)

func (l *Logger) CloseWithStatus(state tasklog.TaskState) {
	if wt, ok := l.task.(*tasklog.WorkerTask); ok {
		wt.CloseWithStatus(state)
	}
}

func (l *Logger) Task() proxmox.Task {
	if wt, ok := l.task.(*tasklog.WorkerTask); ok {
		return wt.Task
	}
	return proxmox.Task{}
}
