//go:build linux

package log

import (
	"github.com/pbs-plus/pbs-plus/internal/proxmox/tasklog"
)

func (l *Logger) CloseWithStatus(state tasklog.TaskState) {
	if wt, ok := l.task.(*tasklog.WorkerTask); ok {
		wt.CloseWithStatus(state)
	}
}
