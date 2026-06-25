//go:build linux

package backup

import (
	"github.com/pbs-plus/pbs-plus/internal/proxmox/tasklog"
)

func getTaskLogPath(upid string) string {
	path, err := tasklog.UPIDLogPath(upid)
	if err != nil {
		return ""
	}
	return path
}
