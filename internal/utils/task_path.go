package utils

import (
	"fmt"
	"strings"
)

func GetTaskLogPath(upid string) string {
	upidSplit := strings.Split(upid, ":")
	if len(upidSplit) < 4 {
		return ""
	}
	parsed := upidSplit[3]
	logFolder := parsed[len(parsed)-2:]
	logFilePath := fmt.Sprintf("/var/log/proxmox-backup/tasks/%s/%s", logFolder, upid)

	return logFilePath
}
