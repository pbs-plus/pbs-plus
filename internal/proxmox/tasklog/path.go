//go:build linux

package tasklog

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/pbs-plus/pbs-plus/internal/conf"
	"github.com/pbs-plus/pbs-plus/internal/proxmox"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

func UPIDLogPath(upid string) (string, error) {
	parsed, err := proxmox.ParseUPID(upid)
	if err != nil {
		return "", fmt.Errorf("tasklog: invalid upid: %w", err)
	}
	logFolder := fmt.Sprintf("%02X", parsed.PStart&0xFF)
	return filepath.Join(conf.TaskLogsBasePath, logFolder, upid), nil
}

func CreateTaskLogFile(upid string) (*os.File, string, error) {
	path, err := UPIDLogPath(upid)
	if err != nil {
		return nil, "", err
	}

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		syslog.L.Error(err).Write()
	}
	if err := os.Chown(dir, 34, 34); err != nil {
		syslog.L.Error(err).Write()
	}

	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0660)
	if err != nil {
		return nil, "", err
	}

	if err := file.Chown(34, 34); err != nil {
		if cerr := file.Close(); cerr != nil {
			syslog.L.Error(cerr).Write()
		}
		return nil, "", err
	}

	return file, path, nil
}
