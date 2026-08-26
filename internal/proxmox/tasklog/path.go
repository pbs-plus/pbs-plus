//go:build linux

package tasklog

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/pbs-plus/pbs-plus/internal/proxmox"
)

func UPIDLogPath(upid string) (string, error) {
	parsed, err := proxmox.ParseUPID(upid)
	if err != nil {
		return "", fmt.Errorf("tasklog: invalid upid: %w", err)
	}
	logFolder := fmt.Sprintf("%02X", parsed.PStart&0xFF)
	return filepath.Join(taskDir, logFolder, upid), nil
}

func CreateTaskLogFile(upid string) (*os.File, string, error) {
	path, err := UPIDLogPath(upid)
	if err != nil {
		return nil, "", err
	}

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, "", fmt.Errorf("tasklog: create task log dir: %w", err)
	}
	if err := proxmox.ChownBackupUser(dir); err != nil {
		return nil, "", err
	}

	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0660)
	if err != nil {
		return nil, "", err
	}

	if err := file.Chown(proxmox.BackupUID, proxmox.BackupGID); err != nil && os.Geteuid() == 0 {
		if cerr := file.Close(); cerr != nil {
			slog.Error(cerr.Error())
		}
		return nil, "", err
	}

	return file, path, nil
}
