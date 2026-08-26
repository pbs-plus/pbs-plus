//go:build linux

package tasklog

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

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

// ChangeUPIDStartTime rewrites a task's starttime, renames its log file
// to the new UPID's path, and leaves a symlink at the old path.
func ChangeUPIDStartTime(upid string, startTime time.Time) (string, error) {
	parsedTask, err := proxmox.ParseUPID(upid)
	if err != nil {
		return "", err
	}
	path, err := UPIDLogPath(upid)
	if err != nil {
		return "", err
	}

	parsedTask.StartTime = startTime.Unix()
	newUpid := parsedTask.GenerateUPID()
	newPath, err := UPIDLogPath(newUpid)
	if err != nil {
		return "", err
	}

	if oldInfo, err := os.Stat(path); err == nil {
		if newInfo, newErr := os.Stat(newPath); newErr == nil && os.SameFile(oldInfo, newInfo) {
			if err := Reconcile(""); err != nil {
				return "", err
			}
			return newUpid, nil
		}
	}

	if err := os.Rename(path, newPath); err != nil {
		return "", err
	}
	slog.Info("updated UPID start time")

	if err := os.Symlink(newPath, path); err != nil {
		slog.Error(err.Error())
	}

	if err := replaceActiveUPID(upid, newUpid, parsedTask); err != nil {
		return "", err
	}
	if err := Reconcile(""); err != nil {
		return "", err
	}

	return newUpid, nil
}

func replaceActiveUPID(oldUPID, newUPID string, task proxmox.Task) error {
	lock, err := lockTaskList(true)
	if err != nil {
		return err
	}
	defer lock.Close()

	active, err := readTaskFile(activeTasks)
	if err != nil {
		return err
	}
	for i := range active {
		if active[i].UPID == oldUPID {
			active[i].UPID = newUPID
			active[i].Task = task
			return replaceFile(activeTasks, renderTaskList(active), 0660)
		}
	}

	return nil
}
