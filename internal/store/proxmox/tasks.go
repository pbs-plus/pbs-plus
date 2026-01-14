//go:build linux

package proxmox

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/store/constants"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"github.com/pbs-plus/pbs-plus/internal/utils/safemap"
)

func init() {
	CleanupPbsPlusActiveTasks()
}

// ListTasksJSON shells out to the CLI and unmarshals into []Task.
func ListTasksJSON(ctx context.Context) ([]Task, error) {
	cmd := exec.CommandContext(
		ctx,
		"proxmox-backup-manager",
		"task", "list",
		"--output-format=json",
		"--all",
	)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out

	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf(
			"ListTasksJSON: command failed: %w, output=%q",
			err, out.String(),
		)
	}

	var tasks []Task
	if err := json.Unmarshal(out.Bytes(), &tasks); err != nil {
		return nil, fmt.Errorf(
			"ListTasksJSON: unmarshal failed: %w, data=%q",
			err, out.String(),
		)
	}
	return tasks, nil
}

type TaskCache struct {
	task      Task
	timestamp time.Time
}

var taskCache = safemap.New[string, TaskCache]()

func GetTaskByUPID(upid string) (Task, error) {
	task, ok := taskCache.Get(upid)
	if ok && time.Now().Sub(task.timestamp) <= 5*time.Second {
		return task.task, nil
	}

	resp, err := ParseUPID(upid)
	if err != nil {
		return Task{}, err
	}

	resp.Status = "stopped"
	if IsUPIDRunning(upid) {
		resp.Status = "running"
		return resp, nil
	}

	lastLog, err := parseLastLogMessage(upid)
	if err != nil {
		resp.ExitStatus = "unknown"
	}
	if lastLog == "TASK OK" {
		resp.ExitStatus = "OK"
	} else if strings.HasPrefix(lastLog, "TASK WARNINGS: ") {
		resp.ExitStatus = strings.TrimPrefix(lastLog, "TASK ")
	} else if strings.HasPrefix(lastLog, "TASK QUEUED: ") {
		resp.ExitStatus = strings.TrimPrefix(lastLog, "TASK ")
	} else {
		resp.ExitStatus = strings.TrimPrefix(lastLog, "TASK ERROR: ")
	}

	endTime, err := GetTaskEndTime(resp)
	if err != nil {
		return Task{}, fmt.Errorf("GetTaskByUPID: error getting task end time -> %w", err)
	}

	resp.EndTime = endTime

	taskCache.Set(upid, TaskCache{task: resp, timestamp: time.Now()})

	return resp, nil
}

func GetTaskEndTime(task Task) (int64, error) {
	logPath, err := GetLogPath(task.UPID)
	if err != nil {
		return -1, fmt.Errorf("GetTaskEndTime: error getting log path (%s) -> %w", logPath, err)
	}

	logStat, err := os.Stat(logPath)
	if err == nil {
		return logStat.ModTime().Unix(), nil
	}

	return -1, fmt.Errorf("GetTaskEndTime: error getting tasks: not found (%s) -> %w", logPath, err)
}

func IsUPIDRunning(upid string) bool {
	activePath := filepath.Join(constants.TaskLogsBasePath, "active")
	cmd := exec.Command("grep", "-F", upid, activePath)
	output, err := cmd.Output()
	if err != nil {
		// If grep exits with a non-zero status, it means the UPID was not found.
		if exitError, ok := err.(*exec.ExitError); ok && exitError.ExitCode() == 1 {
			return false
		}
		syslog.L.Error(err).WithField("upid", upid)
		return false
	}

	// If output is not empty, the UPID was found.
	return strings.TrimSpace(string(output)) != ""
}

func CleanupPbsPlusActiveTasks() error {
	filePath := constants.ActiveLogsPath
	targetNode := "pbsplus"

	lockPath := filepath.Join(filepath.Dir(filePath), "."+filepath.Base(filePath)+".lock")
	lockFile, err := os.OpenFile(lockPath, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return fmt.Errorf("could not create/open lock file: %w", err)
	}
	defer lockFile.Close()

	if err := syscall.Flock(int(lockFile.Fd()), syscall.LOCK_EX); err != nil {
		return fmt.Errorf("could not acquire lock: %w", err)
	}
	defer syscall.Flock(int(lockFile.Fd()), syscall.LOCK_UN)

	originalInfo, err := os.Stat(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // Nothing to clean
		}
		return err
	}

	f, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer f.Close()

	tempFile, err := os.CreateTemp(filepath.Dir(filePath), "active_cleanup_*.tmp")
	if err != nil {
		return err
	}
	tempPath := tempFile.Name()

	success := false
	defer func() {
		if !success {
			tempFile.Close()
			os.Remove(tempPath)
		}
	}()

	if err := tempFile.Chmod(originalInfo.Mode()); err != nil {
		return err
	}
	if stat, ok := originalInfo.Sys().(*syscall.Stat_t); ok {
		_ = tempFile.Chown(int(stat.Uid), int(stat.Gid))
	}

	scanner := bufio.NewScanner(f)
	writer := bufio.NewWriter(tempFile)

	// UPID Format: UPID:NODE:PID:PSTART:TASKID:STARTTIME:WTYPE:WID:USER:
	// Index 1 (splitting by ':') is the Node.
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Split(line, ":")

		isPbsPlusTask := false
		if len(parts) > 1 && parts[1] == targetNode {
			isPbsPlusTask = true
		}

		if !isPbsPlusTask {
			if _, err := writer.WriteString(line + "\n"); err != nil {
				return err
			}
		}
	}

	if err := writer.Flush(); err != nil {
		return err
	}

	if err := tempFile.Sync(); err != nil {
		return err
	}
	tempFile.Close()

	if err := os.Rename(tempPath, filePath); err != nil {
		return err
	}

	success = true
	return nil
}
