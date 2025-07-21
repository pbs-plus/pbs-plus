//go:build linux

package proxmox

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/store/types"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"github.com/pbs-plus/pbs-plus/internal/utils/safemap"
)

// listTasksJSON shells out to the CLI and unmarshals into []Task.
func listTasksJSON(ctx context.Context) ([]Task, error) {
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
			"listTasksJSON: command failed: %w, output=%q",
			err, out.String(),
		)
	}

	var tasks []Task
	if err := json.Unmarshal(out.Bytes(), &tasks); err != nil {
		return nil, fmt.Errorf(
			"listTasksJSON: unmarshal failed: %w, data=%q",
			err, out.String(),
		)
	}
	return tasks, nil
}

func GetJobTask(
	ctx context.Context,
	readyChan chan struct{},
	job types.Job,
	target types.Target,
) (Task, error) {
	hostname, err := os.Hostname()
	if err != nil {
		hostnameFile, err := os.ReadFile("/etc/hostname")
		if err != nil {
			hostname = "localhost"
		} else {
			hostname = strings.TrimSpace(string(hostnameFile))
		}
	}

	isAgent := strings.HasPrefix(target.Path, "agent://")
	backupId := hostname
	if isAgent {
		backupId = strings.TrimSpace(strings.Split(target.Name, " - ")[0])
	}
	backupId = NormalizeHostname(backupId)

	searchString := fmt.Sprintf(":backup:%s%shost-%s", encodeToHexEscapes(job.Store), encodeToHexEscapes(":"), encodeToHexEscapes(backupId))

	initialUPIDs := make(map[string]struct{})
	tasks, err := listTasksJSON(ctx)
	if err != nil {
		return Task{}, err
	}

	for _, t := range tasks {
		initialUPIDs[t.UPID] = struct{}{}
	}

	syslog.L.Info().WithMessage("ready to start backup").Write()
	close(readyChan)

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return Task{}, ctx.Err()

		case <-ticker.C:
			tasks, err := listTasksJSON(ctx)
			if err != nil {
				syslog.L.Error(err).Write()
				continue
			}

			for _, t := range tasks {
				if t.WorkerType == "backup" && strings.Contains(t.UPID, searchString) {
					if _, seen := initialUPIDs[t.UPID]; seen {
						continue // skip tasks in the initial set
					}
					return GetTaskByUPID(t.UPID)
				}
			}
		}
	}
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
