//go:build linux

package proxmox

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/sonroyaalmerol/pbs-plus/internal/store/types"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils/safemap"
)

func (proxmoxSess *ProxmoxSession) GetJobTask(
	ctx context.Context,
	readyChan chan struct{},
	job types.Job,
	target types.Target,
) (Task, error) {
	tasksParentPath := "/var/log/proxmox-backup/tasks"
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return Task{}, fmt.Errorf("failed to create watcher: %w", err)
	}
	defer watcher.Close()

	err = watcher.Add(tasksParentPath)
	if err != nil {
		return Task{}, fmt.Errorf("failed to add folder to watcher: %w", err)
	}

	// Helper function to check if a file matches our search criteria
	checkFile := func(filePath string, searchString string) (Task, error) {
		if !strings.Contains(filePath, ".tmp_") && strings.Contains(filePath, searchString) {
			syslog.L.Info().WithMessage(fmt.Sprintf("Proceeding: %s contains %s\n", filePath, searchString)).Write()
			fileName := filepath.Base(filePath)
			syslog.L.Info().WithMessage(fmt.Sprintf("Getting UPID: %s\n", fileName)).Write()
			newTask, err := proxmoxSess.GetTaskByUPID(fileName)
			if err != nil {
				return Task{}, fmt.Errorf("GetJobTask: error getting task: %v\n", err)
			}
			syslog.L.Info().WithMessage(fmt.Sprintf("Sending UPID: %s\n", fileName)).Write()
			return newTask, nil
		}
		return Task{}, os.ErrNotExist
	}

	// Helper function to scan directory for matching files
	scanDirectory := func(dirPath string, searchString string) (Task, error) {
		files, err := os.ReadDir(dirPath)
		if err != nil {
			syslog.L.Error(err).Write()
			return Task{}, os.ErrNotExist
		}

		for _, file := range files {
			if !file.IsDir() {
				filePath := filepath.Join(dirPath, file.Name())
				task, err := checkFile(filePath, searchString)
				if err == nil {
					return task, nil
				}
				if !os.IsNotExist(err) {
					return Task{}, err
				}
			}
		}
		return Task{}, os.ErrNotExist
	}

	err = filepath.WalkDir(tasksParentPath, func(path string, info os.DirEntry, err error) error {
		if err != nil {
			syslog.L.Error(err).Write()
			return nil // Continue walking the tree
		}
		if info.IsDir() {
			err = watcher.Add(path)
			if err != nil {
				syslog.L.Error(err).Write()
			}
		}
		return nil
	})
	if err != nil {
		return Task{}, fmt.Errorf("failed to walk folder: %w", err)
	}

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

	searchString := fmt.Sprintf(":backup:%s%shost-%s", encodeToHexEscapes(job.Store), encodeToHexEscapes(":"), encodeToHexEscapes(backupId))

	syslog.L.Info().WithMessage("ready to start backup").Write()
	close(readyChan)

	for {
		select {
		case event, ok := <-watcher.Events:
			if !ok {
				return Task{}, fmt.Errorf("watcher events channel closed")
			}
			if event.Op&fsnotify.Create == fsnotify.Create {
				if isDir(event.Name) {
					err = watcher.Add(event.Name)
					if err != nil {
						syslog.L.Error(err).Write()
					}

					task, err := scanDirectory(event.Name, searchString)
					if err == nil {
						return task, nil
					}
					if !os.IsNotExist(err) {
						return Task{}, err
					}
				} else {
					task, err := checkFile(event.Name, searchString)
					if err == nil {
						return task, nil
					}
					if !os.IsNotExist(err) {
						return Task{}, err
					}
				}
			}
		case <-ctx.Done():
			return Task{}, ctx.Err()
		}
	}
}

type TaskCache struct {
	task      Task
	timestamp time.Time
}

var taskCache = safemap.New[string, TaskCache]()

func (proxmoxSess *ProxmoxSession) GetTaskByUPID(upid string) (Task, error) {
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
	} else {
		resp.ExitStatus = strings.TrimPrefix(lastLog, "TASK ERROR: ")
	}

	endTime, err := proxmoxSess.GetTaskEndTime(resp)
	if err != nil {
		return Task{}, fmt.Errorf("GetTaskByUPID: error getting task end time -> %w", err)
	}

	resp.EndTime = endTime

	taskCache.Set(upid, TaskCache{task: resp, timestamp: time.Now()})

	return resp, nil
}

func (proxmoxSess *ProxmoxSession) GetTaskEndTime(task Task) (int64, error) {
	if proxmoxSess.APIToken == nil {
		return -1, fmt.Errorf("GetTaskEndTime: token is required")
	}

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
