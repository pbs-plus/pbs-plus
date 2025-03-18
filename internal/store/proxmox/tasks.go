//go:build linux

package proxmox

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/fsnotify/fsnotify"
	"github.com/sonroyaalmerol/pbs-plus/internal/store/types"
)

func (proxmoxSess *ProxmoxSession) GetJobTask(ctx context.Context, readyChan chan struct{}, job types.Job, target types.Target) (Task, error) {
	tasksParentPath := "/var/log/proxmox-backup/tasks"
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return Task{}, fmt.Errorf("failed to create watcher: %w", err)
	}
	err = watcher.Add(tasksParentPath)
	if err != nil {
		return Task{}, fmt.Errorf("failed to add folder to watcher: %w", err)
	}

	// Helper function to check if a file matches our search criteria
	checkFile := func(filePath string, searchString string) (Task, error) {
		if !strings.Contains(filePath, ".tmp_") && strings.Contains(filePath, searchString) {
			log.Printf("Proceeding: %s contains %s\n", filePath, searchString)
			fileName := filepath.Base(filePath)
			log.Printf("Getting UPID: %s\n", fileName)
			newTask, err := proxmoxSess.GetTaskByUPID(fileName)
			if err != nil {
				return Task{}, fmt.Errorf("GetJobTask: error getting task: %v\n", err)
			}
			log.Printf("Sending UPID: %s\n", fileName)
			return newTask, nil
		}
		return Task{}, os.ErrNotExist
	}

	// Helper function to scan directory for matching files
	scanDirectory := func(dirPath string, searchString string) (Task, error) {
		files, err := os.ReadDir(dirPath)
		if err != nil {
			log.Printf("Error reading directory %s: %v\n", dirPath, err)
			return Task{}, os.ErrNotExist
		}

		for _, file := range files {
			if !file.IsDir() {
				filePath := filepath.Join(dirPath, file.Name())
				task, err := checkFile(filePath, searchString)
				if err != nil {
					return Task{}, err
				}
				return task, nil
			}
		}
		return Task{}, os.ErrNotExist
	}

	err = filepath.Walk(tasksParentPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			log.Println("Error walking the path:", err)
			return err
		}
		if info.IsDir() {
			err = watcher.Add(path)
			if err != nil {
				log.Println("Failed to add directory to watcher:", err)
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
		}
		hostname = strings.TrimSpace(string(hostnameFile))
	}

	isAgent := strings.HasPrefix(target.Path, "agent://")
	backupId := hostname
	if isAgent {
		backupId = strings.TrimSpace(strings.Split(target.Name, " - ")[0])
	}

	searchString := fmt.Sprintf(":backup:%s%shost-%s", job.Store, encodeToHexEscapes(":"), encodeToHexEscapes(backupId))

	close(readyChan)
	defer watcher.Close()

	for {
		select {
		case event := <-watcher.Events:
			if event.Op&fsnotify.Create == fsnotify.Create {
				if isDir(event.Name) {
					err = watcher.Add(event.Name)
					if err != nil {
						log.Println("Failed to add directory to watcher:", err)
					}

					task, err := scanDirectory(event.Name, searchString)
					if err != nil && !os.IsNotExist(err) {
						return Task{}, err
					} else if !os.IsNotExist(err) {
						return task, nil
					}
				} else {
					task, err := checkFile(event.Name, searchString)
					if err != nil {
						return Task{}, err
					}
					return task, nil
				}
			}
		case <-ctx.Done():
			return Task{}, ctx.Err()
		}
	}
}

func (proxmoxSess *ProxmoxSession) GetTaskByUPID(upid string) (Task, error) {
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
