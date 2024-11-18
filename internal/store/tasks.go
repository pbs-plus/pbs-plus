//go:build linux

package store

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/fsnotify/fsnotify"
)

type TaskResponse struct {
	Data  Task `json:"data"`
	Total int  `json:"total"`
}

type Task struct {
	WID        string `json:"id"`
	Node       string `json:"node"`
	PID        int    `json:"pid"`
	PStart     int    `json:"pstart"`
	StartTime  int64  `json:"starttime"`
	EndTime    int64  `json:"endtime"`
	UPID       string `json:"upid"`
	User       string `json:"user"`
	WorkerType string `json:"worker_type"`
	Status     string `json:"status"`
	ExitStatus string `json:"exitstatus"`
}

func isDir(path string) bool {
	info, err := os.Stat(path)
	if err != nil {
		log.Println("Error checking path:", err)
		return false
	}
	return info.IsDir()
}

func encodeToHexEscapes(input string) string {
	var encoded strings.Builder
	for _, char := range input {
		if char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' {
			encoded.WriteRune(char)
		} else {
			encoded.WriteString(fmt.Sprintf(`\x%02x`, char))
		}
	}

	return encoded.String()
}

func (storeInstance *Store) GetMostRecentTask(ctx context.Context, job *Job) (chan Task, error) {
	tasksParentPath := "/var/log/proxmox-backup/tasks"
	hostname, err := os.Hostname()
	if err != nil {
		hostnameFile, err := os.ReadFile("/etc/hostname")
		if err != nil {
			hostname = "localhost"
		}

		hostname = strings.TrimSpace(string(hostnameFile))
	}

	target, err := storeInstance.GetTarget(job.Target)
	if err != nil {
		return nil, fmt.Errorf("GetMostRecentTask -> %w", err)
	}

	if target == nil {
		return nil, fmt.Errorf("GetMostRecentTask: Target '%s' does not exist.", job.Target)
	}

	isAgent := strings.HasPrefix(target.Path, "agent://")

	backupId := hostname
	if isAgent {
		backupId = strings.TrimSpace(strings.Split(target.Name, " - ")[0])
	}

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, fmt.Errorf("failed to create watcher: %w", err)
	}

	err = watcher.Add(tasksParentPath)
	if err != nil {
		return nil, fmt.Errorf("failed to add folder to watcher: %w", err)
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
		return nil, fmt.Errorf("failed to walk folder: %w", err)
	}

	returnChan := make(chan Task)

	go func() {
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
					} else {
						searchString := fmt.Sprintf(":backup:%s%shost-%s", job.Store, encodeToHexEscapes(":"), encodeToHexEscapes(backupId))
						log.Printf("Checking if %s contains %s\n", searchString, event.Name)
						if !strings.Contains(event.Name, ".tmp_") && strings.Contains(event.Name, searchString) {
							fileName := filepath.Base(event.Name)
							colonSplit := strings.Split(fileName, ":")
							actualUpid := colonSplit[:9]
							newTask, err := storeInstance.GetTaskByUPID(strings.Join(actualUpid, ":") + ":")
							if err != nil {
								log.Printf("GetMostRecentTask: error getting tasks: %v\n", err)
								return
							}
							returnChan <- *newTask
							return
						}
					}
				}
			case <-time.After(time.Second * 60):
				log.Println("GetMostRecentTask: error getting tasks: timeout, not found")
				return
			case <-ctx.Done():
				log.Println("GetMostRecentTask: error getting tasks: context cancelled, not found")
				return
			}
		}
	}()

	return returnChan, nil
}

func (storeInstance *Store) GetTaskByUPID(upid string) (*Task, error) {
	var resp TaskResponse
	err := storeInstance.ProxmoxHTTPRequest(
		http.MethodGet,
		fmt.Sprintf("/api2/json/nodes/localhost/tasks/%s/status", upid),
		nil,
		&resp,
	)
	if err != nil {
		return nil, fmt.Errorf("GetTaskByUPID: error creating http request -> %w", err)
	}

	if resp.Data.Status == "stopped" {
		endTime, err := storeInstance.GetTaskEndTime(&resp.Data)
		if err != nil {
			return nil, fmt.Errorf("GetTaskByUPID: error getting task end time -> %w", err)
		}

		resp.Data.EndTime = endTime
	}

	return &resp.Data, nil
}

func (storeInstance *Store) GetTaskEndTime(task *Task) (int64, error) {
	if storeInstance.LastToken == nil && storeInstance.APIToken == nil {
		return -1, fmt.Errorf("GetTaskEndTime: token is required")
	}

	upidSplit := strings.Split(task.UPID, ":")
	if len(upidSplit) < 4 {
		return -1, fmt.Errorf("GetTaskEndTime: error getting tasks: invalid upid")
	}

	parsed := upidSplit[3]
	logFolder := parsed[len(parsed)-2:]

	logPath := fmt.Sprintf("/var/log/proxmox-backup/tasks/%s/%s", logFolder, task.UPID)

	logStat, err := os.Stat(logPath)
	if err == nil {
		return logStat.ModTime().Unix(), nil
	}

	return -1, fmt.Errorf("GetTaskEndTime: error getting tasks: not found (%s) -> %w", logPath, err)
}
