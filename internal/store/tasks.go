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
	"github.com/sonroyaalmerol/pbs-plus/internal/utils"
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

func (storeInstance *Store) GetJobTask(ctx context.Context, taskChan chan Task, job *Job) error {
	tasksParentPath := "/var/log/proxmox-backup/tasks"
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("failed to create watcher: %w", err)
	}

	err = watcher.Add(tasksParentPath)
	if err != nil {
		return fmt.Errorf("failed to add folder to watcher: %w", err)
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
		return fmt.Errorf("failed to walk folder: %w", err)
	}

	fileEventBufferred := make(chan fsnotify.Event, 100)
	go func() {
		for {
			select {
			case event := <-watcher.Events:
				fileEventBufferred <- event
			case <-ctx.Done():
				log.Println("GetJobTask: error getting tasks: context cancelled, not found")
				return
			}
		}
	}()

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
		return fmt.Errorf("GetJobTask -> %w", err)
	}

	if target == nil {
		return fmt.Errorf("GetJobTask: Target '%s' does not exist.", job.Target)
	}

	isAgent := strings.HasPrefix(target.Path, "agent://")

	backupId := hostname
	if isAgent {
		backupId = strings.TrimSpace(strings.Split(target.Name, " - ")[0])
	}

	go func() {
		defer watcher.Close()
		for {
			select {
			case event := <-fileEventBufferred:
				if event.Op&fsnotify.Create == fsnotify.Create {
					if isDir(event.Name) {
						err = watcher.Add(event.Name)
						if err != nil {
							log.Println("Failed to add directory to watcher:", err)
						}
					} else {
						searchString := fmt.Sprintf(":backup:%s%shost-%s", job.Store, encodeToHexEscapes(":"), encodeToHexEscapes(backupId))
						log.Printf("Checking if %s contains %s\n", event.Name, searchString)
						if !strings.Contains(event.Name, ".tmp_") && strings.Contains(event.Name, searchString) {
							log.Printf("Proceeding: %s contains %s\n", event.Name, searchString)

							newLineChan := make(chan string)
							go func() {
								err := utils.TailFile(ctx, event.Name, newLineChan)
								if err != nil {
									fmt.Println("Error:", err)
								}
								close(newLineChan)
							}()

							go func() {
								didxFileName := strings.ReplaceAll(job.Target, " ", "-")
								for {
									select {
									case line := <-newLineChan:
										if strings.Contains(line, didxFileName) {
											fileName := filepath.Base(event.Name)

											log.Printf("Getting UPID: %s\n", fileName)
											newTask, err := storeInstance.GetTaskByUPID(fileName)
											if err != nil {
												log.Printf("GetJobTask: error getting task: %v\n", err)
												return
											}
											log.Printf("Sending UPID: %s\n", fileName)
											taskChan <- *newTask
											return
										}
									case <-ctx.Done():
										log.Println("GetJobTask: error getting tasks: context cancelled, not found")
										return
									case <-time.After(time.Second * 60):
										log.Println("GetJobTask: error getting tasks: timeout, not found")
										return
									}
								}
							}()
						}
					}
				}
			case <-time.After(time.Second * 60):
				log.Println("GetJobTask: error getting tasks: timeout, not found")
				return
			case <-ctx.Done():
				log.Println("GetJobTask: error getting tasks: context cancelled, not found")
				return
			}
		}
	}()

	return nil
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
