//go:build linux

package proxmox

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/fsnotify/fsnotify"
	"github.com/sonroyaalmerol/pbs-plus/internal/store/types"
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

func (proxmoxSess *ProxmoxSession) GetJobTask(ctx context.Context, readyChan chan struct{}, job *types.Job, target *types.Target) (*Task, error) {
	tasksParentPath := "/var/log/proxmox-backup/tasks"
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, fmt.Errorf("failed to create watcher: %w", err)
	}
	err = watcher.Add(tasksParentPath)
	if err != nil {
		return nil, fmt.Errorf("failed to add folder to watcher: %w", err)
	}

	// Helper function to check if a file matches our search criteria
	checkFile := func(filePath string, searchString string) (*Task, error) {
		if !strings.Contains(filePath, ".tmp_") && strings.Contains(filePath, searchString) {
			log.Printf("Proceeding: %s contains %s\n", filePath, searchString)
			fileName := filepath.Base(filePath)
			log.Printf("Getting UPID: %s\n", fileName)
			newTask, err := proxmoxSess.GetTaskByUPID(fileName)
			if err != nil {
				return nil, fmt.Errorf("GetJobTask: error getting task: %v\n", err)
			}
			log.Printf("Sending UPID: %s\n", fileName)
			return newTask, nil
		}
		return nil, nil
	}

	// Helper function to scan directory for matching files
	scanDirectory := func(dirPath string, searchString string) (*Task, error) {
		files, err := os.ReadDir(dirPath)
		if err != nil {
			log.Printf("Error reading directory %s: %v\n", dirPath, err)
			return nil, nil
		}

		for _, file := range files {
			if !file.IsDir() {
				filePath := filepath.Join(dirPath, file.Name())
				task, err := checkFile(filePath, searchString)
				if err != nil {
					return nil, err
				}
				if task != nil {
					return task, nil
				}
			}
		}
		return nil, nil
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
					if err != nil {
						return nil, err
					}
					if task != nil {
						return task, nil
					}
				} else {
					task, err := checkFile(event.Name, searchString)
					if err != nil {
						return nil, err
					}
					if task != nil {
						return task, nil
					}
				}
			}
		case <-ctx.Done():
			return nil, nil
		}
	}
}

func (proxmoxSess *ProxmoxSession) GetTaskByUPID(upid string) (*Task, error) {
	var resp TaskResponse
	err := proxmoxSess.ProxmoxHTTPRequest(
		http.MethodGet,
		fmt.Sprintf("/api2/json/nodes/localhost/tasks/%s/status", upid),
		nil,
		&resp,
	)
	if err != nil {
		return nil, fmt.Errorf("GetTaskByUPID: error creating http request -> %w", err)
	}

	if resp.Data.Status == "stopped" {
		endTime, err := proxmoxSess.GetTaskEndTime(&resp.Data)
		if err != nil {
			return nil, fmt.Errorf("GetTaskByUPID: error getting task end time -> %w", err)
		}

		resp.Data.EndTime = endTime
	}

	return &resp.Data, nil
}

func (proxmoxSess *ProxmoxSession) GetTaskEndTime(task *Task) (int64, error) {
	if proxmoxSess.LastToken == nil && proxmoxSess.APIToken == nil {
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
