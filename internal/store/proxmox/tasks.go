//go:build linux

package proxmox

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/fsnotify/fsnotify"
	"github.com/sonroyaalmerol/pbs-plus/internal/store/types"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
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

// ParseUPID parses a Proxmox Backup Server UPID string and returns a Task struct.
func ParseUPID(upid string) (*Task, error) {
	// Define the regex pattern for the UPID.
	pattern := `^UPID:(?P<node>[a-zA-Z0-9]([a-zA-Z0-9\-]*[a-zA-Z0-9])?):(?P<pid>[0-9A-Fa-f]{8}):(?P<pstart>[0-9A-Fa-f]{8,9}):(?P<task_id>[0-9A-Fa-f]{8,16}):(?P<starttime>[0-9A-Fa-f]{8}):(?P<wtype>[^:\s]+):(?P<wid>[^:\s]*):(?P<authid>[^:\s]+):$`

	// Compile the regex.
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, fmt.Errorf("failed to compile regex: %w", err)
	}

	decodedUpid, err := decodeFromHexEscapes(upid)
	if err != nil {
		return nil, err
	}

	// Match the UPID string against the regex.
	matches := re.FindStringSubmatch(decodedUpid)
	if matches == nil {
		return nil, fmt.Errorf("invalid UPID format")
	}

	// Create a new Task instance.
	task := &Task{
		UPID: upid, // Store the original UPID string.
	}

	// Extract the named groups using the regex's SubexpNames.
	for i, name := range re.SubexpNames() {
		if name == "" || i >= len(matches) {
			continue
		}
		switch name {
		case "node":
			task.Node = matches[i]
		case "pid":
			// Convert PID from hex to int.
			pid, err := strconv.ParseInt(matches[i], 16, 32)
			if err != nil {
				return nil, fmt.Errorf("failed to parse PID: %w", err)
			}
			task.PID = int(pid)
		case "pstart":
			// Convert PStart from hex to int.
			pstart, err := strconv.ParseInt(matches[i], 16, 32)
			if err != nil {
				return nil, fmt.Errorf("failed to parse PStart: %w", err)
			}
			task.PStart = int(pstart)
		case "starttime":
			// Convert StartTime from hex to int64.
			startTime, err := strconv.ParseInt(matches[i], 16, 64)
			if err != nil {
				return nil, fmt.Errorf("failed to parse StartTime: %w", err)
			}
			task.StartTime = startTime
		case "wtype":
			task.WorkerType = matches[i]
		case "wid":
			task.WID = matches[i]
		case "authid":
			task.User = matches[i]
		}
	}

	return task, nil
}

func IsUPIDRunning(upid string) bool {
	cmd := exec.Command("grep", "-F", upid, "/var/log/proxmox-backup/tasks/active")
	output, err := cmd.Output()
	if err != nil {
		// If grep exits with a non-zero status, it means the UPID was not found.
		if exitError, ok := err.(*exec.ExitError); ok && exitError.ExitCode() == 1 {
			return false
		}
		if syslog.L != nil {
			syslog.L.Errorf("error running grep: %w", err)
		}
		return false
	}

	// If output is not empty, the UPID was found.
	return strings.TrimSpace(string(output)) != ""
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

func decodeFromHexEscapes(input string) (string, error) {
	var decoded strings.Builder
	i := 0
	for i < len(input) {
		if input[i] == '\\' && i+3 < len(input) && input[i+1] == 'x' {
			// Extract the hex part
			hexPart := input[i+2 : i+4]
			// Convert the hex string to an integer
			charCode, err := strconv.ParseInt(hexPart, 16, 32)
			if err != nil {
				return "", fmt.Errorf("invalid hex escape sequence: \\x%s", hexPart)
			}
			// Append the decoded character
			decoded.WriteRune(rune(charCode))
			i += 4 // Skip past the \xNN
		} else {
			// Append the regular character
			decoded.WriteByte(input[i])
			i++
		}
	}
	return decoded.String(), nil
}

func getLogPath(upid string) (string, error) {
	upidSplit := strings.Split(upid, ":")
	if len(upidSplit) < 4 {
		return "", fmt.Errorf("invalid upid")
	}

	parsed := upidSplit[3]
	logFolder := parsed[len(parsed)-2:]

	logPath := fmt.Sprintf("/var/log/proxmox-backup/tasks/%s/%s", logFolder, upid)

	return logPath, nil
}

func parseLastLogMessage(upid string) (string, error) {
	logPath, err := getLogPath(upid)
	if err != nil {
		return "", err
	}
	cmd := exec.Command("tail", "-n", "1", logPath)

	var out bytes.Buffer
	cmd.Stdout = &out

	err = cmd.Run()
	if err != nil {
		return "", fmt.Errorf("failed to execute tail command: %w", err)
	}

	lastLine := strings.TrimSpace(out.String())

	re := regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}[-+]\d{2}:\d{2}: `)
	message := re.ReplaceAllString(lastLine, "")

	return strings.TrimSpace(message), nil
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
	resp, err := ParseUPID(upid)
	if err != nil {
		return nil, err
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
		return nil, fmt.Errorf("GetTaskByUPID: error getting task end time -> %w", err)
	}

	resp.EndTime = endTime

	return resp, nil
}

func (proxmoxSess *ProxmoxSession) GetTaskEndTime(task *Task) (int64, error) {
	if proxmoxSess.APIToken == nil {
		return -1, fmt.Errorf("GetTaskEndTime: token is required")
	}

	logPath, err := getLogPath(task.UPID)
	if err != nil {
		return -1, fmt.Errorf("GetTaskEndTime: error getting log path (%s) -> %w", logPath, err)
	}

	logStat, err := os.Stat(logPath)
	if err == nil {
		return logStat.ModTime().Unix(), nil
	}

	return -1, fmt.Errorf("GetTaskEndTime: error getting tasks: not found (%s) -> %w", logPath, err)
}
