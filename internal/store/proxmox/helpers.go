//go:build linux

package proxmox

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/store/constants"
	"github.com/pbs-plus/pbs-plus/internal/store/types"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

func NormalizeHostname(hostname string) string {
	return strings.ReplaceAll(strings.ReplaceAll(hostname, ".", "-"), " ", "-")
}

// ParseUPID parses a Proxmox Backup Server UPID string and returns a Task struct.
func ParseUPID(upid string) (Task, error) {
	// Define the regex pattern for the UPID.
	pattern := `^UPID:(?P<node>[a-zA-Z0-9]([a-zA-Z0-9\-]*[a-zA-Z0-9])?):(?P<pid>[0-9A-Fa-f]{8}):(?P<pstart>[0-9A-Fa-f]{8,9}):(?P<task_id>[0-9A-Fa-f]{8,16}):(?P<starttime>[0-9A-Fa-f]{8}):(?P<wtype>[^:\s]+):(?P<wid>[^:\s]*):(?P<authid>[^:\s]+):$`

	// Compile the regex.
	re, err := regexp.Compile(pattern)
	if err != nil {
		return Task{}, fmt.Errorf("failed to compile regex: %w", err)
	}

	// Match the UPID string against the regex.
	matches := re.FindStringSubmatch(upid)
	if matches == nil {
		return Task{}, fmt.Errorf("invalid UPID format")
	}

	// Create a new Task instance.
	task := Task{
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
				return Task{}, fmt.Errorf("failed to parse PID: %w", err)
			}
			task.PID = int(pid)
		case "pstart":
			// Convert PStart from hex to int.
			pstart, err := strconv.ParseInt(matches[i], 16, 32)
			if err != nil {
				return Task{}, fmt.Errorf("failed to parse PStart: %w", err)
			}
			task.PStart = int(pstart)
		case "starttime":
			// Convert StartTime from hex to int64.
			startTime, err := strconv.ParseInt(matches[i], 16, 64)
			if err != nil {
				return Task{}, fmt.Errorf("failed to parse StartTime: %w", err)
			}
			task.StartTime = startTime
		case "wtype":
			task.WorkerType = matches[i]
		case "task_id":
			task.TaskId = matches[i]
		case "wid":
			task.WID = matches[i]
		case "authid":
			task.User = matches[i]
		}
	}

	return task, nil
}

func (task *Task) GenerateUPID() string {
	// Format the parts into the UPID string structure:
	// UPID:<node>:<pid>:<pstart>:<task_id>:<starttime>:<wtype>:<wid>:<authid>:
	// PID, PStart, and StartTime are formatted as 8-character zero-padded hex.
	// task_id (Task.ID) is included as is (assuming it's the correct hex format).
	// The format strings align with the structure parsed by ParseUPID.
	upid := fmt.Sprintf(
		"UPID:%s:%08X:%08X:%s:%08X:%s:%s:%s:",
		task.Node,
		task.PID,
		task.PStart,
		task.TaskId,
		task.StartTime,
		task.WorkerType,
		task.WID,
		task.User,
	)
	return upid
}

var pstart = atomic.Int32{}

func getPStart() int {
	return int(pstart.Add(1))
}

type QueuedTask struct {
	Task
	sync.Mutex
	closed atomic.Bool
	path   string
	job    types.Job
}

func GenerateQueuedTask(job types.Job, web bool) (QueuedTask, error) {
	targetName := strings.TrimSpace(strings.Split(job.Target, " - ")[0])
	wid := fmt.Sprintf("%s%shost-%s", encodeToHexEscapes(job.Store), encodeToHexEscapes(":"), encodeToHexEscapes(targetName))
	startTime := fmt.Sprintf("%08X", uint32(time.Now().Unix()))

	wtype := "backup"
	node := "pbsplusgen-queue"

	task := Task{
		Node:       node,
		PID:        os.Getpid(),
		PStart:     getPStart(),
		StartTime:  time.Now().Unix(),
		WorkerType: wtype,
		WID:        wid,
		User:       AUTH_ID,
	}

	pid := fmt.Sprintf("%08X", task.PID)
	pstart := fmt.Sprintf("%08X", task.PStart)
	taskID := fmt.Sprintf("%08X", rand.Uint32())

	upid := fmt.Sprintf("UPID:%s:%s:%s:%s:%s:%s:%s:%s:", node, pid, pstart, taskID, startTime, wtype, wid, AUTH_ID)

	task.UPID = upid

	path, err := GetLogPath(upid)
	if err != nil {
		return QueuedTask{}, err
	}

	_ = os.MkdirAll(filepath.Dir(path), 0755)
	_ = os.Chown(filepath.Dir(path), 34, 34)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return QueuedTask{}, err
	}

	err = file.Chown(34, 34)
	if err != nil {
		return QueuedTask{}, err
	}
	defer file.Close()

	timestamp := time.Now().Format(time.RFC3339)
	statusLine := fmt.Sprintf("%s: TASK QUEUED: ", timestamp)
	if web {
		statusLine += "job started from web UI\n"
	} else {
		statusLine += "job started from schedule\n"
	}

	if _, err := file.WriteString(statusLine); err != nil {
		return QueuedTask{}, fmt.Errorf("failed to write status line: %w", err)
	}

	task.Status = "running"

	return QueuedTask{Task: task, job: job, path: path}, nil
}

func ChangeUPIDStartTime(upid string, startTime time.Time) (string, error) {
	if !strings.HasPrefix(upid, "UPID:") || !strings.HasSuffix(upid, ":") {
		return "", fmt.Errorf("invalid UPID format: must start with 'UPID:' and end with ':'")
	}

	parsedTask, err := ParseUPID(upid)
	if err != nil {
		return "", err
	}

	path, err := GetLogPath(upid)
	if err != nil {
		return "", err
	}

	parsedTask.StartTime = startTime.Unix()

	newUpid := parsedTask.GenerateUPID()

	newPath, err := GetLogPath(newUpid)
	if err != nil {
		return "", err
	}

	err = os.Rename(path, newPath)
	if err != nil {
		return "", err
	}
	syslog.L.Info().WithFields(map[string]any{"original": upid, "new": newUpid}).WithMessage("updated UPID start time").Write()

	_ = os.Symlink(newPath, path)

	return newUpid, nil
}

func (task *QueuedTask) UpdateDescription(desc string) error {
	if task.closed.Load() {
		return nil
	}

	task.Lock()
	defer task.Unlock()

	file, err := os.OpenFile(task.path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	timestamp := time.Now().Format(time.RFC3339)
	statusLine := fmt.Sprintf("%s: TASK QUEUED: ", timestamp)
	statusLine += desc + "\n"

	if _, err := file.WriteString(statusLine); err != nil {
		return fmt.Errorf("failed to write status line: %w", err)
	}

	syslog.L.Info().WithJob(task.job.ID).WithMessage(desc).Write()

	return nil
}

func (task *QueuedTask) Close() {
	task.Lock()
	defer task.Unlock()

	_ = os.Remove(task.path)
	task.closed.Store(true)
}

func GenerateTaskErrorFile(job types.Job, pbsError error, additionalData []string) (Task, error) {
	targetName := strings.TrimSpace(strings.Split(job.Target, " - ")[0])
	wid := fmt.Sprintf("%s%shost-%s", encodeToHexEscapes(job.Store), encodeToHexEscapes(":"), encodeToHexEscapes(targetName))
	startTime := fmt.Sprintf("%08X", uint32(time.Now().Unix()))

	wtype := "backup"
	node := "pbsplusgen-error"

	task := Task{
		Node:       node,
		PID:        os.Getpid(),
		PStart:     getPStart(),
		StartTime:  time.Now().Unix(),
		WorkerType: wtype,
		WID:        wid,
		User:       AUTH_ID,
	}

	pid := fmt.Sprintf("%08X", task.PID)
	pstart := fmt.Sprintf("%08X", task.PStart)
	taskID := fmt.Sprintf("%08X", rand.Uint32())

	upid := fmt.Sprintf("UPID:%s:%s:%s:%s:%s:%s:%s:%s:", node, pid, pstart, taskID, startTime, wtype, wid, AUTH_ID)

	task.UPID = upid

	path, err := GetLogPath(upid)
	if err != nil {
		return Task{}, err
	}

	_ = os.MkdirAll(filepath.Dir(path), 0755)
	_ = os.Chown(filepath.Dir(path), 34, 34)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return Task{}, err
	}

	err = file.Chown(34, 34)
	if err != nil {
		return Task{}, err
	}
	defer file.Close()

	timestamp := time.Now().Format(time.RFC3339)

	for _, data := range additionalData {
		dataLine := fmt.Sprintf("%s: %s\n", timestamp, data)
		if _, err := file.WriteString(dataLine); err != nil {
			return Task{}, fmt.Errorf("failed to write additional data line: %w", err)
		}
	}

	errorLine := fmt.Sprintf("%s: TASK ERROR: %s\n", timestamp, pbsError.Error())
	if _, err := file.WriteString(errorLine); err != nil {
		return Task{}, fmt.Errorf("failed to write error line: %w", err)
	}

	archive, err := os.OpenFile(filepath.Join(constants.TaskLogsBasePath, "archive"), os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return Task{}, fmt.Errorf("failed to open file archive: %w", err)
	}
	defer archive.Close()

	archiveLine := fmt.Sprintf("%s %s %s\n", upid, startTime, pbsError.Error())
	if _, err := archive.WriteString(archiveLine); err != nil {
		return Task{}, fmt.Errorf("failed to write archive line: %w", err)
	}

	task.Status = "stopped"
	task.ExitStatus = pbsError.Error()
	task.EndTime = time.Now().Unix()

	return task, nil
}

func GenerateTaskOKFile(job types.Job, additionalData []string) (Task, error) {
	targetName := strings.TrimSpace(strings.Split(job.Target, " - ")[0])
	wid := fmt.Sprintf("%s%shost-%s", encodeToHexEscapes(job.Store), encodeToHexEscapes(":"), encodeToHexEscapes(targetName))
	startTime := fmt.Sprintf("%08X", uint32(time.Now().Unix()))

	wtype := "backup"
	node := "pbsplusgen-ok"

	task := Task{
		Node:       node,
		PID:        os.Getpid(),
		PStart:     getPStart(),
		StartTime:  time.Now().Unix(),
		WorkerType: wtype,
		WID:        wid,
		User:       AUTH_ID,
	}

	pid := fmt.Sprintf("%08X", task.PID)
	pstart := fmt.Sprintf("%08X", task.PStart)
	taskID := fmt.Sprintf("%08X", rand.Uint32())

	upid := fmt.Sprintf("UPID:%s:%s:%s:%s:%s:%s:%s:%s:", node, pid, pstart, taskID, startTime, wtype, wid, AUTH_ID)

	task.UPID = upid

	path, err := GetLogPath(upid)
	if err != nil {
		return Task{}, err
	}

	_ = os.MkdirAll(filepath.Dir(path), 0755)
	_ = os.Chown(filepath.Dir(path), 34, 34)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return Task{}, err
	}

	err = file.Chown(34, 34)
	if err != nil {
		return Task{}, err
	}
	defer file.Close()

	timestamp := time.Now().Format(time.RFC3339)

	for _, data := range additionalData {
		dataLine := fmt.Sprintf("%s: %s\n", timestamp, data)
		if _, err := file.WriteString(dataLine); err != nil {
			return Task{}, fmt.Errorf("failed to write additional data line: %w", err)
		}
	}

	errorLine := fmt.Sprintf("%s: TASK OK\n", timestamp)
	if _, err := file.WriteString(errorLine); err != nil {
		return Task{}, fmt.Errorf("failed to write ok line: %w", err)
	}

	archive, err := os.OpenFile(filepath.Join(constants.TaskLogsBasePath, "archive"), os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return Task{}, fmt.Errorf("failed to open file archive: %w", err)
	}
	defer archive.Close()

	archiveLine := fmt.Sprintf("%s %s OK\n", upid, startTime)
	if _, err := archive.WriteString(archiveLine); err != nil {
		return Task{}, fmt.Errorf("failed to write archive line: %w", err)
	}

	task.Status = "stopped"
	task.ExitStatus = "OK"
	task.EndTime = time.Now().Unix()

	return task, nil
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

func GetLogPath(upid string) (string, error) {
	upidSplit := strings.Split(upid, ":")
	if len(upidSplit) < 4 {
		return "", fmt.Errorf("invalid upid")
	}

	parsed := upidSplit[3]
	logFolder := parsed[len(parsed)-2:]

	logPath := filepath.Join(constants.TaskLogsBasePath, logFolder, upid)

	return logPath, nil
}

func parseLastLogMessage(upid string) (string, error) {
	logPath, err := GetLogPath(upid)
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

func buildGroupPath(ns, backupType, backupID, backupTime string) string {
	var parts []string

	if ns != "" {
		nsParts := strings.Split(ns, "/")
		for _, nsPart := range nsParts {
			if nsPart != "" {
				parts = append(parts, "ns", nsPart)
			}
		}
	}

	parts = append(parts, backupType, backupID, backupTime)

	return filepath.Join(parts...)
}

// BuildPxarPaths constructs the full filesystem paths for pxar/mpxar/ppxar files.
// Returns (mpxarPath, ppxarPath, isMetadataSplit, error).
// For non-split pxar: ppxarPath is empty, mpxarPath contains the .pxar.didx path.
// For split pxar: both mpxarPath and ppxarPath are populated.
// If fileName is empty, scans the backup directory for .pxar.didx or .mpxar.didx files.
func BuildPxarPaths(pbsStoreRoot, ns, backupType, backupID, backupTime, fileName string) (mpxarPath, ppxarPath string, isMetadataSplit bool, err error) {
	groupPath := buildGroupPath(ns, backupType, backupID, backupTime)
	groupDir := filepath.Join(pbsStoreRoot, groupPath)

	// If fileName is empty, scan directory for pxar files
	if fileName == "" {
		entries, err := os.ReadDir(groupDir)
		if err != nil {
			return "", "", false, fmt.Errorf("failed to read backup directory: %w", err)
		}

		// Look for .mpxar.didx first (prefer split archives), then .pxar.didx
		var foundMpxar, foundPxar string
		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}
			name := entry.Name()
			if strings.HasSuffix(name, ".mpxar.didx") && foundMpxar == "" {
				foundMpxar = name
			} else if strings.HasSuffix(name, ".pxar.didx") && foundPxar == "" {
				foundPxar = name
			}
		}

		if foundMpxar != "" {
			fileName = foundMpxar
		} else if foundPxar != "" {
			fileName = foundPxar
		} else {
			return "", "", false, fmt.Errorf("no .pxar.didx or .mpxar.didx file found in %s", groupDir)
		}
	}

	if !strings.HasSuffix(fileName, ".mpxar.didx") &&
		!strings.HasSuffix(fileName, ".pxar.didx") &&
		!strings.HasSuffix(fileName, ".ppxar.didx") {
		return "", "", false, fmt.Errorf("file-name must end with .pxar.didx, .mpxar.didx, or .ppxar.didx")
	}

	switch {
	case strings.HasSuffix(fileName, ".pxar.didx"):
		// Non-split archive
		mpxarPath = filepath.Join(groupDir, fileName)
		return mpxarPath, "", false, nil

	case strings.HasSuffix(fileName, ".mpxar.didx"):
		// Metadata file provided, need to find payload
		mpxarPath = filepath.Join(groupDir, fileName)
		ppxarName := strings.TrimSuffix(fileName, ".mpxar.didx") + ".ppxar.didx"
		ppxarPath = filepath.Join(groupDir, ppxarName)
		if _, err := os.Stat(ppxarPath); err != nil {
			return "", "", false, fmt.Errorf("payload index not found: %s", ppxarPath)
		}
		return mpxarPath, ppxarPath, true, nil

	case strings.HasSuffix(fileName, ".ppxar.didx"):
		// Payload file provided, need to find metadata
		ppxarPath = filepath.Join(groupDir, fileName)
		mpxarName := strings.TrimSuffix(fileName, ".ppxar.didx") + ".mpxar.didx"
		mpxarPath = filepath.Join(groupDir, mpxarName)
		if _, err := os.Stat(mpxarPath); err != nil {
			return "", "", false, fmt.Errorf("metadata index not found: %s", mpxarPath)
		}
		return mpxarPath, ppxarPath, true, nil
	}

	return "", "", false, fmt.Errorf("unexpected file-name format")
}
